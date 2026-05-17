"""
============================================================
ARTICLE FACTORY — P4 DETERMINISTIC QA GATES
============================================================
Bridge module that calls Site Empire's P4 deterministic QA gates.

P4 gates are 22 deterministic checks across 6 categories:
- SEO Technical (title, meta, H1, canonical, etc.)
- Content Quality (word count, duplicates, readability)
- Links (broken links, outbound ratio)
- Media (alt text, broken images)
- Author Attribution (persona assigned, voice match)
- Reviewer Overlay (medical review if applicable)

This module runs P4 gates on articles already in Supabase after
the initial publish. If gates fail, the article stays in
pending_review status.

Usage:
    from p4_gates import run_p4_gates

    result = run_p4_gates(article_id)
    if result['passed']:
        # Article can be auto-published
    else:
        # Article blocked, needs review
============================================================
"""

import os
import sys
import logging
from pathlib import Path

logger = logging.getLogger("article_factory.p4_gates")

# Path to site-empire's factory module
SITE_EMPIRE_PATH = Path(__file__).parent.parent / "site-empire"


def _load_site_empire_env():
    """Load environment from site-empire if not already set."""
    from dotenv import load_dotenv

    # Check if already loaded
    if os.environ.get('NEXT_PUBLIC_SUPABASE_URL'):
        return

    # Try site-empire's .env.local
    site_empire_env = SITE_EMPIRE_PATH / '.env.local'
    if site_empire_env.exists():
        load_dotenv(site_empire_env)
        logger.debug(f"Loaded env from {site_empire_env}")


def get_supabase_client():
    """Get Supabase client with Factory's credentials."""
    from supabase import create_client

    # Load site-empire env if not already set
    _load_site_empire_env()

    # Try Next.js style env vars first (what site-empire uses)
    url = os.environ.get('NEXT_PUBLIC_SUPABASE_URL') or os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or os.environ.get('SUPABASE_KEY')

    if not url or not key:
        raise ValueError(
            "Supabase credentials not found. Set NEXT_PUBLIC_SUPABASE_URL and "
            "SUPABASE_SERVICE_ROLE_KEY environment variables, or ensure "
            "site-empire/.env.local exists."
        )

    return create_client(url, key)


def run_p4_gates(article_id: str, save_results: bool = True) -> dict:
    """
    Run P4 deterministic QA gates on an article.

    This imports and runs site-empire's P4 gate module directly,
    avoiding subprocess overhead.

    Args:
        article_id: UUID of the article in Supabase
        save_results: If True, save results to database

    Returns:
        dict with:
            - passed: bool
            - failure_reasons: list of failure dicts
            - persona_assigned: persona ID if assigned during run
            - error: error message if something went wrong
    """
    result = {
        'passed': False,
        'failure_reasons': [],
        'persona_assigned': None,
        'error': None,
    }

    try:
        # Add site-empire to Python path if not already there
        site_empire_str = str(SITE_EMPIRE_PATH)
        if site_empire_str not in sys.path:
            sys.path.insert(0, site_empire_str)

        # Import the P4 stage module
        from factory.qa.stage import run_qa_stage

        # Get Supabase client
        db = get_supabase_client()

        # Run the P4 gates
        qa_result = run_qa_stage(
            db=db,
            article_id=article_id,
            auto_assign_persona=True,
            compute_ngrams=True,
            save_results=save_results,
        )

        result['passed'] = qa_result.get('passed', False)
        result['persona_assigned'] = qa_result.get('persona_assigned')

        # Extract failure reasons from report
        report = qa_result.get('report')
        if report and hasattr(report, 'failure_reasons'):
            result['failure_reasons'] = report.failure_reasons

        if result['passed']:
            logger.info(f"[p4_gates] ✅ Article {article_id} passed all P4 gates")
        else:
            failure_count = len(result['failure_reasons'])
            logger.warning(
                f"[p4_gates] ❌ Article {article_id} failed {failure_count} P4 gates"
            )
            for failure in result['failure_reasons'][:3]:  # Log first 3
                logger.warning(
                    f"  - [{failure['category']}.{failure['gate']}] {failure['reason']}"
                )

    except ImportError as e:
        result['error'] = f"Could not import P4 gates module: {e}"
        logger.error(f"[p4_gates] {result['error']}")
    except Exception as e:
        result['error'] = str(e)
        logger.error(f"[p4_gates] Error running P4 gates: {e}")

    return result


def update_article_status(article_id: str, qa_score: float, threshold: float) -> dict:
    """
    Update article status based on P4 gates + LLM QA score.

    An article is auto-published only if:
    1. P4 gates passed (qa_passed_at is set)
    2. LLM qa_score >= threshold
    3. Site has auto_publish_enabled

    Args:
        article_id: UUID of the article
        qa_score: LLM QA score from Factory's qa.py
        threshold: Site's publish threshold

    Returns:
        dict with:
            - status: new status (published/pending_review)
            - action: what happened
    """
    from datetime import datetime

    db = get_supabase_client()

    # Fetch article and site info
    article = db.table('content').select(
        'id, status, qa_passed_at, site_id, sites(auto_publish_enabled, publish_threshold)'
    ).eq('id', article_id).single().execute()

    if not article.data:
        return {'status': None, 'action': 'article_not_found'}

    data = article.data
    site = data.get('sites', {})
    auto_publish = site.get('auto_publish_enabled', False)
    site_threshold = site.get('publish_threshold', 8.0)
    qa_passed_at = data.get('qa_passed_at')

    # Use provided threshold or fall back to site threshold
    effective_threshold = threshold if threshold else site_threshold

    # Determine if article should be published
    should_publish = (
        auto_publish
        and qa_passed_at is not None  # P4 gates passed
        and qa_score is not None
        and qa_score >= effective_threshold
    )

    if should_publish:
        # Update to published
        db.table('content').update({
            'status': 'published',
            'published_at': datetime.utcnow().isoformat(),
        }).eq('id', article_id).execute()

        logger.info(f"[p4_gates] ✅ Auto-published article {article_id}")
        return {'status': 'published', 'action': 'auto_published'}
    else:
        # Stay in pending_review
        reasons = []
        if not auto_publish:
            reasons.append('auto_publish disabled')
        if qa_passed_at is None:
            reasons.append('P4 gates not passed')
        if qa_score is None or qa_score < effective_threshold:
            reasons.append(f'qa_score {qa_score} < threshold {effective_threshold}')

        logger.info(
            f"[p4_gates] Article {article_id} stays pending_review: {', '.join(reasons)}"
        )
        return {'status': 'pending_review', 'action': 'blocked', 'reasons': reasons}


# ── CLI ─────────────────────────────────────────────────────

def main():
    """Run P4 gates from command line."""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="Run P4 deterministic QA gates")
    parser.add_argument("article_id", help="Article UUID")
    parser.add_argument("--no-save", action="store_true", help="Don't save results to DB")
    parser.add_argument(
        "--update-status",
        action="store_true",
        help="Update article status after running gates"
    )
    parser.add_argument("--qa-score", type=float, help="LLM QA score for status update")
    parser.add_argument("--threshold", type=float, default=8.0, help="Publish threshold")

    args = parser.parse_args()

    # Load environment
    from dotenv import load_dotenv
    load_dotenv()

    print(f"Running P4 gates on article: {args.article_id}")
    print("=" * 60)

    result = run_p4_gates(args.article_id, save_results=not args.no_save)

    if result['error']:
        print(f"ERROR: {result['error']}")
        sys.exit(1)

    if result['persona_assigned']:
        print(f"Persona assigned: {result['persona_assigned']}")

    if result['passed']:
        print("\nVERDICT: PASS")
    else:
        print("\nVERDICT: FAIL")
        print(f"Failures ({len(result['failure_reasons'])}):")
        for failure in result['failure_reasons']:
            print(f"  - [{failure['category']}.{failure['gate']}] {failure['reason']}")

    # Optionally update status
    if args.update_status:
        print("\nUpdating article status...")
        status_result = update_article_status(
            args.article_id,
            args.qa_score or 0,
            args.threshold
        )
        print(f"Status: {status_result['status']} ({status_result['action']})")

    sys.exit(0 if result['passed'] else 1)


if __name__ == '__main__':
    main()
