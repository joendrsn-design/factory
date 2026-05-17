"""
============================================================
BACKFILL EMBEDDINGS — Generate embeddings for existing articles
============================================================

Backfills embeddings for all published articles that don't have one.
Uses OpenAI text-embedding-3-small (1536 dimensions).

Cost estimate: ~$0.0001 per article (~$0.10 per 1000 articles)

Usage:
    python scripts/backfill_embeddings.py                 # All sites
    python scripts/backfill_embeddings.py --site lamphill # Single site
    python scripts/backfill_embeddings.py --dry-run       # Preview only
    python scripts/backfill_embeddings.py --limit 10      # Process 10 articles
============================================================
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime

import requests
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from linking.embeddings import EmbeddingService

load_dotenv()

logger = logging.getLogger("article_factory.backfill_embeddings")


def get_articles_needing_embeddings(
    supabase_url: str,
    supabase_key: str,
    site_id: str = None,
    limit: int = None,
) -> list[dict]:
    """
    Fetch published articles that don't have embeddings.

    Returns list of dicts with id, title, body, site_id.
    """
    params = {
        "select": "id,title,body,site_id,slug",
        "status": "eq.published",
        "embedding": "is.null",
        "order": "created_at.desc",
    }

    if site_id:
        params["site_id"] = f"eq.{site_id}"

    if limit:
        params["limit"] = limit

    response = requests.get(
        f"{supabase_url}/rest/v1/content",
        headers={
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
        },
        params=params,
        timeout=60,
    )

    if response.status_code != 200:
        logger.error(f"Failed to fetch articles: {response.status_code} - {response.text}")
        return []

    return response.json()


def backfill_embeddings(
    site_id: str = None,
    limit: int = None,
    dry_run: bool = False,
    batch_delay: float = 0.5,
) -> dict:
    """
    Backfill embeddings for published articles.

    Args:
        site_id: Optional site filter.
        limit: Max articles to process.
        dry_run: Preview without making changes.
        batch_delay: Delay between articles (rate limiting).

    Returns:
        Summary dict with counts.
    """
    supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not supabase_key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")

    embedding_service = EmbeddingService()

    if not embedding_service.api_key:
        raise ValueError("OPENAI_API_KEY must be set for embedding generation")

    # Fetch articles needing embeddings
    logger.info(f"Fetching articles needing embeddings{f' for site {site_id}' if site_id else ''}...")
    articles = get_articles_needing_embeddings(
        supabase_url, supabase_key, site_id, limit
    )

    logger.info(f"Found {len(articles)} articles needing embeddings")

    if not articles:
        return {
            "total": 0,
            "processed": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "dry_run": dry_run,
        }

    summary = {
        "total": len(articles),
        "processed": 0,
        "success": 0,
        "failed": 0,
        "skipped": 0,
        "dry_run": dry_run,
        "errors": [],
    }

    start_time = datetime.now()

    for i, article in enumerate(articles, 1):
        article_id = article["id"]
        title = article.get("title", "Untitled")
        site = article.get("site_id", "unknown")
        body = article.get("body", "")

        if not body:
            logger.warning(f"[{i}/{len(articles)}] Skipping {title[:50]} - no body content")
            summary["skipped"] += 1
            continue

        logger.info(f"[{i}/{len(articles)}] Processing: {site}/{article.get('slug', article_id)}")

        if dry_run:
            logger.info(f"  DRY RUN: Would generate embedding ({len(body)} chars)")
            summary["processed"] += 1
            summary["success"] += 1
            continue

        try:
            success = embedding_service.embed_and_store(article_id, title, body)
            summary["processed"] += 1

            if success:
                logger.info("  Embedding stored")
                summary["success"] += 1
            else:
                logger.warning("  Embedding failed")
                summary["failed"] += 1
                summary["errors"].append({
                    "article_id": article_id,
                    "title": title,
                    "error": "embed_and_store returned False",
                })
        except Exception as e:
            logger.error(f"  Error: {e}")
            summary["processed"] += 1
            summary["failed"] += 1
            summary["errors"].append({
                "article_id": article_id,
                "title": title,
                "error": str(e),
            })

        # Rate limiting
        if i < len(articles):
            time.sleep(batch_delay)

    elapsed = (datetime.now() - start_time).total_seconds()
    summary["elapsed_seconds"] = elapsed

    return summary


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Backfill embeddings for published articles"
    )
    parser.add_argument(
        "--site",
        default="",
        help="Filter by site_id (e.g., lamphill)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max articles to process",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without making changes",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay between articles in seconds (default: 0.5)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("BACKFILL EMBEDDINGS")
    print("=" * 60)

    if args.dry_run:
        print("[DRY RUN] No changes will be made")

    if args.site:
        print(f"Site filter: {args.site}")

    if args.limit:
        print(f"Limit: {args.limit} articles")

    print("=" * 60)

    try:
        summary = backfill_embeddings(
            site_id=args.site or None,
            limit=args.limit,
            dry_run=args.dry_run,
            batch_delay=args.delay,
        )
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total articles:  {summary['total']}")
    print(f"Processed:       {summary['processed']}")
    print(f"Success:         {summary['success']}")
    print(f"Failed:          {summary['failed']}")
    print(f"Skipped:         {summary['skipped']}")

    if summary.get("elapsed_seconds"):
        print(f"Elapsed time:    {summary['elapsed_seconds']:.1f}s")

    if summary.get("errors"):
        print(f"\nErrors ({len(summary['errors'])}):")
        for err in summary["errors"][:5]:
            print(f"  - {err['title'][:40]}: {err['error']}")
        if len(summary["errors"]) > 5:
            print(f"  ... and {len(summary['errors']) - 5} more")

    if summary["dry_run"]:
        print("\n[DRY RUN] No changes were made")
    elif summary["success"] > 0:
        print(f"\nSuccessfully embedded {summary['success']} articles")

    # Exit code based on success
    if summary["failed"] > 0 and summary["success"] == 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
