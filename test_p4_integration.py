#!/usr/bin/env python3
"""
Integration test for P4 gates in the Factory deposit flow.

This test verifies that:
1. P4 gates can be called from Factory
2. P4 gates correctly pass/fail articles
3. The deposit flow integrates P4 gates properly

Usage:
    python test_p4_integration.py
    python test_p4_integration.py --article-id <uuid>
    python test_p4_integration.py --slug urolithin-a-mitochondrial-rejuvenation-muscle-longevity
"""

import os
import sys
import argparse

from dotenv import load_dotenv
load_dotenv()


def test_import():
    """Test that P4 gates module can be imported."""
    print("Test 1: Import P4 gates module...")
    try:
        from p4_gates import run_p4_gates, update_article_status
        print("  PASS - P4 gates module imported successfully")
        return True
    except ImportError as e:
        print(f"  FAIL - Import error: {e}")
        return False


def test_supabase_connection():
    """Test that Supabase connection works."""
    print("\nTest 2: Supabase connection...")
    try:
        from p4_gates import get_supabase_client
        db = get_supabase_client()

        # Try a simple query
        result = db.table('sites').select('id').limit(1).execute()
        if result.data:
            print("  PASS - Supabase connected, can query sites table")
            return True
        else:
            print("  WARN - Connected but no sites found")
            return True
    except Exception as e:
        print(f"  FAIL - Connection error: {e}")
        return False


def test_p4_gates_run(article_id: str = None, slug: str = None):
    """Test running P4 gates on an article."""
    print("\nTest 3: Run P4 gates on article...")

    from p4_gates import get_supabase_client, run_p4_gates
    db = get_supabase_client()

    # Find an article to test
    if article_id:
        result = db.table('content').select('id, title, slug').eq('id', article_id).single().execute()
    elif slug:
        result = db.table('content').select('id, title, slug').eq('slug', slug).single().execute()
    else:
        # Find any published or pending article
        result = db.table('content').select('id, title, slug').in_(
            'status', ['published', 'pending_review']
        ).limit(1).execute()
        if result.data:
            result.data = result.data[0]

    if not result.data:
        print("  SKIP - No article found to test")
        return None

    article = result.data
    article_id = article['id']
    print(f"  Testing article: {article['title'][:50]}...")
    print(f"  ID: {article_id}")

    # Run P4 gates (don't save results - this is just a test)
    try:
        p4_result = run_p4_gates(article_id, save_results=False)

        if p4_result.get('error'):
            print(f"  FAIL - Error: {p4_result['error']}")
            return False

        if p4_result['passed']:
            print("  PASS - P4 gates passed")
        else:
            failures = p4_result.get('failure_reasons', [])
            print(f"  INFO - P4 gates found {len(failures)} issues:")
            for f in failures[:5]:
                print(f"    - [{f['category']}.{f['gate']}] {f['reason']}")

        return p4_result

    except Exception as e:
        print(f"  FAIL - Exception: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_site_empire_import():
    """Test that site-empire module imports work."""
    print("\nTest 4: Site Empire module imports...")

    # Add site-empire to path
    site_empire_path = os.path.join(os.path.dirname(__file__), '..', 'site-empire')
    if site_empire_path not in sys.path:
        sys.path.insert(0, site_empire_path)

    try:
        from factory.qa import run_qa_stage, QARunner, QAReport
        print("  PASS - factory.qa imports work")

        from factory.qa.gates.seo_technical import SEOTechnicalGates
        print("  PASS - SEOTechnicalGates import works")

        from factory.qa.gates.content_quality import ContentQualityGates
        print("  PASS - ContentQualityGates import works")

        from factory.qa.persona_assignment import assign_persona
        print("  PASS - persona_assignment import works")

        return True
    except ImportError as e:
        print(f"  FAIL - Import error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_deposit_integration():
    """Test that deposit.py has P4 integration."""
    print("\nTest 5: Deposit module P4 integration...")

    try:
        import deposit

        # Check if P4 imports are present
        if hasattr(deposit, 'run_p4_gates'):
            print("  PASS - deposit.py imports run_p4_gates")
        else:
            print("  FAIL - deposit.py missing run_p4_gates import")
            return False

        if hasattr(deposit, 'update_article_status'):
            print("  PASS - deposit.py imports update_article_status")
        else:
            print("  FAIL - deposit.py missing update_article_status import")
            return False

        return True

    except ImportError as e:
        print(f"  FAIL - Import error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Test P4 gates integration")
    parser.add_argument("--article-id", help="Test with specific article UUID")
    parser.add_argument("--slug", help="Test with specific article slug")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    print("=" * 60)
    print("P4 Gates Integration Test")
    print("=" * 60)

    results = []

    # Run tests
    results.append(("Import P4 gates", test_import()))
    results.append(("Supabase connection", test_supabase_connection()))
    results.append(("Site Empire imports", test_site_empire_import()))
    results.append(("Deposit integration", test_deposit_integration()))

    # P4 gates run test (may return dict or bool)
    p4_result = test_p4_gates_run(args.article_id, args.slug)
    if p4_result is None:
        results.append(("P4 gates run", "SKIP"))
    elif p4_result is False:
        results.append(("P4 gates run", False))
    else:
        results.append(("P4 gates run", True))

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    passed = 0
    failed = 0
    skipped = 0

    for name, result in results:
        if result == "SKIP":
            print(f"  SKIP - {name}")
            skipped += 1
        elif result:
            print(f"  PASS - {name}")
            passed += 1
        else:
            print(f"  FAIL - {name}")
            failed += 1

    print(f"\nTotal: {passed} passed, {failed} failed, {skipped} skipped")

    if failed > 0:
        print("\nIntegration test FAILED")
        return 1
    else:
        print("\nIntegration test PASSED")
        return 0


if __name__ == '__main__':
    sys.exit(main())
