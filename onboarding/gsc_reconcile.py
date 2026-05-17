"""
GSC verification reconciliation job.

Picks up sites with gsc_status='not_started' or 'pending', attempts DNS-based
verification, and updates the database. Designed to run as a periodic job
(e.g., every 15 minutes via cron or GitHub Actions).

Flow per site:
1. Get DNS TXT verification token from GSC API
2. Check if TXT record exists in DNS
3. If exists, verify domain ownership
4. If verified, add property and submit sitemap
5. Update gsc_status accordingly

Usage:
    python -m onboarding.gsc_reconcile [--dry-run] [--max-sites 10]
"""
from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import dns.resolver

from .config import load_config
from .search_console import SearchConsoleClient
from .errors import SearchConsoleError

logger = logging.getLogger("article_factory.onboarding.gsc_reconcile")


def check_dns_txt_record(domain: str, expected_token: str) -> bool:
    """
    Check if the expected GSC verification TXT record exists in DNS.

    Args:
        domain: The domain to check.
        expected_token: The google-site-verification token to look for.

    Returns:
        True if the TXT record exists and matches.
    """
    try:
        answers = dns.resolver.resolve(domain, 'TXT')
        for rdata in answers:
            txt_value = str(rdata).strip('"')
            if expected_token in txt_value:
                logger.debug(f"Found matching TXT record for {domain}")
                return True
        logger.debug(f"No matching TXT record found for {domain}")
        return False
    except dns.resolver.NXDOMAIN:
        logger.debug(f"Domain {domain} does not exist in DNS")
        return False
    except dns.resolver.NoAnswer:
        logger.debug(f"No TXT records for {domain}")
        return False
    except Exception as e:
        logger.warning(f"DNS lookup failed for {domain}: {e}")
        return False


def get_pending_sites(supabase_client: Any) -> list[dict]:
    """
    Fetch sites with gsc_status in ('not_started', 'pending') from Supabase.

    Args:
        supabase_client: Initialized Supabase client.

    Returns:
        List of site records.
    """
    response = (
        supabase_client.table("sites")
        .select("id, domain, gsc_status, gsc_verified_at, gsc_last_check_at")
        .eq("active", True)
        .in_("gsc_status", ["not_started", "pending"])
        .execute()
    )
    return response.data or []


def verify_and_setup_site(
    gsc: SearchConsoleClient,
    supabase_client: Any,
    site: dict,
    dry_run: bool = False,
) -> dict:
    """
    Attempt to verify a single site via GSC and set up property.

    Args:
        gsc: Initialized GSC client.
        supabase_client: Initialized Supabase client.
        site: Site record from database.
        dry_run: If True, don't update database.

    Returns:
        Result dict with domain, old_status, new_status, error, steps.
    """
    domain = site["domain"]
    site_id = site["id"]
    old_status = site.get("gsc_status", "not_started")
    now = datetime.now(timezone.utc).isoformat()

    result = {
        "domain": domain,
        "old_status": old_status,
        "new_status": None,
        "error": None,
        "steps": [],
    }

    try:
        # Step 1: Check if already verified
        if gsc.is_verified(domain):
            result["steps"].append("already_verified")
            result["new_status"] = "verified"

            # Ensure property exists
            if not gsc.property_exists(domain):
                gsc.add_property(domain)
                result["steps"].append("property_added")

            # Submit sitemap
            try:
                gsc.submit_sitemap(domain)
                result["steps"].append("sitemap_submitted")
            except SearchConsoleError as e:
                result["steps"].append(f"sitemap_failed: {str(e)[:50]}")

            if not dry_run:
                supabase_client.table("sites").update({
                    "gsc_status": "verified",
                    "gsc_verified_at": now,
                    "gsc_last_check_at": now,
                }).eq("id", site_id).execute()

            logger.info(f"Site already verified: {domain}")
            return result

        # Step 2: Get verification token
        try:
            token = gsc.get_verification_token(domain)
            result["steps"].append("token_retrieved")
        except SearchConsoleError as e:
            result["error"] = f"Failed to get token: {str(e)[:100]}"
            result["new_status"] = old_status  # Keep current status
            if not dry_run:
                supabase_client.table("sites").update({
                    "gsc_last_check_at": now,
                }).eq("id", site_id).execute()
            return result

        # Step 3: Check DNS for TXT record
        if not check_dns_txt_record(domain, token):
            result["steps"].append("dns_txt_missing")
            result["new_status"] = "pending"
            result["error"] = f"DNS TXT record not found. Expected: {token}"

            if not dry_run:
                supabase_client.table("sites").update({
                    "gsc_status": "pending",
                    "gsc_last_check_at": now,
                }).eq("id", site_id).execute()

            logger.info(f"DNS TXT record missing for {domain}")
            return result

        result["steps"].append("dns_txt_found")

        # Step 4: Verify domain
        try:
            gsc.verify_domain(domain, max_retries=2, retry_delay=10)
            result["steps"].append("domain_verified")
        except SearchConsoleError as e:
            result["error"] = f"Verification failed: {str(e)[:100]}"
            result["new_status"] = "pending"

            if not dry_run:
                supabase_client.table("sites").update({
                    "gsc_status": "pending",
                    "gsc_last_check_at": now,
                }).eq("id", site_id).execute()

            return result

        # Step 5: Add property
        try:
            gsc.add_property(domain)
            result["steps"].append("property_added")
        except SearchConsoleError as e:
            result["steps"].append(f"property_add_failed: {str(e)[:50]}")

        # Step 6: Submit sitemap
        try:
            gsc.submit_sitemap(domain)
            result["steps"].append("sitemap_submitted")
        except SearchConsoleError as e:
            result["steps"].append(f"sitemap_failed: {str(e)[:50]}")

        # Success
        result["new_status"] = "verified"

        if not dry_run:
            supabase_client.table("sites").update({
                "gsc_status": "verified",
                "gsc_verified_at": now,
                "gsc_last_check_at": now,
            }).eq("id", site_id).execute()

        logger.info(f"Site verified successfully: {domain}")
        return result

    except Exception as e:
        result["error"] = str(e)[:200]
        result["new_status"] = old_status
        logger.error(f"Unexpected error for {domain}: {e}")
        return result


def reconcile(
    supabase_client: Any | None = None,
    dry_run: bool = False,
    max_sites: int = 50,
) -> dict:
    """
    Run the full GSC reconciliation job.

    Args:
        supabase_client: Supabase client (if None, will initialize).
        dry_run: If True, don't update database.
        max_sites: Maximum sites to process per run.

    Returns:
        Summary dict with totals and per-site results.
    """
    config = load_config()

    if not config.google_sa_json_path:
        logger.error("GSC service account JSON not configured")
        return {"error": "no_service_account"}

    if not os.path.exists(config.google_sa_json_path):
        logger.error(f"GSC service account file not found: {config.google_sa_json_path}")
        return {"error": "service_account_file_not_found"}

    gsc = SearchConsoleClient(config.google_sa_json_path)

    # Initialize Supabase if not provided
    if supabase_client is None:
        from supabase import create_client
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        if not supabase_url or not supabase_key:
            logger.error("SUPABASE_URL and SUPABASE_SERVICE_KEY required")
            return {"error": "no_supabase_credentials"}
        supabase_client = create_client(supabase_url, supabase_key)

    # Get pending sites
    pending_sites = get_pending_sites(supabase_client)
    logger.info(f"Found {len(pending_sites)} sites needing GSC verification")

    if not pending_sites:
        return {
            "total_checked": 0,
            "verified": 0,
            "pending": 0,
            "failed": 0,
            "errors": 0,
            "results": [],
        }

    # Limit to max_sites
    sites_to_check = pending_sites[:max_sites]

    summary = {
        "total_checked": 0,
        "verified": 0,
        "pending": 0,
        "failed": 0,
        "errors": 0,
        "results": [],
    }

    for site in sites_to_check:
        result = verify_and_setup_site(gsc, supabase_client, site, dry_run)
        summary["total_checked"] += 1
        summary["results"].append(result)

        if result["new_status"] == "verified":
            summary["verified"] += 1
        elif result["new_status"] == "pending":
            summary["pending"] += 1
        elif result["new_status"] == "failed":
            summary["failed"] += 1

        if result["error"]:
            summary["errors"] += 1

        # Small delay between sites to avoid rate limiting
        time.sleep(1)

    logger.info(
        f"GSC Reconciliation complete: {summary['verified']} verified, "
        f"{summary['pending']} pending, {summary['errors']} errors"
    )

    return summary


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Reconcile GSC verification status for pending sites"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check status but don't update database",
    )
    parser.add_argument(
        "--max-sites",
        type=int,
        default=50,
        help="Maximum sites to check per run (default: 50)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    result = reconcile(dry_run=args.dry_run, max_sites=args.max_sites)

    if args.dry_run:
        print("\n[DRY RUN] No database changes made")

    # Print summary
    print("\n" + "=" * 60)
    print("GSC RECONCILIATION SUMMARY")
    print("=" * 60)

    if "error" in result:
        print(f"ERROR: {result['error']}")
        return

    print(f"Total checked: {result['total_checked']}")
    print(f"Verified:      {result['verified']}")
    print(f"Pending:       {result['pending']}")
    print(f"Errors:        {result['errors']}")

    print("\nPer-site results:")
    for r in result.get("results", []):
        status_icon = "[OK]" if r["new_status"] == "verified" else "[--]" if r["new_status"] == "pending" else "[!!]"
        print(f"  {status_icon} {r['domain']}: {r['old_status']} -> {r['new_status']}")
        if r.get("steps"):
            print(f"      Steps: {', '.join(r['steps'])}")
        if r.get("error"):
            print(f"      Error: {r['error'][:80]}")


if __name__ == "__main__":
    main()
