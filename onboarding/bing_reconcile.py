"""
Bing verification reconciliation job.

Polls Bing Webmaster Tools API to check verification status of pending sites
and updates the database accordingly. Designed to run as a periodic job
(e.g., every 15 minutes via cron or GitHub Actions).

Usage:
    python -m onboarding.bing_reconcile [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from typing import Any

from .config import load_config
from .bing import BingWebmasterClient
from .errors import BingWebmasterError

logger = logging.getLogger("article_factory.onboarding.bing_reconcile")


def get_pending_sites(supabase_client: Any) -> list[dict]:
    """
    Fetch sites with bing_status = 'pending' from Supabase.

    Args:
        supabase_client: Initialized Supabase client.

    Returns:
        List of site records with id, domain, bing_status fields.
    """
    response = (
        supabase_client.table("sites")
        .select("id, domain, bing_status, bing_last_check_at")
        .eq("bing_status", "pending")
        .execute()
    )
    return response.data or []


def check_and_update_site(
    bing: BingWebmasterClient,
    supabase_client: Any,
    site: dict,
    dry_run: bool = False,
) -> dict:
    """
    Check Bing verification status for a single site and update database.

    Args:
        bing: Initialized Bing client.
        supabase_client: Initialized Supabase client.
        site: Site record from database.
        dry_run: If True, don't update database.

    Returns:
        Result dict with old_status, new_status, error if any.
    """
    domain = site["domain"]
    site_id = site["id"]
    now = datetime.now(timezone.utc).isoformat()

    result = {
        "domain": domain,
        "old_status": "pending",
        "new_status": None,
        "error": None,
    }

    try:
        # Check if verified
        is_verified = bing.is_verified(domain)

        if is_verified:
            result["new_status"] = "verified"
            if not dry_run:
                supabase_client.table("sites").update({
                    "bing_status": "verified",
                    "bing_verified_at": now,
                    "bing_last_check_at": now,
                }).eq("id", site_id).execute()
                logger.info(f"Site verified: {domain}")
        else:
            # Still pending, just update last check time
            result["new_status"] = "pending"
            if not dry_run:
                supabase_client.table("sites").update({
                    "bing_last_check_at": now,
                }).eq("id", site_id).execute()
                logger.debug(f"Site still pending: {domain}")

    except BingWebmasterError as e:
        # Check if site doesn't exist (might have been deleted)
        if "not found" in str(e).lower():
            result["new_status"] = "failed"
            result["error"] = "Site not found in Bing"
            if not dry_run:
                supabase_client.table("sites").update({
                    "bing_status": "failed",
                    "bing_last_check_at": now,
                }).eq("id", site_id).execute()
                logger.warning(f"Site not found in Bing: {domain}")
        else:
            # Transient error, don't change status
            result["error"] = str(e)[:200]
            logger.warning(f"Error checking {domain}: {e}")

    return result


def reconcile(
    supabase_client: Any | None = None,
    dry_run: bool = False,
    max_sites: int = 50,
) -> dict:
    """
    Run the full reconciliation job.

    Args:
        supabase_client: Supabase client (if None, will initialize).
        dry_run: If True, don't update database.
        max_sites: Maximum sites to check per run.

    Returns:
        Summary dict with total_checked, verified, still_pending, errors.
    """
    config = load_config()

    if not config.bing_api_key:
        logger.warning("Bing API key not configured, skipping reconciliation")
        return {"error": "no_api_key"}

    bing = BingWebmasterClient(config.bing_api_key)

    # Initialize Supabase if not provided
    if supabase_client is None:
        import os
        from supabase import create_client
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        if not supabase_url or not supabase_key:
            logger.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required")
            return {"error": "no_supabase_credentials"}
        supabase_client = create_client(supabase_url, supabase_key)

    # Get pending sites
    pending_sites = get_pending_sites(supabase_client)
    logger.info(f"Found {len(pending_sites)} sites with bing_status=pending")

    if not pending_sites:
        return {"total_checked": 0, "verified": 0, "still_pending": 0, "errors": 0}

    # Limit to max_sites
    sites_to_check = pending_sites[:max_sites]

    summary = {
        "total_checked": 0,
        "verified": 0,
        "still_pending": 0,
        "failed": 0,
        "errors": 0,
        "results": [],
    }

    for site in sites_to_check:
        result = check_and_update_site(bing, supabase_client, site, dry_run)
        summary["total_checked"] += 1
        summary["results"].append(result)

        if result["new_status"] == "verified":
            summary["verified"] += 1
        elif result["new_status"] == "pending":
            summary["still_pending"] += 1
        elif result["new_status"] == "failed":
            summary["failed"] += 1

        if result["error"]:
            summary["errors"] += 1

    logger.info(
        f"Reconciliation complete: {summary['verified']} verified, "
        f"{summary['still_pending']} pending, {summary['errors']} errors"
    )

    return summary


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Reconcile Bing verification status for pending sites"
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

    print(f"\nResults: {result}")


if __name__ == "__main__":
    main()
