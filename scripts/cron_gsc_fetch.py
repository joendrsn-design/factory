#!/usr/bin/env python3
"""
Daily GSC Data Fetch — Cron entry point.

Fetches Google Search Console performance data for all enabled tenants.
Runs daily at 8am UTC via GitHub Actions.

Usage:
    python scripts/cron_gsc_fetch.py               # Fetch for all enabled sites
    python scripts/cron_gsc_fetch.py --domain X    # Fetch for specific domain
    python scripts/cron_gsc_fetch.py --dry-run     # List sites without fetching

Cron example (8am UTC daily):
    0 8 * * * cd /path/to/factory && python scripts/cron_gsc_fetch.py >> /var/log/gsc-fetch.log 2>&1
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

from monitoring.gsc_monitor import GSCMonitor
from monitoring.gsc_alerts import (
    post_traffic_drop_alert,
    post_manual_action_alert,
    post_security_issue_alert,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("article_factory.cron_gsc_fetch")


def send_alert_notifications(results: list) -> int:
    """
    Send Slack notifications for any alerts generated during fetch.

    Args:
        results: List of FetchResult objects.

    Returns:
        Number of alerts notified.
    """
    alerts_sent = 0

    for result in results:
        for alert in result.alerts_generated:
            if alert.alert_type == "traffic_drop":
                metrics = alert.metrics
                success = post_traffic_drop_alert(
                    domain=result.domain,
                    previous_clicks=metrics.get("previous_clicks", 0),
                    current_clicks=metrics.get("current_clicks", 0),
                    change_pct=metrics.get("change_pct", 0),
                )
                if success:
                    alerts_sent += 1

            elif alert.alert_type == "manual_action":
                success = post_manual_action_alert(
                    domain=result.domain,
                    action_type=alert.title,
                    description=alert.description,
                    affected_pages=alert.affected_pages,
                )
                if success:
                    alerts_sent += 1

            elif alert.alert_type == "security_issue":
                success = post_security_issue_alert(
                    domain=result.domain,
                    issue_type=alert.title,
                    description=alert.description,
                    affected_pages=alert.affected_pages,
                )
                if success:
                    alerts_sent += 1

    return alerts_sent


def format_summary_report(results: list) -> str:
    """
    Format a summary report of the fetch operation.

    Args:
        results: List of FetchResult objects.

    Returns:
        Formatted report string.
    """
    lines = [
        "=" * 60,
        "GSC Daily Fetch Report",
        f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "=" * 60,
        "",
    ]

    # Summary stats
    success_count = sum(1 for r in results if r.status == "success")
    failed_count = sum(1 for r in results if r.status == "failed")
    total_rows = sum(r.rows_fetched for r in results)
    total_inserted = sum(r.rows_inserted for r in results)
    total_alerts = sum(len(r.alerts_generated) for r in results)

    lines.append(f"Sites processed: {len(results)}")
    lines.append(f"Successful: {success_count}")
    lines.append(f"Failed: {failed_count}")
    lines.append(f"Total rows fetched: {total_rows:,}")
    lines.append(f"Total rows inserted: {total_inserted:,}")
    lines.append(f"Alerts generated: {total_alerts}")
    lines.append("")

    # Per-site details
    if results:
        lines.append("-" * 60)
        lines.append(f"{'Domain':<30} {'Status':<10} {'Rows':<10} {'Time':<10}")
        lines.append("-" * 60)

        for r in sorted(results, key=lambda x: x.domain):
            status_str = r.status.upper()[:10]
            rows_str = f"{r.rows_fetched:,}"
            time_str = f"{r.duration_ms}ms"
            lines.append(f"{r.domain:<30} {status_str:<10} {rows_str:<10} {time_str:<10}")

            if r.error_message:
                lines.append(f"  ERROR: {r.error_message[:50]}...")

            if r.alerts_generated:
                for alert in r.alerts_generated:
                    lines.append(f"  ALERT: {alert.alert_type} - {alert.title}")

    lines.append("")
    lines.append("=" * 60)

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Daily GSC data fetch for all enabled tenants"
    )
    parser.add_argument(
        "--domain",
        help="Fetch for a specific domain only"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List sites without fetching data"
    )
    parser.add_argument(
        "--no-alerts",
        action="store_true",
        help="Skip sending Slack alerts"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        monitor = GSCMonitor()

        # Dry run - just list sites
        if args.dry_run:
            sites = monitor.get_sites_for_monitoring()
            print(f"\nSites with GSC monitoring enabled: {len(sites)}")
            for site in sites:
                domain = site.get("domain", "Unknown")
                keywords = site.get("brand_keywords", [])
                keywords_str = ", ".join(keywords[:3]) if keywords else "(none)"
                print(f"  - {domain} [brand: {keywords_str}]")
            return 0

        # Fetch for specific domain
        if args.domain:
            site = monitor.get_site_by_domain(args.domain)
            if not site:
                logger.error(f"Site not found: {args.domain}")
                return 1

            logger.info(f"Fetching GSC data for {args.domain}")
            result = monitor.fetch_for_site(site)
            results = [result]

        # Fetch for all enabled sites
        else:
            results = monitor.run_daily_fetch()

        # Print summary report
        report = format_summary_report(results)
        print(report)

        # Send alert notifications
        if not args.no_alerts:
            alerts_sent = send_alert_notifications(results)
            if alerts_sent > 0:
                logger.info(f"Sent {alerts_sent} alert notifications to Slack")

        # Determine exit code
        failed = [r for r in results if r.status == "failed"]
        if failed:
            logger.warning(f"{len(failed)} sites failed during fetch")
            return 1

        return 0

    except Exception as e:
        logger.error(f"GSC fetch failed: {e}", exc_info=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
