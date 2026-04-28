#!/usr/bin/env python3
"""
Weekly GSC Rollup and Report — Cron entry point.

Aggregates weekly GSC metrics and sends a summary report to Slack.
Runs every Monday at 9am UTC via GitHub Actions.

Usage:
    python scripts/cron_gsc_weekly.py              # Generate rollup and report
    python scripts/cron_gsc_weekly.py --dry-run    # Preview without posting
    python scripts/cron_gsc_weekly.py --no-slack   # Generate without Slack

Cron example (Monday 9am UTC):
    0 9 * * 1 cd /path/to/factory && python scripts/cron_gsc_weekly.py >> /var/log/gsc-weekly.log 2>&1
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

import requests

from monitoring.gsc_alerts import post_weekly_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("article_factory.cron_gsc_weekly")


class WeeklyRollupGenerator:
    """Generates weekly GSC rollups and reports."""

    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Supabase credentials not configured")

    def _request(self, method: str, endpoint: str, data: dict | list = None, headers_extra: dict = None) -> dict | list:
        """Make a request to Supabase REST API."""
        url = f"{self.supabase_url}/rest/v1/{endpoint}"
        headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }
        if headers_extra:
            headers.update(headers_extra)

        resp = requests.request(method, url, headers=headers, json=data, timeout=60)
        resp.raise_for_status()

        if resp.text:
            return resp.json()
        return {}

    def get_week_boundaries(self, reference_date: date = None) -> tuple[date, date]:
        """
        Get the week boundaries (Monday to Sunday) for the previous complete week.

        Args:
            reference_date: Reference date (default: today).

        Returns:
            Tuple of (week_start, week_end) dates.
        """
        if reference_date is None:
            reference_date = date.today()

        # Find the Monday of the current week
        current_monday = reference_date - timedelta(days=reference_date.weekday())

        # Previous week is 7 days before
        week_start = current_monday - timedelta(days=7)
        week_end = week_start + timedelta(days=6)  # Sunday

        return week_start, week_end

    def get_sites_with_monitoring(self) -> list[dict]:
        """Get all sites with GSC monitoring enabled."""
        try:
            data = self._request(
                "GET",
                "sites?select=id,domain,brand_keywords&gsc_monitoring_enabled=eq.true"
            )
            return data or []
        except Exception as e:
            logger.error(f"Failed to get sites: {e}")
            return []

    def aggregate_weekly_metrics(
        self,
        site_id: str,
        week_start: date,
        week_end: date,
    ) -> dict:
        """
        Aggregate metrics for a site for the given week.

        Args:
            site_id: The site UUID.
            week_start: Start of week (Monday).
            week_end: End of week (Sunday).

        Returns:
            Dict with aggregated metrics.
        """
        try:
            # Fetch all metrics for the week
            data = self._request(
                "GET",
                f"gsc_metrics?site_id=eq.{site_id}"
                f"&date=gte.{week_start.isoformat()}"
                f"&date=lte.{week_end.isoformat()}"
                f"&select=clicks,impressions,ctr,position,is_branded,page,query"
            )

            if not data:
                return {
                    "total_clicks": 0,
                    "total_impressions": 0,
                    "avg_ctr": None,
                    "avg_position": None,
                    "branded_clicks": 0,
                    "branded_impressions": 0,
                    "non_branded_clicks": 0,
                    "non_branded_impressions": 0,
                    "top_pages": [],
                    "top_queries": [],
                }

            # Calculate totals
            total_clicks = sum(row.get("clicks", 0) for row in data)
            total_impressions = sum(row.get("impressions", 0) for row in data)

            # Calculate averages (weighted by impressions)
            total_ctr_weight = 0
            total_position_weight = 0
            ctr_sum = 0
            position_sum = 0

            for row in data:
                impressions = row.get("impressions", 0)
                if impressions > 0:
                    ctr = row.get("ctr")
                    position = row.get("position")
                    if ctr is not None:
                        ctr_sum += ctr * impressions
                        total_ctr_weight += impressions
                    if position is not None:
                        position_sum += position * impressions
                        total_position_weight += impressions

            avg_ctr = ctr_sum / total_ctr_weight if total_ctr_weight > 0 else None
            avg_position = position_sum / total_position_weight if total_position_weight > 0 else None

            # Branded vs non-branded
            branded_clicks = sum(row.get("clicks", 0) for row in data if row.get("is_branded"))
            branded_impressions = sum(row.get("impressions", 0) for row in data if row.get("is_branded"))
            non_branded_clicks = total_clicks - branded_clicks
            non_branded_impressions = total_impressions - branded_impressions

            # Top pages (aggregate by page)
            page_metrics = {}
            for row in data:
                page = row.get("page", "")
                if page not in page_metrics:
                    page_metrics[page] = {"clicks": 0, "impressions": 0}
                page_metrics[page]["clicks"] += row.get("clicks", 0)
                page_metrics[page]["impressions"] += row.get("impressions", 0)

            top_pages = sorted(
                [{"page": p, **m} for p, m in page_metrics.items()],
                key=lambda x: x["clicks"],
                reverse=True
            )[:10]

            # Top queries (aggregate by query)
            query_metrics = {}
            for row in data:
                query = row.get("query")
                if query:
                    if query not in query_metrics:
                        query_metrics[query] = {"clicks": 0, "impressions": 0, "is_branded": row.get("is_branded", False)}
                    query_metrics[query]["clicks"] += row.get("clicks", 0)
                    query_metrics[query]["impressions"] += row.get("impressions", 0)

            top_queries = sorted(
                [{"query": q, **m} for q, m in query_metrics.items()],
                key=lambda x: x["clicks"],
                reverse=True
            )[:10]

            return {
                "total_clicks": total_clicks,
                "total_impressions": total_impressions,
                "avg_ctr": round(avg_ctr, 4) if avg_ctr else None,
                "avg_position": round(avg_position, 2) if avg_position else None,
                "branded_clicks": branded_clicks,
                "branded_impressions": branded_impressions,
                "non_branded_clicks": non_branded_clicks,
                "non_branded_impressions": non_branded_impressions,
                "top_pages": top_pages,
                "top_queries": top_queries,
            }

        except Exception as e:
            logger.error(f"Failed to aggregate metrics for site {site_id}: {e}")
            return {}

    def calculate_wow_change(
        self,
        site_id: str,
        current_week_start: date,
    ) -> dict:
        """
        Calculate week-over-week changes.

        Args:
            site_id: The site UUID.
            current_week_start: Start of the current week.

        Returns:
            Dict with WoW change percentages.
        """
        previous_week_start = current_week_start - timedelta(days=7)
        previous_week_end = current_week_start - timedelta(days=1)
        current_week_end = current_week_start + timedelta(days=6)

        # Get previous week totals
        prev_metrics = self.aggregate_weekly_metrics(site_id, previous_week_start, previous_week_end)
        curr_metrics = self.aggregate_weekly_metrics(site_id, current_week_start, current_week_end)

        prev_clicks = prev_metrics.get("total_clicks", 0)
        curr_clicks = curr_metrics.get("total_clicks", 0)
        prev_impressions = prev_metrics.get("total_impressions", 0)
        curr_impressions = curr_metrics.get("total_impressions", 0)

        clicks_change = None
        impressions_change = None

        if prev_clicks > 0:
            clicks_change = ((curr_clicks - prev_clicks) / prev_clicks) * 100

        if prev_impressions > 0:
            impressions_change = ((curr_impressions - prev_impressions) / prev_impressions) * 100

        return {
            "clicks_wow_change": round(clicks_change, 2) if clicks_change is not None else None,
            "impressions_wow_change": round(impressions_change, 2) if impressions_change is not None else None,
        }

    def save_weekly_rollup(
        self,
        site_id: str,
        week_start: date,
        week_end: date,
        metrics: dict,
        wow_changes: dict,
    ) -> bool:
        """
        Save weekly rollup to database.

        Args:
            site_id: The site UUID.
            week_start: Start of week.
            week_end: End of week.
            metrics: Aggregated metrics.
            wow_changes: Week-over-week changes.

        Returns:
            True if saved successfully.
        """
        try:
            self._request(
                "POST",
                "gsc_weekly_rollup",
                {
                    "site_id": site_id,
                    "week_start": week_start.isoformat(),
                    "week_end": week_end.isoformat(),
                    "total_clicks": metrics.get("total_clicks", 0),
                    "total_impressions": metrics.get("total_impressions", 0),
                    "avg_ctr": metrics.get("avg_ctr"),
                    "avg_position": metrics.get("avg_position"),
                    "branded_clicks": metrics.get("branded_clicks", 0),
                    "branded_impressions": metrics.get("branded_impressions", 0),
                    "non_branded_clicks": metrics.get("non_branded_clicks", 0),
                    "non_branded_impressions": metrics.get("non_branded_impressions", 0),
                    "top_pages": json.dumps(metrics.get("top_pages", [])),
                    "top_queries": json.dumps(metrics.get("top_queries", [])),
                    "clicks_wow_change": wow_changes.get("clicks_wow_change"),
                    "impressions_wow_change": wow_changes.get("impressions_wow_change"),
                },
                headers_extra={"Prefer": "return=minimal,resolution=merge-duplicates"},
            )
            return True
        except Exception as e:
            logger.error(f"Failed to save weekly rollup: {e}")
            return False

    def get_unresolved_alerts(self) -> list[dict]:
        """Get all unresolved GSC alerts."""
        try:
            data = self._request(
                "GET",
                "gsc_alerts?resolved_at=is.null&select=id,site_id,alert_type,title,severity,detected_at"
                "&order=detected_at.desc"
            )

            # Get site domains for the alerts
            if data:
                site_ids = list(set(a.get("site_id") for a in data if a.get("site_id")))
                if site_ids:
                    sites_data = self._request(
                        "GET",
                        f"sites?id=in.({','.join(site_ids)})&select=id,domain"
                    )
                    site_map = {s["id"]: s["domain"] for s in sites_data}
                    for alert in data:
                        alert["domain"] = site_map.get(alert.get("site_id"), "Unknown")

            return data or []
        except Exception as e:
            logger.error(f"Failed to get unresolved alerts: {e}")
            return []

    def generate_rollups(self, reference_date: date = None) -> list[dict]:
        """
        Generate weekly rollups for all sites.

        Args:
            reference_date: Reference date for week calculation.

        Returns:
            List of site summaries with rollup data.
        """
        week_start, week_end = self.get_week_boundaries(reference_date)
        logger.info(f"Generating weekly rollups for {week_start} to {week_end}")

        sites = self.get_sites_with_monitoring()
        if not sites:
            logger.info("No sites with GSC monitoring enabled")
            return []

        results = []

        for site in sites:
            site_id = site.get("id")
            domain = site.get("domain")

            logger.info(f"Processing {domain}...")

            # Aggregate metrics
            metrics = self.aggregate_weekly_metrics(site_id, week_start, week_end)

            # Calculate WoW changes
            wow_changes = self.calculate_wow_change(site_id, week_start)

            # Save to database
            self.save_weekly_rollup(site_id, week_start, week_end, metrics, wow_changes)

            # Add to results for report
            results.append({
                "domain": domain,
                "site_id": site_id,
                "week_start": week_start.isoformat(),
                "week_end": week_end.isoformat(),
                "total_clicks": metrics.get("total_clicks", 0),
                "total_impressions": metrics.get("total_impressions", 0),
                "avg_ctr": metrics.get("avg_ctr"),
                "avg_position": metrics.get("avg_position"),
                "branded_clicks": metrics.get("branded_clicks", 0),
                "non_branded_clicks": metrics.get("non_branded_clicks", 0),
                "clicks_wow_change": wow_changes.get("clicks_wow_change"),
                "impressions_wow_change": wow_changes.get("impressions_wow_change"),
            })

        return results


def format_console_report(results: list[dict], unresolved_alerts: list[dict]) -> str:
    """Format a console-friendly report."""
    lines = [
        "=" * 70,
        "GSC Weekly Rollup Report",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "=" * 70,
        "",
    ]

    if not results:
        lines.append("No sites with GSC data.")
        return "\n".join(lines)

    # Summary
    total_clicks = sum(r.get("total_clicks", 0) for r in results)
    total_impressions = sum(r.get("total_impressions", 0) for r in results)
    lines.append(f"Sites: {len(results)}")
    lines.append(f"Total clicks: {total_clicks:,}")
    lines.append(f"Total impressions: {total_impressions:,}")
    lines.append("")

    # Per-site table
    lines.append("-" * 70)
    lines.append(f"{'Domain':<25} {'Clicks':<12} {'Impr':<12} {'WoW %':<10} {'Pos':<8}")
    lines.append("-" * 70)

    for r in sorted(results, key=lambda x: x.get("total_clicks", 0), reverse=True):
        domain = r.get("domain", "")[:24]
        clicks = f"{r.get('total_clicks', 0):,}"
        impressions = f"{r.get('total_impressions', 0):,}"
        wow = r.get("clicks_wow_change")
        wow_str = f"{wow:+.1f}%" if wow is not None else "N/A"
        pos = r.get("avg_position")
        pos_str = f"{pos:.1f}" if pos else "N/A"
        lines.append(f"{domain:<25} {clicks:<12} {impressions:<12} {wow_str:<10} {pos_str:<8}")

    # Unresolved alerts
    if unresolved_alerts:
        lines.append("")
        lines.append("-" * 70)
        lines.append(f"Unresolved Alerts: {len(unresolved_alerts)}")
        lines.append("-" * 70)
        for alert in unresolved_alerts[:10]:
            alert_type = alert.get("alert_type", "unknown")
            domain = alert.get("domain", "Unknown")
            lines.append(f"  [{alert_type}] {domain}")

    lines.append("")
    lines.append("=" * 70)

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Weekly GSC rollup and Slack report"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate report without saving or posting"
    )
    parser.add_argument(
        "--no-slack",
        action="store_true",
        help="Skip posting to Slack"
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
        generator = WeeklyRollupGenerator()

        # Generate rollups
        results = generator.generate_rollups()

        # Get unresolved alerts
        unresolved_alerts = generator.get_unresolved_alerts()

        # Print console report
        report = format_console_report(results, unresolved_alerts)
        print(report)

        # Post to Slack
        if not args.dry_run and not args.no_slack and results:
            success = post_weekly_report(results, unresolved_alerts)
            if success:
                logger.info("Weekly report posted to Slack")
            else:
                logger.warning("Failed to post weekly report to Slack")

        return 0

    except Exception as e:
        logger.error(f"Weekly rollup failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
