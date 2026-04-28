#!/usr/bin/env python3
"""
Quarterly Content Audit — Identifies underperforming content.

Analyzes GSC metrics to find bottom decile content and provides
recommendations: noindex, consolidate, or refresh.

Usage:
    python scripts/quarterly_audit.py                    # Audit all sites
    python scripts/quarterly_audit.py --domain X         # Audit specific domain
    python scripts/quarterly_audit.py --threshold 20     # Custom bottom percentile

Output:
    - Console report with recommendations
    - Optional JSON export for further processing
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta, datetime, timezone
from pathlib import Path
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("article_factory.quarterly_audit")

# Default: bottom 10% of content
DEFAULT_PERCENTILE_THRESHOLD = 10

# Minimum impressions to consider a page for audit (avoid noise)
MIN_IMPRESSIONS_THRESHOLD = 100

# Time period for audit (90 days)
AUDIT_PERIOD_DAYS = 90


@dataclass
class ContentRecommendation:
    """Recommendation for a piece of content."""
    page: str
    clicks: int
    impressions: int
    ctr: float
    position: float
    recommendation: str  # 'noindex', 'consolidate', 'refresh', 'keep'
    reason: str
    confidence: float  # 0.0 to 1.0


@dataclass
class SiteAudit:
    """Audit results for a single site."""
    domain: str
    site_id: str
    audit_period_start: date
    audit_period_end: date
    total_pages_analyzed: int
    bottom_decile_count: int
    recommendations: list[ContentRecommendation] = field(default_factory=list)


class QuarterlyAuditor:
    """Performs quarterly content audits based on GSC data."""

    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Supabase credentials not configured")

    def _request(self, method: str, endpoint: str, data: dict | list = None) -> dict | list:
        """Make a request to Supabase REST API."""
        url = f"{self.supabase_url}/rest/v1/{endpoint}"
        headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

        resp = requests.request(method, url, headers=headers, json=data, timeout=60)
        resp.raise_for_status()

        if resp.text:
            return resp.json()
        return {}

    def get_sites_for_audit(self) -> list[dict]:
        """Get all sites with sufficient GSC data for audit."""
        try:
            data = self._request(
                "GET",
                "sites?select=id,domain&gsc_monitoring_enabled=eq.true"
            )
            return data or []
        except Exception as e:
            logger.error(f"Failed to get sites: {e}")
            return []

    def get_site_by_domain(self, domain: str) -> Optional[dict]:
        """Get a single site by domain."""
        try:
            data = self._request("GET", f"sites?domain=eq.{domain}&select=id,domain")
            return data[0] if data else None
        except Exception as e:
            logger.error(f"Failed to get site {domain}: {e}")
            return None

    def get_page_metrics(
        self,
        site_id: str,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        Get aggregated page-level metrics for the audit period.

        Args:
            site_id: The site UUID.
            start_date: Start of audit period.
            end_date: End of audit period.

        Returns:
            List of page metrics dicts.
        """
        try:
            # Fetch all metrics for the period
            data = self._request(
                "GET",
                f"gsc_metrics?site_id=eq.{site_id}"
                f"&date=gte.{start_date.isoformat()}"
                f"&date=lte.{end_date.isoformat()}"
                f"&select=page,clicks,impressions,ctr,position"
            )

            if not data:
                return []

            # Aggregate by page
            page_data = {}
            for row in data:
                page = row.get("page", "")
                if not page:
                    continue

                if page not in page_data:
                    page_data[page] = {
                        "page": page,
                        "clicks": 0,
                        "impressions": 0,
                        "ctr_sum": 0,
                        "ctr_count": 0,
                        "position_sum": 0,
                        "position_count": 0,
                    }

                page_data[page]["clicks"] += row.get("clicks", 0)
                page_data[page]["impressions"] += row.get("impressions", 0)

                ctr = row.get("ctr")
                if ctr is not None:
                    page_data[page]["ctr_sum"] += ctr
                    page_data[page]["ctr_count"] += 1

                position = row.get("position")
                if position is not None:
                    page_data[page]["position_sum"] += position
                    page_data[page]["position_count"] += 1

            # Calculate averages
            result = []
            for p in page_data.values():
                avg_ctr = p["ctr_sum"] / p["ctr_count"] if p["ctr_count"] > 0 else 0
                avg_position = p["position_sum"] / p["position_count"] if p["position_count"] > 0 else 0

                result.append({
                    "page": p["page"],
                    "clicks": p["clicks"],
                    "impressions": p["impressions"],
                    "ctr": round(avg_ctr, 4),
                    "position": round(avg_position, 2),
                })

            return result

        except Exception as e:
            logger.error(f"Failed to get page metrics: {e}")
            return []

    def identify_bottom_decile(
        self,
        pages: list[dict],
        percentile: int = DEFAULT_PERCENTILE_THRESHOLD,
    ) -> list[dict]:
        """
        Identify bottom decile pages by clicks.

        Args:
            pages: List of page metrics.
            percentile: Bottom percentile to identify (default: 10).

        Returns:
            List of bottom decile pages.
        """
        # Filter out pages with very low impressions (noise)
        filtered = [p for p in pages if p.get("impressions", 0) >= MIN_IMPRESSIONS_THRESHOLD]

        if not filtered:
            return []

        # Sort by clicks (ascending - worst first)
        sorted_pages = sorted(filtered, key=lambda x: x.get("clicks", 0))

        # Calculate cutoff index
        cutoff_idx = max(1, int(len(sorted_pages) * (percentile / 100)))

        return sorted_pages[:cutoff_idx]

    def generate_recommendation(self, page: dict, site_metrics: dict) -> ContentRecommendation:
        """
        Generate a recommendation for a page.

        Decision logic:
        - High impressions, low CTR, poor position -> Refresh (content is being shown but not clicked)
        - Low impressions, low clicks, any position -> Consider noindex (not getting visibility)
        - Moderate impressions, low clicks, decent position -> Consolidate (may be competing with other pages)

        Args:
            page: Page metrics dict.
            site_metrics: Overall site metrics for comparison.

        Returns:
            ContentRecommendation object.
        """
        clicks = page.get("clicks", 0)
        impressions = page.get("impressions", 0)
        ctr = page.get("ctr", 0)
        position = page.get("position", 0)
        page_url = page.get("page", "")

        site_avg_ctr = site_metrics.get("avg_ctr", 0.02)
        site_avg_position = site_metrics.get("avg_position", 20)

        # Decision logic
        recommendation = "keep"
        reason = ""
        confidence = 0.5

        # High impressions but very low CTR - needs content refresh
        if impressions > MIN_IMPRESSIONS_THRESHOLD * 5 and ctr < site_avg_ctr * 0.3:
            recommendation = "refresh"
            reason = (
                f"Getting {impressions:,} impressions but only {ctr:.2%} CTR "
                f"(site avg: {site_avg_ctr:.2%}). Title/meta description may need improvement."
            )
            confidence = 0.8

        # Very few impressions - Google isn't showing it
        elif impressions < MIN_IMPRESSIONS_THRESHOLD * 2 and clicks < 10:
            recommendation = "noindex"
            reason = (
                f"Only {impressions:,} impressions and {clicks} clicks over 90 days. "
                f"Content may be thin, duplicate, or not meeting search intent."
            )
            confidence = 0.7

        # Moderate impressions but poor position - may be cannibalizing
        elif impressions > MIN_IMPRESSIONS_THRESHOLD and position > 20 and clicks < 20:
            recommendation = "consolidate"
            reason = (
                f"Ranking at position {position:.0f} with {clicks} clicks. "
                f"Consider consolidating with similar higher-performing content."
            )
            confidence = 0.6

        # Poor position despite impressions
        elif position > 30:
            recommendation = "refresh"
            reason = (
                f"Average position {position:.0f} is too low. "
                f"Content may need significant update to compete."
            )
            confidence = 0.7

        # Low CTR relative to position
        elif position < 10 and ctr < 0.02:
            recommendation = "refresh"
            reason = (
                f"Position {position:.0f} is good but CTR is only {ctr:.2%}. "
                f"Title and meta description need optimization."
            )
            confidence = 0.8

        else:
            recommendation = "keep"
            reason = "Page is underperforming but doesn't meet clear action criteria."
            confidence = 0.4

        return ContentRecommendation(
            page=page_url,
            clicks=clicks,
            impressions=impressions,
            ctr=ctr,
            position=position,
            recommendation=recommendation,
            reason=reason,
            confidence=confidence,
        )

    def calculate_site_metrics(self, pages: list[dict]) -> dict:
        """Calculate overall site metrics for comparison."""
        if not pages:
            return {"avg_ctr": 0.02, "avg_position": 20, "total_clicks": 0, "total_impressions": 0}

        total_clicks = sum(p.get("clicks", 0) for p in pages)
        total_impressions = sum(p.get("impressions", 0) for p in pages)

        # Weighted average CTR
        ctr_sum = sum(p.get("ctr", 0) * p.get("impressions", 0) for p in pages)
        avg_ctr = ctr_sum / total_impressions if total_impressions > 0 else 0.02

        # Weighted average position
        pos_sum = sum(p.get("position", 0) * p.get("impressions", 0) for p in pages)
        avg_position = pos_sum / total_impressions if total_impressions > 0 else 20

        return {
            "avg_ctr": avg_ctr,
            "avg_position": avg_position,
            "total_clicks": total_clicks,
            "total_impressions": total_impressions,
        }

    def audit_site(
        self,
        site: dict,
        percentile: int = DEFAULT_PERCENTILE_THRESHOLD,
    ) -> SiteAudit:
        """
        Perform content audit for a single site.

        Args:
            site: Site dict with id and domain.
            percentile: Bottom percentile to identify.

        Returns:
            SiteAudit object with recommendations.
        """
        site_id = site.get("id")
        domain = site.get("domain")

        # Calculate audit period (last 90 days minus GSC lag)
        end_date = date.today() - timedelta(days=3)  # GSC lag
        start_date = end_date - timedelta(days=AUDIT_PERIOD_DAYS)

        logger.info(f"Auditing {domain} ({start_date} to {end_date})")

        # Get page metrics
        pages = self.get_page_metrics(site_id, start_date, end_date)

        if not pages:
            logger.warning(f"No data for {domain}")
            return SiteAudit(
                domain=domain,
                site_id=site_id,
                audit_period_start=start_date,
                audit_period_end=end_date,
                total_pages_analyzed=0,
                bottom_decile_count=0,
                recommendations=[],
            )

        # Calculate site metrics
        site_metrics = self.calculate_site_metrics(pages)

        # Identify bottom decile
        bottom_pages = self.identify_bottom_decile(pages, percentile)

        # Generate recommendations
        recommendations = []
        for page in bottom_pages:
            rec = self.generate_recommendation(page, site_metrics)
            recommendations.append(rec)

        # Sort by confidence (highest first)
        recommendations.sort(key=lambda x: x.confidence, reverse=True)

        return SiteAudit(
            domain=domain,
            site_id=site_id,
            audit_period_start=start_date,
            audit_period_end=end_date,
            total_pages_analyzed=len(pages),
            bottom_decile_count=len(bottom_pages),
            recommendations=recommendations,
        )

    def audit_all_sites(self, percentile: int = DEFAULT_PERCENTILE_THRESHOLD) -> list[SiteAudit]:
        """
        Audit all sites with GSC monitoring enabled.

        Args:
            percentile: Bottom percentile to identify.

        Returns:
            List of SiteAudit objects.
        """
        sites = self.get_sites_for_audit()

        if not sites:
            logger.info("No sites to audit")
            return []

        results = []
        for site in sites:
            audit = self.audit_site(site, percentile)
            results.append(audit)

        return results


def format_audit_report(audits: list[SiteAudit]) -> str:
    """Format audit results as a console report."""
    lines = [
        "=" * 80,
        "QUARTERLY CONTENT AUDIT REPORT",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "=" * 80,
        "",
    ]

    if not audits:
        lines.append("No sites audited.")
        return "\n".join(lines)

    # Summary
    total_pages = sum(a.total_pages_analyzed for a in audits)
    total_bottom = sum(a.bottom_decile_count for a in audits)

    lines.append(f"Sites Audited: {len(audits)}")
    lines.append(f"Total Pages Analyzed: {total_pages:,}")
    lines.append(f"Bottom Decile Pages: {total_bottom:,}")
    lines.append("")

    # Count recommendations by type
    rec_counts = {"noindex": 0, "consolidate": 0, "refresh": 0, "keep": 0}
    for audit in audits:
        for rec in audit.recommendations:
            rec_counts[rec.recommendation] = rec_counts.get(rec.recommendation, 0) + 1

    lines.append("Recommendations Summary:")
    lines.append(f"  - Refresh: {rec_counts['refresh']}")
    lines.append(f"  - Noindex: {rec_counts['noindex']}")
    lines.append(f"  - Consolidate: {rec_counts['consolidate']}")
    lines.append(f"  - Keep (unclear): {rec_counts['keep']}")
    lines.append("")

    # Per-site details
    for audit in sorted(audits, key=lambda x: x.bottom_decile_count, reverse=True):
        if not audit.recommendations:
            continue

        lines.append("-" * 80)
        lines.append(f"SITE: {audit.domain}")
        lines.append(f"Period: {audit.audit_period_start} to {audit.audit_period_end}")
        lines.append(f"Pages analyzed: {audit.total_pages_analyzed}, Bottom decile: {audit.bottom_decile_count}")
        lines.append("")

        # Show top recommendations (max 10)
        for i, rec in enumerate(audit.recommendations[:10], 1):
            action_icon = {
                "noindex": "[X]",
                "consolidate": "[C]",
                "refresh": "[R]",
                "keep": "[?]",
            }.get(rec.recommendation, "[?]")

            lines.append(f"  {i}. {action_icon} {rec.page[:60]}")
            lines.append(f"     Clicks: {rec.clicks}, Impressions: {rec.impressions:,}, "
                        f"CTR: {rec.ctr:.2%}, Pos: {rec.position:.0f}")
            lines.append(f"     {rec.reason}")
            lines.append("")

        if len(audit.recommendations) > 10:
            lines.append(f"  ... and {len(audit.recommendations) - 10} more")
            lines.append("")

    lines.append("=" * 80)
    lines.append("Legend: [X]=Noindex, [C]=Consolidate, [R]=Refresh, [?]=Unclear")
    lines.append("=" * 80)

    return "\n".join(lines)


def export_to_json(audits: list[SiteAudit], output_path: str) -> None:
    """Export audit results to JSON."""
    data = []
    for audit in audits:
        site_data = {
            "domain": audit.domain,
            "site_id": audit.site_id,
            "audit_period": {
                "start": audit.audit_period_start.isoformat(),
                "end": audit.audit_period_end.isoformat(),
            },
            "total_pages_analyzed": audit.total_pages_analyzed,
            "bottom_decile_count": audit.bottom_decile_count,
            "recommendations": [
                {
                    "page": rec.page,
                    "clicks": rec.clicks,
                    "impressions": rec.impressions,
                    "ctr": rec.ctr,
                    "position": rec.position,
                    "recommendation": rec.recommendation,
                    "reason": rec.reason,
                    "confidence": rec.confidence,
                }
                for rec in audit.recommendations
            ],
        }
        data.append(site_data)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Exported audit to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Quarterly content audit based on GSC data"
    )
    parser.add_argument(
        "--domain",
        help="Audit specific domain only"
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=DEFAULT_PERCENTILE_THRESHOLD,
        help=f"Bottom percentile to identify (default: {DEFAULT_PERCENTILE_THRESHOLD})"
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output JSON file path"
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
        auditor = QuarterlyAuditor()

        if args.domain:
            site = auditor.get_site_by_domain(args.domain)
            if not site:
                logger.error(f"Site not found: {args.domain}")
                return 1
            audits = [auditor.audit_site(site, args.threshold)]
        else:
            audits = auditor.audit_all_sites(args.threshold)

        # Print report
        report = format_audit_report(audits)
        print(report)

        # Export to JSON if requested
        if args.output:
            export_to_json(audits, args.output)

        return 0

    except Exception as e:
        logger.error(f"Audit failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
