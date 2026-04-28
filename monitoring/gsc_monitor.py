"""
GSC Monitor — Per-tenant Google Search Console data fetching and analysis.

Fetches GSC performance data, detects branded queries, computes WoW changes,
and generates alerts for traffic drops.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("article_factory.monitoring.gsc_monitor")

# GSC has a 2-3 day data lag; fetch data from 3 days ago
GSC_DATA_LAG_DAYS = 3

# Alert threshold: >30% WoW click drop triggers traffic_drop alert
TRAFFIC_DROP_THRESHOLD = 0.30

# Batch size for database inserts
BATCH_INSERT_SIZE = 1000


@dataclass
class GSCMetricRow:
    """A single row of GSC metrics data."""
    site_id: str
    date: date
    page: str
    query: Optional[str]
    clicks: int
    impressions: int
    ctr: Optional[float]
    position: Optional[float]
    is_branded: bool
    device: Optional[str]
    country: Optional[str]


@dataclass
class FetchResult:
    """Result of a GSC data fetch operation."""
    site_id: str
    domain: str
    fetch_date: date
    status: str  # 'success', 'partial', 'failed', 'skipped'
    rows_fetched: int = 0
    rows_inserted: int = 0
    duration_ms: int = 0
    error_message: Optional[str] = None
    alerts_generated: list = field(default_factory=list)


@dataclass
class AlertData:
    """Data for a GSC alert."""
    alert_type: str
    severity: str
    title: str
    description: str
    affected_pages: list[str] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)


class GSCMonitor:
    """
    Monitor for Google Search Console data.

    Handles per-tenant data fetching, branded query detection,
    and alert generation.
    """

    def __init__(self, gsc_client=None):
        """
        Initialize the GSC Monitor.

        Args:
            gsc_client: Optional SearchConsoleClient instance.
                        If not provided, will be created from env vars.
        """
        self.supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        self._gsc_client = gsc_client
        self._offline = not (self.supabase_url and self.supabase_key)

        if self._offline:
            logger.warning("Supabase credentials not found. Operating in offline mode.")

    @property
    def gsc_client(self):
        """Lazy-load GSC client."""
        if self._gsc_client is None:
            from onboarding.search_console import SearchConsoleClient

            sa_path = os.getenv("GSC_SERVICE_ACCOUNT_JSON") or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH")
            if not sa_path:
                raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON_PATH environment variable not set")

            self._gsc_client = SearchConsoleClient(sa_path)

        return self._gsc_client

    def _request(self, method: str, endpoint: str, data: dict | list = None, headers_extra: dict = None) -> dict | list:
        """Make a request to Supabase REST API."""
        if self._offline:
            raise ConnectionError("GSCMonitor is in offline mode (no Supabase credentials)")

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

    def get_sites_for_monitoring(self) -> list[dict]:
        """
        Get all sites with GSC monitoring enabled.

        Returns sites where:
        - gsc_status = 'verified' (GSC is set up)
        - gsc_monitoring_enabled = true

        Returns:
            List of site dicts with id, domain, brand_keywords.
        """
        try:
            data = self._request(
                "GET",
                "sites?select=id,domain,brand_keywords&gsc_status=eq.verified&gsc_monitoring_enabled=eq.true"
            )
            return data or []
        except Exception as e:
            logger.error(f"Failed to get sites for monitoring: {e}")
            return []

    def get_site_by_domain(self, domain: str) -> Optional[dict]:
        """
        Get a single site by domain.

        Args:
            domain: The domain to look up.

        Returns:
            Site dict or None if not found.
        """
        try:
            data = self._request("GET", f"sites?domain=eq.{domain}&select=id,domain,brand_keywords")
            return data[0] if data else None
        except Exception as e:
            logger.error(f"Failed to get site {domain}: {e}")
            return None

    def is_branded_query(self, query: str, brand_keywords: list[str]) -> bool:
        """
        Check if a query is branded (contains brand keywords).

        Args:
            query: The search query.
            brand_keywords: List of brand-related keywords.

        Returns:
            True if query contains any brand keyword.
        """
        if not query or not brand_keywords:
            return False

        query_lower = query.lower()
        for keyword in brand_keywords:
            if keyword.lower() in query_lower:
                return True

        return False

    def fetch_date_range(self) -> tuple[date, date]:
        """
        Get the date range to fetch (accounting for GSC data lag).

        Returns:
            Tuple of (start_date, end_date) for a single day fetch.
        """
        # Fetch data from GSC_DATA_LAG_DAYS ago
        target_date = date.today() - timedelta(days=GSC_DATA_LAG_DAYS)
        return target_date, target_date

    def fetch_gsc_data(
        self,
        domain: str,
        start_date: date,
        end_date: date,
        brand_keywords: list[str] = None,
    ) -> list[dict]:
        """
        Fetch GSC search analytics data for a domain.

        Args:
            domain: The domain to fetch data for.
            start_date: Start date for the query.
            end_date: End date for the query.
            brand_keywords: Keywords to identify branded queries.

        Returns:
            List of row dicts with GSC data.
        """
        brand_keywords = brand_keywords or []

        rows = self.gsc_client.query_search_analytics_all(
            domain=domain,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            dimensions=["page", "query", "date", "device", "country"],
        )

        # Process and enrich rows
        processed = []
        for row in rows:
            keys = row.get("keys", [])
            if len(keys) < 5:
                continue

            page, query, row_date, device, country = keys

            processed.append({
                "page": page,
                "query": query if query else None,
                "date": row_date,
                "device": device,
                "country": country,
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "ctr": row.get("ctr"),
                "position": row.get("position"),
                "is_branded": self.is_branded_query(query, brand_keywords),
            })

        return processed

    def insert_metrics(self, site_id: str, rows: list[dict]) -> int:
        """
        Insert GSC metrics into the database.

        Uses upsert (ON CONFLICT UPDATE) to handle reruns.

        Args:
            site_id: The site UUID.
            rows: List of metric row dicts.

        Returns:
            Number of rows inserted/updated.
        """
        if not rows:
            return 0

        total_inserted = 0

        # Batch insert in chunks
        for i in range(0, len(rows), BATCH_INSERT_SIZE):
            batch = rows[i:i + BATCH_INSERT_SIZE]

            # Add site_id to each row
            insert_data = [
                {
                    "site_id": site_id,
                    "date": row["date"],
                    "page": row["page"],
                    "query": row["query"],
                    "clicks": row["clicks"],
                    "impressions": row["impressions"],
                    "ctr": row.get("ctr"),
                    "position": row.get("position"),
                    "is_branded": row.get("is_branded", False),
                    "device": row.get("device"),
                    "country": row.get("country"),
                }
                for row in batch
            ]

            try:
                # Upsert with ON CONFLICT
                self._request(
                    "POST",
                    "gsc_metrics",
                    insert_data,
                    headers_extra={
                        "Prefer": "return=minimal,resolution=merge-duplicates",
                    }
                )
                total_inserted += len(batch)
            except Exception as e:
                logger.error(f"Failed to insert batch starting at {i}: {e}")

        return total_inserted

    def log_fetch(self, result: FetchResult) -> None:
        """
        Log a fetch operation to gsc_fetch_log.

        Args:
            result: The FetchResult to log.
        """
        try:
            self._request(
                "POST",
                "gsc_fetch_log",
                {
                    "site_id": result.site_id,
                    "fetch_date": result.fetch_date.isoformat(),
                    "status": result.status,
                    "rows_fetched": result.rows_fetched,
                    "rows_inserted": result.rows_inserted,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "duration_ms": result.duration_ms,
                    "error_message": result.error_message,
                },
                headers_extra={"Prefer": "return=minimal,resolution=merge-duplicates"},
            )
        except Exception as e:
            logger.error(f"Failed to log fetch result: {e}")

    def get_previous_week_metrics(self, site_id: str, reference_date: date) -> dict:
        """
        Get aggregated metrics for the previous week.

        Args:
            site_id: The site UUID.
            reference_date: The reference date (end of current period).

        Returns:
            Dict with total_clicks, total_impressions for the previous 7-day period.
        """
        # Previous week: 14 days ago to 8 days ago
        end_date = reference_date - timedelta(days=7)
        start_date = end_date - timedelta(days=6)

        try:
            # Query aggregated metrics
            data = self._request(
                "GET",
                f"gsc_metrics?site_id=eq.{site_id}"
                f"&date=gte.{start_date.isoformat()}"
                f"&date=lte.{end_date.isoformat()}"
                f"&select=clicks,impressions"
            )

            total_clicks = sum(row.get("clicks", 0) for row in data)
            total_impressions = sum(row.get("impressions", 0) for row in data)

            return {
                "total_clicks": total_clicks,
                "total_impressions": total_impressions,
                "start_date": start_date,
                "end_date": end_date,
            }
        except Exception as e:
            logger.error(f"Failed to get previous week metrics: {e}")
            return {"total_clicks": 0, "total_impressions": 0}

    def get_current_week_metrics(self, site_id: str, end_date: date) -> dict:
        """
        Get aggregated metrics for the current week.

        Args:
            site_id: The site UUID.
            end_date: The end date (typically today - GSC_DATA_LAG_DAYS).

        Returns:
            Dict with total_clicks, total_impressions for the 7-day period ending on end_date.
        """
        start_date = end_date - timedelta(days=6)

        try:
            data = self._request(
                "GET",
                f"gsc_metrics?site_id=eq.{site_id}"
                f"&date=gte.{start_date.isoformat()}"
                f"&date=lte.{end_date.isoformat()}"
                f"&select=clicks,impressions"
            )

            total_clicks = sum(row.get("clicks", 0) for row in data)
            total_impressions = sum(row.get("impressions", 0) for row in data)

            return {
                "total_clicks": total_clicks,
                "total_impressions": total_impressions,
                "start_date": start_date,
                "end_date": end_date,
            }
        except Exception as e:
            logger.error(f"Failed to get current week metrics: {e}")
            return {"total_clicks": 0, "total_impressions": 0}

    def detect_traffic_drop(self, site_id: str, domain: str, reference_date: date) -> Optional[AlertData]:
        """
        Check for significant traffic drops (>30% WoW).

        Args:
            site_id: The site UUID.
            domain: The domain for logging.
            reference_date: The date to check against.

        Returns:
            AlertData if traffic drop detected, None otherwise.
        """
        current = self.get_current_week_metrics(site_id, reference_date)
        previous = self.get_previous_week_metrics(site_id, reference_date)

        prev_clicks = previous.get("total_clicks", 0)
        curr_clicks = current.get("total_clicks", 0)

        if prev_clicks == 0:
            # No baseline data
            return None

        change_pct = (curr_clicks - prev_clicks) / prev_clicks

        if change_pct < -TRAFFIC_DROP_THRESHOLD:
            return AlertData(
                alert_type="traffic_drop",
                severity="warning",
                title=f"Traffic Drop: {domain}",
                description=(
                    f"Clicks dropped by {abs(change_pct) * 100:.1f}% week-over-week.\n"
                    f"Previous week: {prev_clicks:,} clicks\n"
                    f"Current week: {curr_clicks:,} clicks"
                ),
                metrics={
                    "previous_clicks": prev_clicks,
                    "current_clicks": curr_clicks,
                    "change_pct": round(change_pct * 100, 2),
                    "previous_period": {
                        "start": previous.get("start_date", "").isoformat() if previous.get("start_date") else None,
                        "end": previous.get("end_date", "").isoformat() if previous.get("end_date") else None,
                    },
                    "current_period": {
                        "start": current.get("start_date", "").isoformat() if current.get("start_date") else None,
                        "end": current.get("end_date", "").isoformat() if current.get("end_date") else None,
                    },
                },
            )

        return None

    def create_alert(self, site_id: str, alert: AlertData) -> Optional[str]:
        """
        Create an alert in the database.

        Args:
            site_id: The site UUID.
            alert: AlertData object.

        Returns:
            Alert ID if created, None on error.
        """
        try:
            result = self._request(
                "POST",
                "gsc_alerts",
                {
                    "site_id": site_id,
                    "alert_type": alert.alert_type,
                    "severity": alert.severity,
                    "title": alert.title,
                    "description": alert.description,
                    "affected_pages": alert.affected_pages,
                    "metrics": alert.metrics,
                },
            )
            if result and isinstance(result, list) and len(result) > 0:
                return str(result[0].get("id"))
            return None
        except Exception as e:
            logger.error(f"Failed to create alert: {e}")
            return None

    def fetch_for_site(self, site: dict) -> FetchResult:
        """
        Fetch GSC data for a single site.

        Args:
            site: Site dict with id, domain, brand_keywords.

        Returns:
            FetchResult with status and metrics.
        """
        import time

        site_id = site.get("id")
        domain = site.get("domain")
        brand_keywords = site.get("brand_keywords") or []

        start_time = time.time()
        start_date, end_date = self.fetch_date_range()

        result = FetchResult(
            site_id=site_id,
            domain=domain,
            fetch_date=end_date,
            status="pending",
        )

        try:
            logger.info(f"Fetching GSC data for {domain} ({start_date} to {end_date})")

            # Fetch data from GSC API
            rows = self.fetch_gsc_data(
                domain=domain,
                start_date=start_date,
                end_date=end_date,
                brand_keywords=brand_keywords,
            )

            result.rows_fetched = len(rows)

            # Insert into database
            if rows:
                result.rows_inserted = self.insert_metrics(site_id, rows)

            result.status = "success"

            # Check for traffic drops after we have enough data
            alert = self.detect_traffic_drop(site_id, domain, end_date)
            if alert:
                alert_id = self.create_alert(site_id, alert)
                if alert_id:
                    result.alerts_generated.append(alert)
                    logger.warning(f"Traffic drop alert created for {domain}")

        except Exception as e:
            logger.error(f"Failed to fetch GSC data for {domain}: {e}")
            result.status = "failed"
            result.error_message = str(e)

        result.duration_ms = int((time.time() - start_time) * 1000)

        # Log the fetch
        self.log_fetch(result)

        return result

    def run_daily_fetch(self) -> list[FetchResult]:
        """
        Run daily GSC fetch for all enabled sites.

        Returns:
            List of FetchResult objects for each site.
        """
        sites = self.get_sites_for_monitoring()

        if not sites:
            logger.info("No sites with GSC monitoring enabled")
            return []

        logger.info(f"Running daily GSC fetch for {len(sites)} sites")

        results = []
        for site in sites:
            result = self.fetch_for_site(site)
            results.append(result)

            if result.status == "success":
                logger.info(
                    f"  {result.domain}: {result.rows_fetched} rows fetched, "
                    f"{result.rows_inserted} inserted ({result.duration_ms}ms)"
                )
            else:
                logger.error(f"  {result.domain}: FAILED - {result.error_message}")

        # Summary
        success = sum(1 for r in results if r.status == "success")
        failed = len(results) - success
        logger.info(f"Daily fetch complete: {success} success, {failed} failed")

        return results


# CLI for testing
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="GSC Monitor CLI")
    parser.add_argument("--domain", help="Fetch data for a specific domain")
    parser.add_argument("--list-sites", action="store_true", help="List sites with GSC monitoring enabled")
    args = parser.parse_args()

    monitor = GSCMonitor()

    if args.list_sites:
        sites = monitor.get_sites_for_monitoring()
        print(f"\nSites with GSC monitoring enabled: {len(sites)}")
        for site in sites:
            print(f"  - {site.get('domain')}")

    elif args.domain:
        site = monitor.get_site_by_domain(args.domain)
        if not site:
            print(f"Site not found: {args.domain}")
        else:
            result = monitor.fetch_for_site(site)
            print(f"\nFetch result for {args.domain}:")
            print(f"  Status: {result.status}")
            print(f"  Rows fetched: {result.rows_fetched}")
            print(f"  Rows inserted: {result.rows_inserted}")
            print(f"  Duration: {result.duration_ms}ms")
            if result.error_message:
                print(f"  Error: {result.error_message}")
            if result.alerts_generated:
                print(f"  Alerts: {len(result.alerts_generated)}")

    else:
        results = monitor.run_daily_fetch()
        print(f"\nProcessed {len(results)} sites")
