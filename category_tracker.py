"""
============================================================
ARTICLE FACTORY — CATEGORY TRACKER
============================================================
Tracks category publication history and calculates priorities.

Features:
  1. Category Gap Analysis - Which categories need content?
  2. Quota Enforcement - Max articles per category per period
  3. Priority Hints - Tell expansion which categories to favor

Queries Supabase content table for recent publications.
============================================================
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass

import requests

logger = logging.getLogger("article_factory.category_tracker")


@dataclass
class CategoryStatus:
    """Status of a single category."""
    slug: str
    label: str
    recent_count: int        # Articles published in tracking period
    quota_max: int           # Max allowed per period (0 = unlimited)
    quota_remaining: int     # How many more can be published
    priority_score: float    # Higher = needs more content (0-100)
    is_saturated: bool       # True if at or over quota


@dataclass
class CategoryPriorities:
    """Priority information for expansion module."""
    hungry_categories: list[str]      # Categories that need content (sorted by priority)
    saturated_categories: list[str]   # Categories at quota (avoid these)
    category_scores: dict[str, float] # Priority score per category
    total_slots_available: int        # How many more articles can be published


class CategoryTracker:
    """
    Tracks category publication history and enforces quotas.

    Usage:
        tracker = CategoryTracker()
        priorities = tracker.get_priorities("lamphill", site_config)
        # Pass priorities.hungry_categories to expansion module
    """

    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not self.supabase_url or not self.supabase_key:
            logger.warning("Supabase credentials not found. CategoryTracker in offline mode.")
            self._offline = True
        else:
            self._offline = False

    def _request(self, method: str, endpoint: str, params: dict = None) -> list:
        """Make a request to Supabase REST API."""
        if self._offline:
            return []

        url = f"{self.supabase_url}/rest/v1/{endpoint}"
        headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
        }

        resp = requests.request(method, url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()

        if resp.text:
            return resp.json()
        return []

    def get_recent_publications(
        self,
        site_key: str,
        days: int = 7,
    ) -> dict[str, int]:
        """
        Get count of recent publications per category.

        Returns: {category_slug: count}
        """
        if self._offline:
            logger.warning("[category_tracker] Offline mode, returning empty counts")
            return {}

        try:
            # Calculate cutoff date
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            # Query content table joined with categories
            # We need to get category slug from the categories table
            endpoint = "content"
            params = {
                "select": "id,category_id,categories(slug)",
                "site_id": f"eq.{self._get_site_id(site_key)}",
                "status": "eq.published",
                "publish_date": f"gte.{cutoff}",
            }

            results = self._request("GET", endpoint, params)

            # Count by category
            counts = {}
            for row in results:
                cat_data = row.get("categories")
                if cat_data:
                    # Handle both single object and array from join
                    if isinstance(cat_data, list):
                        slug = cat_data[0].get("slug") if cat_data else None
                    else:
                        slug = cat_data.get("slug")

                    if slug:
                        counts[slug] = counts.get(slug, 0) + 1

            logger.info(f"[category_tracker] Recent publications for {site_key}: {counts}")
            return counts

        except Exception as e:
            logger.error(f"[category_tracker] Failed to get publications: {e}")
            return {}

    def _get_site_id(self, site_key: str) -> Optional[str]:
        """Get site UUID from site_key."""
        try:
            results = self._request("GET", "sites", {"site_key": f"eq.{site_key}", "select": "id"})
            if results:
                return results[0].get("id")
        except Exception as e:
            logger.error(f"[category_tracker] Failed to get site_id: {e}")
        return None

    def get_category_statuses(
        self,
        site_key: str,
        categories: list[dict],
        quotas: dict[str, int],
        tracking_days: int = 7,
    ) -> list[CategoryStatus]:
        """
        Get status of all categories including quota enforcement.

        Args:
            site_key: Site identifier
            categories: List of category dicts from site config
            quotas: {category_slug: max_per_period} from site config
            tracking_days: How many days back to track

        Returns: List of CategoryStatus objects
        """
        recent_counts = self.get_recent_publications(site_key, days=tracking_days)

        statuses = []
        for cat in categories:
            slug = cat.get("slug", "")
            label = cat.get("label", slug)

            recent = recent_counts.get(slug, 0)
            quota_max = quotas.get(slug, 0)  # 0 = unlimited

            if quota_max > 0:
                quota_remaining = max(0, quota_max - recent)
                is_saturated = recent >= quota_max
            else:
                quota_remaining = 999  # Unlimited
                is_saturated = False

            # Calculate priority score (higher = needs more content)
            # Categories with 0 recent articles get highest priority
            # Categories near quota get lowest priority
            if is_saturated:
                priority_score = 0
            elif quota_max > 0:
                # Score based on how much quota remains
                priority_score = (quota_remaining / quota_max) * 100
            else:
                # No quota: score based on inverse of recent count
                # 0 articles = 100, 5 articles = 50, 10+ articles = 10
                priority_score = max(10, 100 - (recent * 10))

            statuses.append(CategoryStatus(
                slug=slug,
                label=label,
                recent_count=recent,
                quota_max=quota_max,
                quota_remaining=quota_remaining,
                priority_score=priority_score,
                is_saturated=is_saturated,
            ))

        return statuses

    def get_priorities(
        self,
        site_key: str,
        site_context,  # SiteContext object
    ) -> CategoryPriorities:
        """
        Get category priorities for the expansion module.

        This is the main method to call before expansion.
        Returns prioritized lists of categories.
        """
        categories = site_context.categories

        # Get quota config from site
        quota_config = site_context.raw_config.get("category_quotas", {})
        quotas = quota_config.get("quotas", {})
        tracking_days = quota_config.get("tracking_days", 7)

        statuses = self.get_category_statuses(
            site_key=site_key,
            categories=categories,
            quotas=quotas,
            tracking_days=tracking_days,
        )

        # Sort by priority score (highest first)
        sorted_statuses = sorted(statuses, key=lambda s: s.priority_score, reverse=True)

        hungry = []
        saturated = []
        scores = {}
        total_slots = 0

        for status in sorted_statuses:
            scores[status.slug] = status.priority_score

            if status.is_saturated:
                saturated.append(status.slug)
            else:
                hungry.append(status.slug)
                total_slots += status.quota_remaining

        priorities = CategoryPriorities(
            hungry_categories=hungry,
            saturated_categories=saturated,
            category_scores=scores,
            total_slots_available=total_slots,
        )

        logger.info(f"[category_tracker] Priorities for {site_key}:")
        logger.info(f"  Hungry: {hungry[:5]}...")  # Top 5
        logger.info(f"  Saturated: {saturated}")
        logger.info(f"  Total slots: {total_slots}")

        return priorities

    def can_publish_to_category(
        self,
        site_key: str,
        category_slug: str,
        site_context,
    ) -> tuple[bool, str]:
        """
        Check if we can publish another article to this category.

        Returns: (allowed: bool, reason: str)
        """
        quota_config = site_context.raw_config.get("category_quotas", {})
        quotas = quota_config.get("quotas", {})
        tracking_days = quota_config.get("tracking_days", 7)

        quota_max = quotas.get(category_slug, 0)
        if quota_max == 0:
            return True, "No quota limit"

        recent_counts = self.get_recent_publications(site_key, days=tracking_days)
        recent = recent_counts.get(category_slug, 0)

        if recent >= quota_max:
            return False, f"Quota reached: {recent}/{quota_max} in last {tracking_days} days"

        return True, f"Quota OK: {recent}/{quota_max}"


# ── CLI for testing ─────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from site_loader import SiteLoader

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Category Tracker")
    parser.add_argument("command", choices=["status", "priorities"])
    parser.add_argument("--site", required=True, help="Site key")
    parser.add_argument("--days", type=int, default=7, help="Tracking period in days")

    args = parser.parse_args()

    tracker = CategoryTracker()
    loader = SiteLoader()
    site_context = loader.load(args.site)

    if args.command == "status":
        quota_config = site_context.raw_config.get("category_quotas", {})
        statuses = tracker.get_category_statuses(
            args.site,
            site_context.categories,
            quota_config.get("quotas", {}),
            args.days,
        )

        print(f"\nCategory Status for {args.site} (last {args.days} days):")
        print("-" * 70)
        print(f"{'Category':<20} {'Recent':<8} {'Quota':<10} {'Priority':<10} {'Status'}")
        print("-" * 70)

        for s in sorted(statuses, key=lambda x: x.priority_score, reverse=True):
            quota_str = f"{s.recent_count}/{s.quota_max}" if s.quota_max > 0 else f"{s.recent_count}/∞"
            status_str = "🔴 SATURATED" if s.is_saturated else "🟢 OK"
            print(f"{s.slug:<20} {s.recent_count:<8} {quota_str:<10} {s.priority_score:<10.1f} {status_str}")

    elif args.command == "priorities":
        priorities = tracker.get_priorities(args.site, site_context)

        print(f"\nCategory Priorities for {args.site}:")
        print("-" * 50)
        print(f"Total slots available: {priorities.total_slots_available}")
        print(f"\nHungry categories (need content):")
        for i, cat in enumerate(priorities.hungry_categories[:10], 1):
            score = priorities.category_scores.get(cat, 0)
            print(f"  {i}. {cat} (score: {score:.1f})")

        if priorities.saturated_categories:
            print(f"\nSaturated categories (at quota):")
            for cat in priorities.saturated_categories:
                print(f"  ❌ {cat}")
