"""
============================================================
ARTICLE FACTORY — SITE REGISTRY
============================================================
Tracks factory run state for each site in Supabase.

Usage:
    from registry import Registry

    reg = Registry()

    # Get all active sites due for a run
    due_sites = reg.get_due_sites()

    # Get single site status
    status = reg.get_site("lamphill")

    # Record a completed run
    reg.record_run(
        site_key="lamphill",
        run_id="run_20260411_064817_25d6544f",
        articles_generated=3,
        articles_published=2,
        articles_killed=1,
        cost_cents=186,
        duration_seconds=120,
    )

    # Pause a site
    reg.set_status("lamphill", "paused")

    # List all sites
    sites = reg.list_sites()
============================================================
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from dataclasses import dataclass

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("article_factory.registry")

# Frequency to timedelta mapping
FREQUENCY_INTERVALS = {
    "hourly": timedelta(hours=1),
    "twice_daily": timedelta(hours=12),
    "daily": timedelta(days=1),
    "twice_weekly": timedelta(days=3.5),
    "weekly": timedelta(weeks=1),
    "biweekly": timedelta(weeks=2),
    "monthly": timedelta(days=30),
    "manual": None,  # Never auto-scheduled
}


@dataclass
class SiteStatus:
    """Registry entry for a site."""
    site_key: str
    run_frequency: str
    articles_per_run: int
    last_run_at: Optional[datetime]
    next_run_at: Optional[datetime]
    status: str
    last_error: Optional[str]
    consecutive_failures: int
    total_runs: int
    total_articles_generated: int
    total_articles_published: int
    total_articles_killed: int
    total_rewrites: int
    total_cost_cents: int

    @property
    def is_due(self) -> bool:
        """Check if site is due for a run."""
        if self.status != "active":
            return False
        if self.next_run_at is None:
            return True
        return datetime.now(timezone.utc) >= self.next_run_at

    @property
    def total_cost_dollars(self) -> float:
        """Total cost in dollars."""
        return self.total_cost_cents / 100


class Registry:
    """
    Interface to the factory_registry table in Supabase.
    """

    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not self.supabase_url or not self.supabase_key:
            logger.warning("Supabase credentials not found. Registry will operate in offline mode.")
            self._offline = True
        else:
            self._offline = False

    def _request(self, method: str, endpoint: str, data: dict = None) -> dict:
        """Make a request to Supabase REST API."""
        if self._offline:
            raise ConnectionError("Registry is in offline mode (no Supabase credentials)")

        url = f"{self.supabase_url}/rest/v1/{endpoint}"
        headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

        resp = requests.request(method, url, headers=headers, json=data, timeout=30)
        resp.raise_for_status()

        if resp.text:
            return resp.json()
        return {}

    def get_site(self, site_key: str) -> Optional[SiteStatus]:
        """Get registry entry for a site."""
        try:
            data = self._request("GET", f"factory_registry?site_key=eq.{site_key}")
            if not data:
                return None
            return self._parse_status(data[0])
        except Exception as e:
            logger.error(f"Failed to get site {site_key}: {e}")
            return None

    def list_sites(self, status: str = None) -> list[SiteStatus]:
        """List all registered sites, optionally filtered by status."""
        try:
            endpoint = "factory_registry?order=site_key"
            if status:
                endpoint += f"&status=eq.{status}"
            data = self._request("GET", endpoint)
            return [self._parse_status(row) for row in data]
        except Exception as e:
            logger.error(f"Failed to list sites: {e}")
            return []

    def get_due_sites(self) -> list[SiteStatus]:
        """Get all active sites that are due for a run."""
        now = datetime.now(timezone.utc).isoformat()
        try:
            # Get active sites where next_run_at is null or in the past
            data = self._request(
                "GET",
                f"factory_registry?status=eq.active&or=(next_run_at.is.null,next_run_at.lte.{now})&order=next_run_at.nullsfirst"
            )
            return [self._parse_status(row) for row in data]
        except Exception as e:
            logger.error(f"Failed to get due sites: {e}")
            return []

    def register_site(
        self,
        site_key: str,
        run_frequency: str = "daily",
        articles_per_run: int = 1,
        status: str = "active",
    ) -> bool:
        """Register a new site or update existing."""
        try:
            # Calculate next run time
            next_run = self._calculate_next_run(run_frequency)

            self._request("POST", "factory_registry", {
                "site_key": site_key,
                "run_frequency": run_frequency,
                "articles_per_run": articles_per_run,
                "status": status,
                "next_run_at": next_run.isoformat() if next_run else None,
            })
            logger.info(f"Registered site: {site_key}")
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:  # Conflict - already exists
                # Update instead
                return self.update_site(site_key, run_frequency=run_frequency,
                                        articles_per_run=articles_per_run, status=status)
            logger.error(f"Failed to register site {site_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to register site {site_key}: {e}")
            return False

    def update_site(
        self,
        site_key: str,
        run_frequency: str = None,
        articles_per_run: int = None,
        status: str = None,
    ) -> bool:
        """Update site settings."""
        try:
            updates = {}
            if run_frequency is not None:
                updates["run_frequency"] = run_frequency
            if articles_per_run is not None:
                updates["articles_per_run"] = articles_per_run
            if status is not None:
                updates["status"] = status

            if not updates:
                return True

            self._request("PATCH", f"factory_registry?site_key=eq.{site_key}", updates)
            logger.info(f"Updated site {site_key}: {updates}")
            return True
        except Exception as e:
            logger.error(f"Failed to update site {site_key}: {e}")
            return False

    def set_status(self, site_key: str, status: str) -> bool:
        """Set site status (active, paused, archived)."""
        return self.update_site(site_key, status=status)

    def record_run(
        self,
        site_key: str,
        run_id: str,
        status: str = "success",
        articles_generated: int = 0,
        articles_published: int = 0,
        articles_rewritten: int = 0,
        articles_killed: int = 0,
        cost_cents: int = 0,
        duration_seconds: int = 0,
        error_message: str = None,
    ) -> bool:
        """Record a completed run and update registry."""
        try:
            now = datetime.now(timezone.utc)

            # Get current site data
            site = self.get_site(site_key)
            if not site:
                logger.error(f"Site {site_key} not found in registry")
                return False

            # Calculate next run
            next_run = self._calculate_next_run(site.run_frequency, now)

            # Update registry
            updates = {
                "last_run_at": now.isoformat(),
                "next_run_at": next_run.isoformat() if next_run else None,
                "total_runs": site.total_runs + 1,
                "total_articles_generated": site.total_articles_generated + articles_generated,
                "total_articles_published": site.total_articles_published + articles_published,
                "total_articles_killed": site.total_articles_killed + articles_killed,
                "total_rewrites": site.total_rewrites + articles_rewritten,
                "total_cost_cents": site.total_cost_cents + cost_cents,
            }

            if status == "success":
                updates["consecutive_failures"] = 0
                updates["last_error"] = None
            else:
                updates["consecutive_failures"] = site.consecutive_failures + 1
                updates["last_error"] = error_message

            self._request("PATCH", f"factory_registry?site_key=eq.{site_key}", updates)

            # Record in factory_runs history
            self._request("POST", "factory_runs", {
                "site_key": site_key,
                "run_id": run_id,
                "status": status,
                "articles_generated": articles_generated,
                "articles_published": articles_published,
                "articles_rewritten": articles_rewritten,
                "articles_killed": articles_killed,
                "cost_cents": cost_cents,
                "started_at": (now - timedelta(seconds=duration_seconds)).isoformat(),
                "completed_at": now.isoformat(),
                "duration_seconds": duration_seconds,
                "error_message": error_message,
            })

            logger.info(f"Recorded run {run_id} for {site_key}: {status}")
            return True

        except Exception as e:
            logger.error(f"Failed to record run for {site_key}: {e}")
            return False

    def record_failure(self, site_key: str, run_id: str, error_message: str, duration_seconds: int = 0) -> bool:
        """Convenience method to record a failed run."""
        return self.record_run(
            site_key=site_key,
            run_id=run_id,
            status="failed",
            error_message=error_message,
            duration_seconds=duration_seconds,
        )

    def get_run_history(self, site_key: str, limit: int = 10) -> list[dict]:
        """Get recent run history for a site."""
        try:
            data = self._request(
                "GET",
                f"factory_runs?site_key=eq.{site_key}&order=created_at.desc&limit={limit}"
            )
            return data
        except Exception as e:
            logger.error(f"Failed to get run history for {site_key}: {e}")
            return []

    def _calculate_next_run(self, frequency: str, from_time: datetime = None) -> Optional[datetime]:
        """Calculate the next run time based on frequency."""
        if frequency == "manual":
            return None

        interval = FREQUENCY_INTERVALS.get(frequency)
        if not interval:
            logger.warning(f"Unknown frequency '{frequency}', defaulting to daily")
            interval = FREQUENCY_INTERVALS["daily"]

        base = from_time or datetime.now(timezone.utc)
        return base + interval

    def _parse_status(self, row: dict) -> SiteStatus:
        """Parse a database row into SiteStatus."""
        return SiteStatus(
            site_key=row.get("site_key", ""),
            run_frequency=row.get("run_frequency", "daily"),
            articles_per_run=row.get("articles_per_run", 1),
            last_run_at=self._parse_datetime(row.get("last_run_at")),
            next_run_at=self._parse_datetime(row.get("next_run_at")),
            status=row.get("status", "active"),
            last_error=row.get("last_error"),
            consecutive_failures=row.get("consecutive_failures", 0),
            total_runs=row.get("total_runs", 0),
            total_articles_generated=row.get("total_articles_generated", 0),
            total_articles_published=row.get("total_articles_published", 0),
            total_articles_killed=row.get("total_articles_killed", 0),
            total_rewrites=row.get("total_rewrites", 0),
            total_cost_cents=row.get("total_cost_cents", 0),
        )

    def _parse_datetime(self, value: str) -> Optional[datetime]:
        """Parse ISO datetime string."""
        if not value:
            return None
        try:
            # Handle various ISO formats
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            return datetime.fromisoformat(value)
        except ValueError:
            return None


# ── CLI ─────────────────────────────────────────────────────

def main():
    """CLI for registry operations."""
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Factory site registry")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # list command
    list_parser = subparsers.add_parser("list", help="List all registered sites")
    list_parser.add_argument("--status", help="Filter by status")

    # status command
    status_parser = subparsers.add_parser("status", help="Get site status")
    status_parser.add_argument("site_key", help="Site key")

    # due command
    subparsers.add_parser("due", help="List sites due for a run")

    # register command
    reg_parser = subparsers.add_parser("register", help="Register a site")
    reg_parser.add_argument("site_key", help="Site key")
    reg_parser.add_argument("--frequency", default="daily", help="Run frequency")
    reg_parser.add_argument("--articles", type=int, default=1, help="Articles per run")

    # pause/resume commands
    pause_parser = subparsers.add_parser("pause", help="Pause a site")
    pause_parser.add_argument("site_key", help="Site key")

    resume_parser = subparsers.add_parser("resume", help="Resume a site")
    resume_parser.add_argument("site_key", help="Site key")

    # history command
    hist_parser = subparsers.add_parser("history", help="Show run history")
    hist_parser.add_argument("site_key", help="Site key")
    hist_parser.add_argument("--limit", type=int, default=10, help="Number of runs")

    args = parser.parse_args()
    reg = Registry()

    if args.command == "list":
        sites = reg.list_sites(args.status)
        if not sites:
            print("No sites registered")
            return

        print(f"\n{'Site':<20} {'Status':<10} {'Frequency':<12} {'Last Run':<20} {'Next Run':<20}")
        print("-" * 85)
        for s in sites:
            last = s.last_run_at.strftime("%Y-%m-%d %H:%M") if s.last_run_at else "never"
            next_r = s.next_run_at.strftime("%Y-%m-%d %H:%M") if s.next_run_at else "not scheduled"
            print(f"{s.site_key:<20} {s.status:<10} {s.run_frequency:<12} {last:<20} {next_r:<20}")

    elif args.command == "status":
        s = reg.get_site(args.site_key)
        if not s:
            print(f"Site '{args.site_key}' not found")
            return

        print(f"\nSite: {s.site_key}")
        print(f"Status: {s.status}")
        print(f"Frequency: {s.run_frequency} ({s.articles_per_run} articles/run)")
        print(f"Last run: {s.last_run_at or 'never'}")
        print(f"Next run: {s.next_run_at or 'not scheduled'}")
        print(f"Is due: {s.is_due}")
        print(f"\nMetrics:")
        print(f"  Total runs: {s.total_runs}")
        print(f"  Articles: {s.total_articles_generated} generated, {s.total_articles_published} published, {s.total_articles_killed} killed")
        print(f"  Rewrites: {s.total_rewrites}")
        print(f"  Total cost: ${s.total_cost_dollars:.2f}")
        if s.last_error:
            print(f"  Last error: {s.last_error}")
            print(f"  Consecutive failures: {s.consecutive_failures}")

    elif args.command == "due":
        sites = reg.get_due_sites()
        if not sites:
            print("No sites due for a run")
            return

        print(f"\nSites due for a run ({len(sites)}):")
        for s in sites:
            print(f"  - {s.site_key} ({s.articles_per_run} articles)")

    elif args.command == "register":
        if reg.register_site(args.site_key, args.frequency, args.articles):
            print(f"Registered: {args.site_key}")
        else:
            print(f"Failed to register: {args.site_key}")

    elif args.command == "pause":
        if reg.set_status(args.site_key, "paused"):
            print(f"Paused: {args.site_key}")
        else:
            print(f"Failed to pause: {args.site_key}")

    elif args.command == "resume":
        if reg.set_status(args.site_key, "active"):
            print(f"Resumed: {args.site_key}")
        else:
            print(f"Failed to resume: {args.site_key}")

    elif args.command == "history":
        runs = reg.get_run_history(args.site_key, args.limit)
        if not runs:
            print(f"No run history for {args.site_key}")
            return

        print(f"\nRun history for {args.site_key}:")
        print(f"{'Run ID':<35} {'Status':<10} {'Articles':<10} {'Cost':<10} {'Duration':<10}")
        print("-" * 80)
        for r in runs:
            cost = f"${r.get('cost_cents', 0) / 100:.2f}"
            dur = f"{r.get('duration_seconds', 0)}s"
            arts = f"{r.get('articles_published', 0)}/{r.get('articles_generated', 0)}"
            print(f"{r.get('run_id', ''):<35} {r.get('status', ''):<10} {arts:<10} {cost:<10} {dur:<10}")


if __name__ == "__main__":
    main()
