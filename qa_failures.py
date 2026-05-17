"""
============================================================
ARTICLE FACTORY — QA FAILURES TRACKER
============================================================
Records articles that fail QA (KILL or exceeded max rewrites)
to Supabase for admin review, editing, and republishing.

Usage:
    from qa_failures import QAFailureTracker

    tracker = QAFailureTracker()

    # Record a failed article
    tracker.record_failure(
        metadata={
            "article_id": "art_abc123",
            "run_id": "run_20260411_064817_xyz",
            "site_id": "lamphill",
            "title": "Article Title",
            "slug": "article-slug",
            "category": "supplements",
            "meta_description": "...",
            "tags": ["magnesium", "sleep"],
            "sources": [...]
        },
        body="# Article content...",
        verdict="KILL",
        score=4.5,
        scores_breakdown={"voice": 8, "accuracy": 3, ...},
        feedback="The article contains factual errors...",
        rewrite_instructions="Fix the dosage claims...",
        rewrite_count=2,
    )

    # Get pending failures for admin review
    failures = tracker.get_pending(site_key="lamphill", limit=50)

    # Update failure status (after admin edits)
    tracker.update_status(failure_id="uuid", status="republished")
============================================================
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("article_factory.qa_failures")


class QAFailureTracker:
    """
    Interface to the qa_failures table in Supabase.
    Records articles that fail QA for admin review and recovery.
    """

    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not self.supabase_url or not self.supabase_key:
            logger.warning("Supabase credentials not found. QAFailureTracker will operate in offline mode.")
            self._offline = True
        else:
            self._offline = False

    def _request(self, method: str, endpoint: str, data: dict = None) -> dict:
        """Make a request to Supabase REST API."""
        if self._offline:
            logger.warning("QAFailureTracker is offline. Skipping request.")
            return {}

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

    def record_failure(
        self,
        metadata: dict,
        body: str,
        verdict: str,
        score: float,
        scores_breakdown: dict = None,
        feedback: str = None,
        rewrite_instructions: str = None,
        rewrite_count: int = 0,
    ) -> bool:
        """
        Record a failed article to qa_failures table.

        Args:
            metadata: Article metadata dict with article_id, run_id, site_id, title, etc.
            body: Article body content
            verdict: QA verdict (KILL or REWRITE)
            score: QA composite score
            scores_breakdown: Individual dimension scores
            feedback: QA feedback text
            rewrite_instructions: Instructions for rewriting
            rewrite_count: Number of rewrite attempts made

        Returns:
            True if recorded successfully, False otherwise
        """
        if self._offline:
            logger.warning("QAFailureTracker is offline. Cannot record failure.")
            return False

        try:
            article_id = metadata.get("article_id", "")
            run_id = metadata.get("run_id", "")
            site_key = metadata.get("site_id", "") or metadata.get("site_key", "")

            if not article_id or not site_key:
                logger.error("Cannot record failure: missing article_id or site_key")
                return False

            # Build record
            record = {
                "article_id": article_id,
                "run_id": run_id,
                "site_key": site_key,
                "verdict": verdict.upper(),
                "qa_score": score,
                "scores_breakdown": scores_breakdown or {},
                "feedback": feedback,
                "rewrite_instructions": rewrite_instructions,
                "rewrite_count": rewrite_count,
                "title": metadata.get("title", "Untitled"),
                "slug": metadata.get("slug"),
                "body": body,
                "meta_description": metadata.get("meta_description"),
                "category": metadata.get("category"),
                "tags": metadata.get("tags", []),
                "sources": metadata.get("sources", []),
                "status": "pending",
                "failed_at": datetime.now(timezone.utc).isoformat(),
            }

            # Upsert (update if article_id exists, insert otherwise)
            self._request(
                "POST",
                "qa_failures?on_conflict=article_id",
                record
            )

            logger.info(f"Recorded QA failure: {article_id} ({verdict}, score={score})")
            return True

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error recording QA failure: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to record QA failure: {e}")
            return False

    def get_pending(self, site_key: str = None, limit: int = 50) -> list[dict]:
        """
        Get pending failures for admin review.

        Args:
            site_key: Optional site key to filter by
            limit: Maximum number of results

        Returns:
            List of failure records
        """
        if self._offline:
            return []

        try:
            endpoint = f"qa_failures?status=eq.pending&order=failed_at.desc&limit={limit}"
            if site_key:
                endpoint += f"&site_key=eq.{site_key}"

            data = self._request("GET", endpoint)
            return data if isinstance(data, list) else []

        except Exception as e:
            logger.error(f"Failed to get pending failures: {e}")
            return []

    def get_by_id(self, failure_id: str) -> Optional[dict]:
        """
        Get a single failure record by ID.

        Args:
            failure_id: UUID of the failure record

        Returns:
            Failure record dict or None
        """
        if self._offline:
            return None

        try:
            data = self._request("GET", f"qa_failures?id=eq.{failure_id}")
            if data and len(data) > 0:
                return data[0]
            return None

        except Exception as e:
            logger.error(f"Failed to get failure {failure_id}: {e}")
            return None

    def get_by_article_id(self, article_id: str) -> Optional[dict]:
        """
        Get a failure record by article_id.

        Args:
            article_id: The article's unique ID

        Returns:
            Failure record dict or None
        """
        if self._offline:
            return None

        try:
            data = self._request("GET", f"qa_failures?article_id=eq.{article_id}")
            if data and len(data) > 0:
                return data[0]
            return None

        except Exception as e:
            logger.error(f"Failed to get failure for article {article_id}: {e}")
            return None

    def update_status(
        self,
        failure_id: str,
        status: str,
        admin_notes: str = None,
        reviewed_by: str = None,
    ) -> bool:
        """
        Update the status of a failure record.

        Args:
            failure_id: UUID of the failure record
            status: New status (pending, editing, republished, archived)
            admin_notes: Optional admin notes
            reviewed_by: Optional reviewer identifier

        Returns:
            True if updated successfully, False otherwise
        """
        if self._offline:
            return False

        try:
            updates = {"status": status}
            if admin_notes is not None:
                updates["admin_notes"] = admin_notes
            if reviewed_by is not None:
                updates["reviewed_by"] = reviewed_by

            self._request("PATCH", f"qa_failures?id=eq.{failure_id}", updates)
            logger.info(f"Updated failure {failure_id} status to {status}")
            return True

        except Exception as e:
            logger.error(f"Failed to update failure {failure_id}: {e}")
            return False

    def update_content(
        self,
        failure_id: str,
        title: str = None,
        body: str = None,
        slug: str = None,
        meta_description: str = None,
        category: str = None,
        tags: list = None,
    ) -> bool:
        """
        Update the content of a failure record (for admin editing).

        Args:
            failure_id: UUID of the failure record
            title: New title
            body: New body content
            slug: New slug
            meta_description: New meta description
            category: New category
            tags: New tags list

        Returns:
            True if updated successfully, False otherwise
        """
        if self._offline:
            return False

        try:
            updates = {}
            if title is not None:
                updates["title"] = title
            if body is not None:
                updates["body"] = body
            if slug is not None:
                updates["slug"] = slug
            if meta_description is not None:
                updates["meta_description"] = meta_description
            if category is not None:
                updates["category"] = category
            if tags is not None:
                updates["tags"] = tags

            if not updates:
                return True

            self._request("PATCH", f"qa_failures?id=eq.{failure_id}", updates)
            logger.info(f"Updated failure {failure_id} content")
            return True

        except Exception as e:
            logger.error(f"Failed to update failure content {failure_id}: {e}")
            return False

    def count_pending(self, site_key: str = None) -> int:
        """
        Count pending failures.

        Args:
            site_key: Optional site key to filter by

        Returns:
            Number of pending failures
        """
        if self._offline:
            return 0

        try:
            endpoint = "qa_failures?status=eq.pending&select=id"
            if site_key:
                endpoint += f"&site_key=eq.{site_key}"

            # Use HEAD request with Prefer header to get count
            url = f"{self.supabase_url}/rest/v1/{endpoint}"
            headers = {
                "apikey": self.supabase_key,
                "Authorization": f"Bearer {self.supabase_key}",
                "Prefer": "count=exact",
            }

            resp = requests.head(url, headers=headers, timeout=30)
            resp.raise_for_status()

            # Count is in Content-Range header: "0-N/total"
            content_range = resp.headers.get("Content-Range", "")
            if "/" in content_range:
                return int(content_range.split("/")[1])
            return 0

        except Exception as e:
            logger.error(f"Failed to count pending failures: {e}")
            return 0

    def delete(self, failure_id: str) -> bool:
        """
        Delete a failure record.

        Args:
            failure_id: UUID of the failure record

        Returns:
            True if deleted successfully, False otherwise
        """
        if self._offline:
            return False

        try:
            self._request("DELETE", f"qa_failures?id=eq.{failure_id}")
            logger.info(f"Deleted failure {failure_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete failure {failure_id}: {e}")
            return False


# ── CLI ─────────────────────────────────────────────────────

def main():
    """CLI for QA failures operations."""
    import argparse
    import json

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="QA Failures Tracker")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # list command
    list_parser = subparsers.add_parser("list", help="List pending failures")
    list_parser.add_argument("--site", help="Filter by site key")
    list_parser.add_argument("--limit", type=int, default=20, help="Max results")

    # count command
    count_parser = subparsers.add_parser("count", help="Count pending failures")
    count_parser.add_argument("--site", help="Filter by site key")

    # get command
    get_parser = subparsers.add_parser("get", help="Get failure details")
    get_parser.add_argument("id", help="Failure ID (UUID)")

    # status command
    status_parser = subparsers.add_parser("status", help="Update failure status")
    status_parser.add_argument("id", help="Failure ID (UUID)")
    status_parser.add_argument("new_status", choices=["pending", "editing", "republished", "archived"])
    status_parser.add_argument("--notes", help="Admin notes")

    args = parser.parse_args()
    tracker = QAFailureTracker()

    if args.command == "list":
        failures = tracker.get_pending(site_key=args.site, limit=args.limit)
        if not failures:
            print("No pending failures")
            return

        print(f"\n{'ID':<36} {'Site':<15} {'Title':<40} {'Score':<8} {'Verdict'}")
        print("-" * 110)
        for f in failures:
            title = f.get("title", "")[:38]
            print(f"{f.get('id', ''):<36} {f.get('site_key', ''):<15} {title:<40} {f.get('qa_score', 0):<8.1f} {f.get('verdict', '')}")

    elif args.command == "count":
        count = tracker.count_pending(site_key=args.site)
        site_str = f" for {args.site}" if args.site else ""
        print(f"Pending failures{site_str}: {count}")

    elif args.command == "get":
        failure = tracker.get_by_id(args.id)
        if not failure:
            print(f"Failure not found: {args.id}")
            return

        print(f"\nFailure: {failure.get('id')}")
        print(f"Article: {failure.get('article_id')}")
        print(f"Site: {failure.get('site_key')}")
        print(f"Title: {failure.get('title')}")
        print(f"Verdict: {failure.get('verdict')} (score: {failure.get('qa_score')})")
        print(f"Status: {failure.get('status')}")
        print(f"Rewrite count: {failure.get('rewrite_count')}")
        print(f"\nFeedback:\n{failure.get('feedback', 'N/A')}")
        if failure.get("rewrite_instructions"):
            print(f"\nRewrite instructions:\n{failure.get('rewrite_instructions')}")
        print(f"\nBody preview:\n{failure.get('body', '')[:500]}...")

    elif args.command == "status":
        if tracker.update_status(args.id, args.new_status, admin_notes=args.notes):
            print(f"Updated {args.id} to {args.new_status}")
        else:
            print(f"Failed to update {args.id}")


if __name__ == "__main__":
    main()
