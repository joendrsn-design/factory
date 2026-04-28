"""
Google Search Console and Site Verification API client.

Uses service account authentication to verify domain ownership
and add properties to Search Console.
"""
from __future__ import annotations

import logging
import time

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .errors import SearchConsoleError

logger = logging.getLogger("article_factory.onboarding.search_console")

# Required OAuth scopes
SCOPES = [
    "https://www.googleapis.com/auth/siteverification",
    "https://www.googleapis.com/auth/webmasters",
]


class SearchConsoleClient:
    """
    Client for Google Search Console and Site Verification APIs.

    Uses service account credentials for authentication. The service
    account automatically becomes Owner of any property it verifies.
    """

    def __init__(self, sa_json_path: str):
        """
        Initialize the client with service account credentials.

        Args:
            sa_json_path: Path to the service account JSON key file.
        """
        self.sa_json_path = sa_json_path
        self._credentials = None
        self._verification_service = None
        self._webmasters_service = None

    @property
    def credentials(self) -> service_account.Credentials:
        """Lazy-load service account credentials."""
        if self._credentials is None:
            try:
                self._credentials = service_account.Credentials.from_service_account_file(
                    self.sa_json_path,
                    scopes=SCOPES,
                )
            except Exception as e:
                raise SearchConsoleError(
                    f"Failed to load service account credentials: {e}\n"
                    f"  Path: {self.sa_json_path}"
                ) from e
        return self._credentials

    @property
    def verification_service(self):
        """Lazy-load Site Verification API service."""
        if self._verification_service is None:
            self._verification_service = build(
                "siteVerification",
                "v1",
                credentials=self.credentials,
                cache_discovery=False,
            )
        return self._verification_service

    @property
    def webmasters_service(self):
        """Lazy-load Search Console (Webmasters) API service."""
        if self._webmasters_service is None:
            self._webmasters_service = build(
                "searchconsole",
                "v1",
                credentials=self.credentials,
                cache_discovery=False,
            )
        return self._webmasters_service

    def get_verification_token(self, domain: str) -> str:
        """
        Get the DNS TXT verification token for a domain.

        Calls the Site Verification API to get a token that must be
        added as a TXT record to prove domain ownership.

        Args:
            domain: The domain to verify (e.g., "example.com").

        Returns:
            The TXT record value (e.g., "google-site-verification=xxx").

        Raises:
            SearchConsoleError: On API errors.
        """
        logger.info(f"Getting verification token for: {domain}")

        try:
            request_body = {
                "site": {
                    "type": "INET_DOMAIN",
                    "identifier": domain,
                },
                "verificationMethod": "DNS_TXT",
            }

            response = (
                self.verification_service.webResource()
                .getToken(body=request_body)
                .execute()
            )

            token = response.get("token", "")
            if not token:
                raise SearchConsoleError(
                    f"No verification token returned for {domain}"
                )

            logger.info(f"Got verification token: {token[:50]}...")
            return token

        except HttpError as e:
            raise SearchConsoleError(
                f"Failed to get verification token for {domain}: {e}"
            ) from e

    def verify_domain(self, domain: str, max_retries: int = 3, retry_delay: int = 30) -> None:
        """
        Verify domain ownership via DNS TXT record.

        Calls the Site Verification API to verify that the DNS TXT
        record has been set correctly. Retries on transient failures.

        Args:
            domain: The domain to verify.
            max_retries: Maximum verification attempts (default: 3).
            retry_delay: Seconds between retries (default: 30).

        Raises:
            SearchConsoleError: If verification fails after all retries.
        """
        logger.info(f"Verifying domain ownership: {domain}")

        request_body = {
            "site": {
                "type": "INET_DOMAIN",
                "identifier": domain,
            },
        }

        last_error = None

        for attempt in range(1, max_retries + 1):
            try:
                self.verification_service.webResource().insert(
                    verificationMethod="DNS_TXT",
                    body=request_body,
                ).execute()

                logger.info(f"Domain verified successfully: {domain}")
                return

            except HttpError as e:
                last_error = e
                status_code = e.resp.status if hasattr(e, "resp") else 0

                # 4xx errors (except 404) are permanent failures
                if 400 <= status_code < 500 and status_code != 404:
                    raise SearchConsoleError(
                        f"Domain verification failed for {domain}: {e}"
                    ) from e

                # Transient error, retry
                logger.warning(
                    f"Verification attempt {attempt}/{max_retries} failed: {e}"
                )

                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)

        raise SearchConsoleError(
            f"Domain verification failed after {max_retries} attempts: {domain}\n"
            f"  Last error: {last_error}"
        )

    def property_exists(self, domain: str) -> bool:
        """
        Check if a Search Console property exists for this domain.

        Args:
            domain: The domain to check (e.g., "example.com").

        Returns:
            True if the sc-domain property exists and is accessible.
        """
        site_url = f"sc-domain:{domain}"

        try:
            self.webmasters_service.sites().get(siteUrl=site_url).execute()
            return True
        except HttpError as e:
            status_code = e.resp.status if hasattr(e, "resp") else 0
            if status_code == 404 or status_code == 403:
                return False
            raise SearchConsoleError(
                f"Failed to check property existence for {domain}: {e}"
            ) from e

    def add_property(self, domain: str) -> None:
        """
        Add a Domain property to Search Console.

        Creates an sc-domain: property. The service account will
        automatically be granted Owner access.

        Args:
            domain: The domain to add (e.g., "example.com").

        Raises:
            SearchConsoleError: On API errors.
        """
        site_url = f"sc-domain:{domain}"
        logger.info(f"Adding Search Console property: {site_url}")

        try:
            self.webmasters_service.sites().add(siteUrl=site_url).execute()
            logger.info(f"Property added successfully: {site_url}")

        except HttpError as e:
            # Check if property already exists (idempotency)
            if "already exists" in str(e).lower():
                logger.info(f"Property already exists: {site_url}")
                return

            raise SearchConsoleError(
                f"Failed to add Search Console property for {domain}: {e}"
            ) from e

    def is_verified(self, domain: str) -> bool:
        """
        Check if a domain is already verified.

        Args:
            domain: The domain to check.

        Returns:
            True if the domain is verified via Site Verification API.
        """
        try:
            resources = (
                self.verification_service.webResource()
                .list()
                .execute()
            )

            for resource in resources.get("items", []):
                site = resource.get("site", {})
                if (
                    site.get("type") == "INET_DOMAIN"
                    and site.get("identifier", "").lower() == domain.lower()
                ):
                    return True

            return False

        except HttpError as e:
            logger.warning(f"Failed to check verification status: {e}")
            return False

    def submit_sitemap(self, domain: str, sitemap_path: str = "/sitemap.xml") -> None:
        """
        Submit a sitemap URL to Search Console.

        Args:
            domain: The domain (used to construct sc-domain property).
            sitemap_path: Path to sitemap (default: /sitemap.xml).

        Raises:
            SearchConsoleError: On API errors.
        """
        site_url = f"sc-domain:{domain}"
        sitemap_url = f"https://{domain}{sitemap_path}"

        logger.info(f"Submitting sitemap to Search Console: {sitemap_url}")

        try:
            self.webmasters_service.sitemaps().submit(
                siteUrl=site_url,
                feedpath=sitemap_url,
            ).execute()

            logger.info(f"Sitemap submitted successfully: {sitemap_url}")

        except HttpError as e:
            # Check for common transient errors
            if "notFound" in str(e).lower():
                raise SearchConsoleError(
                    f"Property not found when submitting sitemap: {site_url}\n"
                    f"  Ensure the property exists before submitting sitemap."
                ) from e

            raise SearchConsoleError(
                f"Failed to submit sitemap for {domain}: {e}"
            ) from e

    def list_sitemaps(self, domain: str) -> list[dict]:
        """
        List all sitemaps submitted for a domain.

        Args:
            domain: The domain to list sitemaps for.

        Returns:
            List of sitemap info dicts with keys: path, lastSubmitted, etc.
        """
        site_url = f"sc-domain:{domain}"

        try:
            response = self.webmasters_service.sitemaps().list(
                siteUrl=site_url
            ).execute()

            sitemaps = response.get("sitemap", [])
            logger.info(f"Found {len(sitemaps)} sitemaps for {domain}")
            return sitemaps

        except HttpError as e:
            logger.warning(f"Failed to list sitemaps for {domain}: {e}")
            return []

    def delete_sitemap(self, domain: str, sitemap_path: str = "/sitemap.xml") -> None:
        """
        Delete a sitemap from Search Console.

        Args:
            domain: The domain.
            sitemap_path: Path to sitemap to delete.
        """
        site_url = f"sc-domain:{domain}"
        sitemap_url = f"https://{domain}{sitemap_path}"

        logger.info(f"Deleting sitemap from Search Console: {sitemap_url}")

        try:
            self.webmasters_service.sitemaps().delete(
                siteUrl=site_url,
                feedpath=sitemap_url,
            ).execute()

            logger.info(f"Sitemap deleted: {sitemap_url}")

        except HttpError as e:
            # Ignore 404 (sitemap doesn't exist)
            if "notFound" not in str(e).lower():
                raise SearchConsoleError(
                    f"Failed to delete sitemap for {domain}: {e}"
                ) from e

    def query_search_analytics(
        self,
        domain: str,
        start_date: str,
        end_date: str,
        dimensions: list[str] = None,
        row_limit: int = 25000,
        start_row: int = 0,
        data_state: str = "final",
    ) -> list[dict]:
        """
        Query Search Analytics data from Google Search Console.

        Uses the searchanalytics.query API to fetch performance data
        including clicks, impressions, CTR, and position.

        Args:
            domain: The domain to query (e.g., "example.com").
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.
            dimensions: List of dimensions to group by. Options:
                        'query', 'page', 'country', 'device', 'date'.
                        Default: ['page', 'query', 'date'].
            row_limit: Maximum rows to return (max 25000 per request).
            start_row: Starting row for pagination.
            data_state: 'final' (default, 2-3 day lag) or 'all' (includes fresh data).

        Returns:
            List of row dicts with keys: keys[], clicks, impressions, ctr, position.
            Example: [{'keys': ['/page', 'query', '2026-04-20'], 'clicks': 10, ...}]

        Raises:
            SearchConsoleError: On API errors.
        """
        site_url = f"sc-domain:{domain}"

        if dimensions is None:
            dimensions = ["page", "query", "date"]

        logger.info(
            f"Querying Search Analytics for {domain}: "
            f"{start_date} to {end_date}, dimensions={dimensions}"
        )

        request_body = {
            "startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions,
            "rowLimit": row_limit,
            "startRow": start_row,
            "dataState": data_state,
        }

        try:
            response = (
                self.webmasters_service.searchanalytics()
                .query(siteUrl=site_url, body=request_body)
                .execute()
            )

            rows = response.get("rows", [])
            logger.info(f"Retrieved {len(rows)} rows from Search Analytics")
            return rows

        except HttpError as e:
            status_code = e.resp.status if hasattr(e, "resp") else 0
            if status_code == 403:
                raise SearchConsoleError(
                    f"Access denied to Search Analytics for {domain}. "
                    f"Ensure the service account has access to the property."
                ) from e
            raise SearchConsoleError(
                f"Failed to query Search Analytics for {domain}: {e}"
            ) from e

    def query_search_analytics_all(
        self,
        domain: str,
        start_date: str,
        end_date: str,
        dimensions: list[str] = None,
        data_state: str = "final",
    ) -> list[dict]:
        """
        Query all Search Analytics data, handling pagination automatically.

        Repeatedly calls query_search_analytics() until all data is retrieved.
        Useful for large sites that exceed the 25000 row limit.

        Args:
            domain: The domain to query.
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.
            dimensions: List of dimensions (default: ['page', 'query', 'date']).
            data_state: 'final' or 'all'.

        Returns:
            Complete list of all rows across all pages.
        """
        all_rows = []
        start_row = 0
        row_limit = 25000

        while True:
            rows = self.query_search_analytics(
                domain=domain,
                start_date=start_date,
                end_date=end_date,
                dimensions=dimensions,
                row_limit=row_limit,
                start_row=start_row,
                data_state=data_state,
            )

            all_rows.extend(rows)

            # If we got fewer rows than the limit, we're done
            if len(rows) < row_limit:
                break

            start_row += row_limit
            logger.info(f"Paginating: fetched {len(all_rows)} total rows so far...")

        logger.info(f"Total rows retrieved: {len(all_rows)}")
        return all_rows

    def get_manual_actions(self, domain: str) -> list[dict]:
        """
        Get manual actions for a domain (best-effort).

        Note: The Search Console API does not provide a direct endpoint
        for manual actions. This method attempts to check via the
        webResource API, but manual action detection may be limited.

        For reliable manual action detection, implement:
        1. Security Issues API (if available)
        2. Email notifications via Google alerts
        3. Regular manual checks in Search Console UI

        Args:
            domain: The domain to check.

        Returns:
            List of manual action dicts (may be empty if API doesn't expose them).
        """
        logger.info(f"Checking for manual actions on {domain}")

        # The GSC API doesn't directly expose manual actions.
        # This is a placeholder for potential future API support or
        # integration with Google's security issues endpoint.
        #
        # In practice, manual actions are detected via:
        # - Email notifications from Google
        # - Manual checks in GSC UI
        # - Third-party monitoring tools

        logger.warning(
            f"Manual action detection via API is limited. "
            f"Check Search Console UI for {domain} or set up email alerts."
        )

        return []

    def get_coverage_issues(
        self,
        domain: str,
        category: str = None,
    ) -> dict:
        """
        Get URL inspection / coverage summary for a domain.

        Note: Full coverage data requires the URL Inspection API which
        has rate limits. This returns a summary if available.

        Args:
            domain: The domain to check.
            category: Optional filter ('error', 'warning', 'valid', 'excluded').

        Returns:
            Dict with coverage summary information.
        """
        site_url = f"sc-domain:{domain}"

        logger.info(f"Fetching coverage summary for {domain}")

        # The Search Console API provides limited coverage data.
        # Full coverage reports require URL Inspection API with
        # significant rate limits (2000 requests/day).

        try:
            # Try to get basic site info which may include status
            response = self.webmasters_service.sites().get(siteUrl=site_url).execute()

            return {
                "site_url": site_url,
                "permission_level": response.get("permissionLevel", "unknown"),
                "note": "Full coverage data requires URL Inspection API",
            }

        except HttpError as e:
            logger.warning(f"Failed to get coverage for {domain}: {e}")
            return {"error": str(e)}
