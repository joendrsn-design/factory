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
