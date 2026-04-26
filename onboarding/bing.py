"""
Bing Webmaster Tools API client.

Handles site verification and sitemap submission for Bing/DuckDuckGo.
Uses the Bing Webmaster API: https://www.bing.com/webmasters/help/webmaster-api
"""
from __future__ import annotations

import logging
import time
from typing import Any

import requests

from .errors import BingWebmasterError

logger = logging.getLogger("article_factory.onboarding.bing")

API_BASE = "https://ssl.bing.com/webmaster/api.svc/json"


class BingWebmasterClient:
    """
    Client for Bing Webmaster Tools API.

    Provides site verification, sitemap submission, and URL submission
    for Bing and DuckDuckGo search engines.
    """

    def __init__(self, api_key: str):
        """
        Initialize the Bing Webmaster client.

        Args:
            api_key: Bing Webmaster API key.
                    Get from: https://www.bing.com/webmasters/apikey
        """
        self.api_key = api_key

    def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list | None:
        """
        Make a request to the Bing Webmaster API.

        Args:
            method: HTTP method (GET, POST).
            endpoint: API endpoint (e.g., "GetSites").
            params: Query parameters.
            json_data: JSON body for POST requests.

        Returns:
            Parsed JSON response or None.

        Raises:
            BingWebmasterError: On API errors.
        """
        url = f"{API_BASE}/{endpoint}"

        # API key goes in query params
        if params is None:
            params = {}
        params["apikey"] = self.api_key

        logger.debug(f"Bing API: {method} {endpoint}")

        try:
            if method.upper() == "GET":
                response = requests.get(url, params=params, timeout=30)
            else:
                response = requests.post(url, params=params, json=json_data, timeout=30)

            # Bing API returns empty response on some successful operations
            if response.status_code == 200:
                if response.text:
                    return response.json()
                return None

            # Handle errors
            error_msg = response.text or f"HTTP {response.status_code}"
            raise BingWebmasterError(
                f"Bing API error: {error_msg}\n"
                f"  Endpoint: {endpoint}"
            )

        except requests.RequestException as e:
            raise BingWebmasterError(f"Bing API request failed: {e}") from e

    def get_sites(self) -> list[dict]:
        """
        Get all sites registered in Bing Webmaster Tools.

        Returns:
            List of site dicts with Url, SiteAuthStatus, etc.
        """
        result = self._request("GET", "GetUserSites")
        if result and isinstance(result, dict) and "d" in result:
            return result["d"]
        return result if isinstance(result, list) else []

    def site_exists(self, domain: str) -> bool:
        """
        Check if a site is already registered.

        Args:
            domain: The domain to check (e.g., "example.com").

        Returns:
            True if the site exists in Bing Webmaster Tools.
        """
        site_url = f"https://{domain}/"
        sites = self.get_sites()

        for site in sites:
            if site.get("Url", "").lower().rstrip("/") == site_url.lower().rstrip("/"):
                return True
        return False

    def add_site(self, domain: str) -> None:
        """
        Add a site to Bing Webmaster Tools.

        Args:
            domain: The domain to add (e.g., "example.com").

        Raises:
            BingWebmasterError: On API errors.
        """
        site_url = f"https://{domain}/"
        logger.info(f"Adding site to Bing Webmaster Tools: {site_url}")

        try:
            # Bing AddSite uses POST with siteUrl in JSON body
            self._request("POST", "AddSite", json_data={"siteUrl": site_url})
            logger.info(f"Site added: {site_url}")
        except BingWebmasterError as e:
            # Check if site already exists (idempotency)
            if "already" in str(e).lower() or "exists" in str(e).lower():
                logger.info(f"Site already exists: {site_url}")
                return
            raise

    def _get_site_info(self, domain: str) -> dict | None:
        """
        Get site info from GetUserSites by matching domain.

        Args:
            domain: The domain to find.

        Returns:
            Site dict with Url, AuthenticationCode, IsVerified, etc. or None.
        """
        site_url = f"https://{domain}/"
        sites = self.get_sites()

        for site in sites:
            if site.get("Url", "").lower().rstrip("/") == site_url.lower().rstrip("/"):
                return site
        return None

    def get_verification_token(self, domain: str) -> str:
        """
        Get the DNS TXT verification token for a site.

        The token should be added as a TXT record at the root domain.
        Format: BingSiteAuth (not wrapped in quotes).

        Args:
            domain: The domain to get verification for.

        Returns:
            The verification code (to be used as BingSiteAuth TXT value).

        Note:
            The actual TXT record format is: BingSiteAuth authentication_code
            So the full TXT value would be like: BingSiteAuth 1234567890ABCDEF
        """
        logger.info(f"Getting Bing verification token for: {domain}")

        # First, ensure site is added
        if not self.site_exists(domain):
            self.add_site(domain)

        # Get site details from GetUserSites
        site_info = self._get_site_info(domain)

        if site_info:
            # Try AuthenticationCode first, then DnsVerificationCode
            auth_code = site_info.get("AuthenticationCode") or site_info.get("DnsVerificationCode")
            if auth_code:
                logger.info(f"Got Bing verification code: {auth_code}")
                return auth_code

        raise BingWebmasterError(
            f"Could not retrieve Bing verification token for {domain}\n"
            f"  Visit https://www.bing.com/webmasters/siteauth to get the verification code manually."
        )

    def verify_site(self, domain: str, max_retries: int = 3, retry_delay: int = 30) -> bool:
        """
        Verify site ownership in Bing Webmaster Tools.

        Call this after adding the BingSiteAuth DNS TXT record.

        Args:
            domain: The domain to verify.
            max_retries: Number of verification attempts.
            retry_delay: Seconds between retries.

        Returns:
            True if verification succeeded.

        Raises:
            BingWebmasterError: If verification fails after all retries.
        """
        site_url = f"https://{domain}/"
        logger.info(f"Verifying site in Bing: {site_url}")

        for attempt in range(1, max_retries + 1):
            try:
                # Trigger verification check (POST with siteUrl in body)
                self._request("POST", "VerifySite", json_data={"siteUrl": site_url})

                # Check status from GetUserSites
                site_info = self._get_site_info(domain)

                if site_info:
                    is_verified = site_info.get("IsVerified", False)
                    if is_verified:
                        logger.info(f"Site verified: {site_url}")
                        return True

                logger.warning(
                    f"Verification attempt {attempt}/{max_retries}: not yet verified"
                )

            except BingWebmasterError as e:
                logger.warning(f"Verification attempt {attempt} failed: {e}")

            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay}s...")
                time.sleep(retry_delay)

        raise BingWebmasterError(
            f"Bing site verification failed after {max_retries} attempts: {domain}"
        )

    def is_verified(self, domain: str) -> bool:
        """
        Check if a site is verified.

        Args:
            domain: The domain to check.

        Returns:
            True if the site is verified in Bing.
        """
        try:
            site_info = self._get_site_info(domain)
            if site_info:
                return site_info.get("IsVerified", False)
            return False
        except BingWebmasterError:
            return False

    def submit_sitemap(self, domain: str, sitemap_path: str = "/sitemap.xml") -> None:
        """
        Submit a sitemap to Bing.

        Args:
            domain: The domain.
            sitemap_path: Path to the sitemap (default: /sitemap.xml).

        Raises:
            BingWebmasterError: On API errors.
        """
        site_url = f"https://{domain}/"
        sitemap_url = f"https://{domain}{sitemap_path}"

        logger.info(f"Submitting sitemap to Bing: {sitemap_url}")

        try:
            self._request(
                "POST",
                "SubmitFeed",
                json_data={"siteUrl": site_url, "feedUrl": sitemap_url},
            )
            logger.info(f"Sitemap submitted: {sitemap_url}")

        except BingWebmasterError as e:
            # Some errors are non-fatal (e.g., sitemap already submitted)
            if "already" in str(e).lower():
                logger.info(f"Sitemap already submitted: {sitemap_url}")
                return
            raise

    def submit_url(self, url: str) -> None:
        """
        Submit a single URL for indexing.

        Args:
            url: The full URL to submit (e.g., "https://example.com/page").

        Note:
            There are daily quotas on URL submissions. Use sparingly.
        """
        logger.info(f"Submitting URL to Bing: {url}")

        try:
            # Extract siteUrl from the URL
            from urllib.parse import urlparse
            parsed = urlparse(url)
            site_url = f"{parsed.scheme}://{parsed.netloc}/"

            self._request("POST", "SubmitUrl", json_data={"siteUrl": site_url, "url": url})
            logger.info(f"URL submitted: {url}")

        except BingWebmasterError as e:
            # Quota exceeded is common - log but don't fail
            if "quota" in str(e).lower():
                logger.warning(f"Bing URL submission quota exceeded")
                return
            raise

    def get_sitemaps(self, domain: str) -> list[dict]:
        """
        Get all sitemaps submitted for a domain.

        Args:
            domain: The domain to list sitemaps for.

        Returns:
            List of sitemap info dicts.
        """
        site_url = f"https://{domain}/"

        try:
            result = self._request("GET", "GetFeeds", params={"siteUrl": site_url})
            if result and isinstance(result, list):
                return result
            return []

        except BingWebmasterError as e:
            logger.warning(f"Failed to list Bing sitemaps: {e}")
            return []
