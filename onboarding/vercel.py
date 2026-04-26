"""
Vercel API client for domain management.

Handles adding domains to a Vercel project and polling for
domain verification/activation status.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import requests

from .config import Config
from .errors import VercelError

logger = logging.getLogger("article_factory.onboarding.vercel")

API_BASE = "https://api.vercel.com"


class VercelClient:
    """
    Client for the Vercel API.

    Manages domain registration and status polling for a specific project.
    """

    def __init__(self, config: Config):
        """
        Initialize the Vercel client.

        Args:
            config: Configuration containing API token and project details.
        """
        self.token = config.vercel_api_token
        self.project_id = config.vercel_project_id
        self.team_id = config.vercel_team_id

    def _headers(self) -> dict[str, str]:
        """Build authorization headers."""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _build_url(self, path: str) -> str:
        """
        Build full URL with optional team_id query parameter.

        Args:
            path: API path (e.g., "/v10/projects/{id}/domains")

        Returns:
            Full URL with teamId appended if configured.
        """
        url = f"{API_BASE}{path}"
        if self.team_id:
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}teamId={self.team_id}"
        return url

    def _request(
        self,
        method: str,
        path: str,
        json_data: dict[str, Any] | None = None,
        expected_status: list[int] | None = None,
    ) -> dict[str, Any]:
        """
        Make an authenticated request to the Vercel API.

        Args:
            method: HTTP method (GET, POST, DELETE, etc.)
            path: API path.
            json_data: Optional JSON body for POST/PUT requests.
            expected_status: List of acceptable status codes (default: [200, 201])

        Returns:
            Parsed JSON response.

        Raises:
            VercelError: On HTTP errors or unexpected status codes.
        """
        if expected_status is None:
            expected_status = [200, 201]

        url = self._build_url(path)
        logger.debug(f"Vercel API: {method} {path}")

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self._headers(),
                json=json_data,
                timeout=30,
            )
        except requests.RequestException as e:
            raise VercelError(f"Vercel API request failed: {e}") from e

        # Handle expected status codes
        if response.status_code in expected_status:
            if response.text:
                return response.json()
            return {}

        # Error handling with full response body
        try:
            error_body = response.json()
            error_message = error_body.get("error", {}).get("message", response.text)
        except ValueError:
            error_message = response.text

        raise VercelError(
            f"Vercel API error ({response.status_code}): {error_message}\n"
            f"  Endpoint: {method} {path}"
        )

    def domain_exists(self, domain: str) -> bool:
        """
        Check if a domain is already registered with this Vercel project.

        Args:
            domain: The domain to check (e.g., "example.com").

        Returns:
            True if the domain exists on this project.
        """
        path = f"/v9/projects/{self.project_id}/domains/{domain}"

        try:
            self._request("GET", path, expected_status=[200])
            return True
        except VercelError as e:
            if "404" in str(e):
                return False
            raise

    def add_domain(
        self,
        domain: str,
        redirect_to: str | None = None,
        redirect_status_code: int = 301,
    ) -> dict[str, Any]:
        """
        Add a domain to the Vercel project.

        Args:
            domain: The domain to add (e.g., "example.com").
            redirect_to: Optional target domain for redirect (e.g., "canonical.com").
                        If set, this domain will 301 redirect to the target.
            redirect_status_code: HTTP status code for redirect (default: 301).

        Returns:
            Dict containing domain info and required DNS records.
            Keys include: name, verified, verification (list of DNS records needed).

        Raises:
            VercelError: On API errors.
        """
        logger.info(f"Adding domain to Vercel: {domain}")

        path = f"/v10/projects/{self.project_id}/domains"
        data = {"name": domain}

        # Configure redirect if specified
        if redirect_to:
            data["redirect"] = redirect_to
            data["redirectStatusCode"] = redirect_status_code
            logger.info(f"  Configuring {redirect_status_code} redirect to: {redirect_to}")

        # 409 means domain already exists, which is fine for idempotency
        try:
            result = self._request("POST", path, json_data=data, expected_status=[200, 201])
            logger.info(f"Domain added: {domain}")
            return result
        except VercelError as e:
            if "409" in str(e) or "already" in str(e).lower():
                logger.info(f"Domain already exists on Vercel: {domain}")
                # If redirect was requested, update it
                if redirect_to:
                    return self.configure_redirect(domain, redirect_to, redirect_status_code)
                # Fetch current status instead
                return self.get_domain_status(domain)
            raise

    def configure_redirect(
        self,
        domain: str,
        redirect_to: str,
        redirect_status_code: int = 301,
    ) -> dict[str, Any]:
        """
        Configure a redirect for an existing domain.

        Args:
            domain: The source domain to redirect from.
            redirect_to: The target domain to redirect to.
            redirect_status_code: HTTP status code (301 or 308).

        Returns:
            Updated domain configuration.

        Raises:
            VercelError: On API errors.
        """
        logger.info(f"Configuring redirect: {domain} -> {redirect_to} ({redirect_status_code})")

        path = f"/v9/projects/{self.project_id}/domains/{domain}"
        data = {
            "redirect": redirect_to,
            "redirectStatusCode": redirect_status_code,
        }

        result = self._request("PATCH", path, json_data=data)
        logger.info(f"Redirect configured: {domain} -> {redirect_to}")
        return result

    def remove_redirect(self, domain: str) -> dict[str, Any]:
        """
        Remove redirect configuration from a domain.

        Args:
            domain: The domain to remove redirect from.

        Returns:
            Updated domain configuration.
        """
        logger.info(f"Removing redirect from: {domain}")

        path = f"/v9/projects/{self.project_id}/domains/{domain}"
        data = {
            "redirect": None,
            "redirectStatusCode": None,
        }

        result = self._request("PATCH", path, json_data=data)
        logger.info(f"Redirect removed from: {domain}")
        return result

    def get_domain_status(self, domain: str) -> dict[str, Any]:
        """
        Get the current status of a domain.

        Args:
            domain: The domain to check.

        Returns:
            Dict with domain status including:
            - verified: bool
            - verification: list of pending verification records
            - configured: bool (DNS is correctly configured)
        """
        path = f"/v9/projects/{self.project_id}/domains/{domain}"
        return self._request("GET", path)

    def is_domain_active(self, domain: str) -> bool:
        """
        Check if a domain is fully active (verified and configured).

        Args:
            domain: The domain to check.

        Returns:
            True if domain is verified and serving traffic.
        """
        try:
            status = self.get_domain_status(domain)
            verified = status.get("verified", False)
            # 'configured' means DNS is set up correctly
            configured = status.get("configured", False)
            return verified and configured
        except VercelError:
            return False

    def wait_for_active(self, domain: str, timeout_seconds: int = 600) -> None:
        """
        Poll until domain is verified and active.

        Checks domain status every 10 seconds until it's fully active
        (verified and configured) or timeout is reached.

        Args:
            domain: The domain to wait for.
            timeout_seconds: Maximum wait time (default: 10 minutes).

        Raises:
            VercelError: If timeout is reached before domain is active.
        """
        logger.info(f"Waiting for Vercel domain activation: {domain}")

        poll_interval = 10
        start_time = time.time()

        while True:
            elapsed = time.time() - start_time

            try:
                status = self.get_domain_status(domain)
                verified = status.get("verified", False)
                configured = status.get("configured", False)

                logger.debug(
                    f"Domain status: verified={verified}, configured={configured}"
                )

                if verified and configured:
                    logger.info(
                        f"Domain is active: {domain} ({int(elapsed)}s)"
                    )
                    return

                # Log any pending verification requirements
                verification = status.get("verification", [])
                if verification and not verified:
                    logger.debug(f"Pending verification: {verification}")

            except VercelError as e:
                logger.warning(f"Status check failed (will retry): {e}")

            # Check timeout
            if elapsed >= timeout_seconds:
                raise VercelError(
                    f"Timeout waiting for domain activation: {domain}\n"
                    f"  Waited: {timeout_seconds}s\n"
                    f"  Last status: verified={verified}, configured={configured}"
                )

            # Wait before next poll
            remaining = timeout_seconds - elapsed
            wait_time = min(poll_interval, remaining)
            logger.debug(f"Waiting {wait_time}s before next check...")
            time.sleep(wait_time)

    def get_required_dns_records(self, domain: str) -> list[dict[str, str]]:
        """
        Get the DNS records required to configure this domain.

        Args:
            domain: The domain to check.

        Returns:
            List of dicts with keys: type, name, value
            Typically includes A record for apex and CNAME for www.
        """
        status = self.get_domain_status(domain)

        records = []

        # Check for verification records (usually TXT for ownership)
        for v in status.get("verification", []):
            records.append({
                "type": v.get("type", "TXT"),
                "name": v.get("domain", domain),
                "value": v.get("value", ""),
            })

        # Standard Vercel DNS configuration
        # Apex domain: A record to Vercel's IP
        # www: CNAME to cname.vercel-dns.com
        if not status.get("configured", False):
            # These are Vercel's standard DNS requirements
            records.append({
                "type": "A",
                "name": "@",
                "value": "76.76.21.21",  # Vercel's anycast IP
            })
            records.append({
                "type": "CNAME",
                "name": "www",
                "value": "cname.vercel-dns.com",
            })

        return records
