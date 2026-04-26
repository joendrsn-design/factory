"""
Tenant provisioning orchestrator.

Coordinates the full domain provisioning flow for Site Empire tenants:
- DNS configuration via Namecheap
- Vercel domain setup (with redirect support)
- Google Search Console verification
- Bing Webmaster Tools verification
- IndexNow key generation
- Sitemap submission

Supports both canonical domains and redirect-only domains.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from .config import Config, load_config
from .namecheap import NamecheapClient
from .vercel import VercelClient
from .search_console import SearchConsoleClient
from .bing import BingWebmasterClient
from .indexnow import generate_key as generate_indexnow_key
from .errors import (
    OnboardingError,
    VercelError,
    NamecheapError,
    SearchConsoleError,
    BingWebmasterError,
)

logger = logging.getLogger("article_factory.onboarding.provisioner")


@dataclass
class TenantManifest:
    """
    Configuration for a tenant domain to provision.

    Attributes:
        domain: The domain to provision (e.g., "example.com").
        domain_role: Either 'canonical' (primary) or 'redirect_to' (301 redirect).
        canonical_domain: For redirect domains, the target canonical domain.
        cluster: Content cluster (health-performance, trading-finance, etc.).
        site_id: Optional UUID for existing site record in database.
    """
    domain: str
    domain_role: str = "canonical"  # 'canonical' | 'redirect_to'
    canonical_domain: str | None = None
    cluster: str | None = None
    site_id: str | None = None

    def __post_init__(self):
        # Validation
        if self.domain_role not in ("canonical", "redirect_to"):
            raise ValueError(f"domain_role must be 'canonical' or 'redirect_to', got '{self.domain_role}'")
        if self.domain_role == "redirect_to" and not self.canonical_domain:
            raise ValueError("redirect_to domains must specify canonical_domain")


@dataclass
class ProvisioningResult:
    """
    Result of a provisioning operation.

    Attributes:
        domain: The domain that was provisioned.
        success: True if all steps completed successfully.
        steps: Dict of step_name -> step result details.
        indexnow_key: Generated IndexNow key (canonical domains only).
        errors: List of any errors encountered.
        duration_seconds: Total time taken.
    """
    domain: str
    success: bool = True
    steps: dict[str, dict[str, Any]] = field(default_factory=dict)
    indexnow_key: str | None = None
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0


class TenantProvisioner:
    """
    Orchestrates domain provisioning for Site Empire tenants.

    Usage:
        config = load_config()
        provisioner = TenantProvisioner(config)

        manifest = TenantManifest(
            domain="newsite.com",
            domain_role="canonical",
            cluster="health-performance",
        )

        result = provisioner.provision(manifest)
        if result.success:
            print(f"IndexNow key: {result.indexnow_key}")
    """

    def __init__(
        self,
        config: Config,
        log_callback: Callable[[str, str, str, str, dict | None, str | None], None] | None = None,
    ):
        """
        Initialize the provisioner with configuration.

        Args:
            config: Configuration containing API credentials.
            log_callback: Optional callback for logging provisioning steps.
                         Signature: (site_id, domain, phase, status, details, error_message)
                         This can be used to insert into provisioning_log table.
        """
        self.config = config
        self.log_callback = log_callback

        # Initialize clients
        self.namecheap = NamecheapClient(config)
        self.vercel = VercelClient(config)
        self.gsc = SearchConsoleClient(config.google_sa_json_path)
        self.bing = BingWebmasterClient(config.bing_api_key) if config.bing_api_key else None

    def _log_step(
        self,
        site_id: str | None,
        domain: str,
        phase: str,
        status: str,
        details: dict | None = None,
        error_message: str | None = None,
    ) -> None:
        """Log a provisioning step."""
        log_msg = f"[{phase}] {domain}: {status}"
        if error_message:
            log_msg += f" - {error_message}"
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

        if self.log_callback:
            self.log_callback(
                site_id or "",
                domain,
                phase,
                status,
                details,
                error_message,
            )

    def provision(
        self,
        manifest: TenantManifest,
        skip_dns: bool = False,
        skip_vercel: bool = False,
        skip_gsc: bool = False,
        skip_bing: bool = False,
        skip_indexnow: bool = False,
        skip_sitemap: bool = False,
        dns_propagation_wait: int = 60,
    ) -> ProvisioningResult:
        """
        Provision a tenant domain.

        For canonical domains, runs the full flow:
        1. DNS configuration (A record, verification TXT records)
        2. Vercel domain setup
        3. GSC verification and property creation
        4. Bing verification
        5. IndexNow key generation
        6. Sitemap submission

        For redirect domains, runs a subset:
        1. DNS configuration (A record pointing to Vercel)
        2. Vercel domain setup with redirect configuration

        Args:
            manifest: Tenant manifest specifying domain and configuration.
            skip_dns: Skip DNS configuration step.
            skip_vercel: Skip Vercel domain setup.
            skip_gsc: Skip Google Search Console verification.
            skip_bing: Skip Bing Webmaster Tools verification.
            skip_indexnow: Skip IndexNow key generation.
            skip_sitemap: Skip sitemap submission.
            dns_propagation_wait: Seconds to wait for DNS propagation.

        Returns:
            ProvisioningResult with status and any generated keys.
        """
        start_time = time.time()
        result = ProvisioningResult(domain=manifest.domain)
        is_canonical = manifest.domain_role == "canonical"

        logger.info(f"Starting provisioning for {manifest.domain} (role={manifest.domain_role})")

        try:
            # Step 1: DNS Configuration
            if not skip_dns:
                self._provision_dns(manifest, result)
                if result.errors:
                    result.success = False
                    return result

            # Step 2: Vercel Domain Setup
            if not skip_vercel:
                self._provision_vercel(manifest, result)
                if result.errors:
                    result.success = False
                    return result

            # Wait for DNS propagation before verification steps
            if not skip_dns and (not skip_gsc or not skip_bing) and is_canonical:
                logger.info(f"Waiting {dns_propagation_wait}s for DNS propagation...")
                time.sleep(dns_propagation_wait)

            # Canonical-only steps
            if is_canonical:
                # Step 3: GSC Verification
                if not skip_gsc:
                    self._provision_gsc(manifest, result)

                # Step 4: Bing Verification
                if not skip_bing and self.bing:
                    self._provision_bing(manifest, result)
                elif not skip_bing and not self.bing:
                    self._log_step(
                        manifest.site_id, manifest.domain, "bing",
                        "skipped", {"reason": "no_api_key"}
                    )
                    result.steps["bing"] = {"status": "skipped", "reason": "no_api_key"}

                # Step 5: IndexNow Key Generation
                if not skip_indexnow:
                    self._provision_indexnow(manifest, result)

                # Step 6: Sitemap Submission
                if not skip_sitemap:
                    self._provision_sitemap(manifest, result)

        except Exception as e:
            logger.exception(f"Unexpected error during provisioning: {e}")
            result.errors.append(f"Unexpected error: {e}")
            result.success = False

        result.duration_seconds = time.time() - start_time
        result.success = len(result.errors) == 0

        status = "completed" if result.success else "failed"
        logger.info(f"Provisioning {status} for {manifest.domain} ({result.duration_seconds:.1f}s)")

        return result

    def _provision_dns(self, manifest: TenantManifest, result: ProvisioningResult) -> None:
        """Configure DNS records for the domain."""
        phase = "dns"
        self._log_step(manifest.site_id, manifest.domain, phase, "started")

        try:
            # Verify domain is in our Namecheap account
            if not self.namecheap.domain_exists(manifest.domain):
                raise NamecheapError(f"Domain not found in Namecheap account: {manifest.domain}")

            # Add A record pointing to Vercel
            self.namecheap.add_record(
                domain=manifest.domain,
                record_type="A",
                host="@",
                value="76.76.21.21",  # Vercel's anycast IP
                ttl=1800,
            )

            # Add www CNAME for redundancy
            self.namecheap.add_record(
                domain=manifest.domain,
                record_type="CNAME",
                host="www",
                value="cname.vercel-dns.com",
                ttl=1800,
            )

            self._log_step(manifest.site_id, manifest.domain, phase, "completed")
            result.steps[phase] = {"status": "completed"}

        except NamecheapError as e:
            error_msg = str(e)
            self._log_step(manifest.site_id, manifest.domain, phase, "failed", error_message=error_msg)
            result.steps[phase] = {"status": "failed", "error": error_msg}
            result.errors.append(f"DNS: {error_msg}")

    def _provision_vercel(self, manifest: TenantManifest, result: ProvisioningResult) -> None:
        """Add domain to Vercel project."""
        phase = "vercel"
        self._log_step(manifest.site_id, manifest.domain, phase, "started")

        try:
            if manifest.domain_role == "redirect_to":
                # Redirect domain: configure 301 redirect to canonical
                domain_info = self.vercel.add_domain(
                    domain=manifest.domain,
                    redirect_to=manifest.canonical_domain,
                    redirect_status_code=301,
                )
            else:
                # Canonical domain: standard setup
                domain_info = self.vercel.add_domain(domain=manifest.domain)

            self._log_step(
                manifest.site_id, manifest.domain, phase, "completed",
                {"verified": domain_info.get("verified", False)}
            )
            result.steps[phase] = {
                "status": "completed",
                "verified": domain_info.get("verified", False),
            }

        except VercelError as e:
            error_msg = str(e)
            self._log_step(manifest.site_id, manifest.domain, phase, "failed", error_message=error_msg)
            result.steps[phase] = {"status": "failed", "error": error_msg}
            result.errors.append(f"Vercel: {error_msg}")

    def _provision_gsc(self, manifest: TenantManifest, result: ProvisioningResult) -> None:
        """Verify domain with Google Search Console."""
        phase = "gsc"
        self._log_step(manifest.site_id, manifest.domain, phase, "started")

        try:
            # Check if already verified
            if self.gsc.is_verified(manifest.domain):
                self._log_step(manifest.site_id, manifest.domain, phase, "completed", {"already_verified": True})
                result.steps[phase] = {"status": "completed", "already_verified": True}
                return

            # Get verification token
            token = self.gsc.get_verification_token(manifest.domain)

            # Add TXT record for verification
            self.namecheap.add_record(
                domain=manifest.domain,
                record_type="TXT",
                host="@",
                value=token,
                ttl=1800,
            )

            # Attempt verification (with retries built into the method)
            self.gsc.verify_domain(manifest.domain, max_retries=3, retry_delay=30)

            # Add Search Console property
            if not self.gsc.property_exists(manifest.domain):
                self.gsc.add_property(manifest.domain)

            self._log_step(manifest.site_id, manifest.domain, phase, "completed")
            result.steps[phase] = {"status": "completed", "property_id": f"sc-domain:{manifest.domain}"}

        except (SearchConsoleError, NamecheapError) as e:
            error_msg = str(e)
            self._log_step(manifest.site_id, manifest.domain, phase, "failed", error_message=error_msg)
            result.steps[phase] = {"status": "failed", "error": error_msg}
            # GSC failure is non-fatal for overall provisioning
            logger.warning(f"GSC verification failed (non-fatal): {error_msg}")

    def _provision_bing(self, manifest: TenantManifest, result: ProvisioningResult) -> None:
        """Verify domain with Bing Webmaster Tools."""
        phase = "bing"
        self._log_step(manifest.site_id, manifest.domain, phase, "started")

        try:
            # Check if already verified
            if self.bing.is_verified(manifest.domain):
                self._log_step(manifest.site_id, manifest.domain, phase, "completed", {"already_verified": True})
                result.steps[phase] = {"status": "completed", "already_verified": True}
                return

            # Add site if not exists
            if not self.bing.site_exists(manifest.domain):
                self.bing.add_site(manifest.domain)

            # Get verification token
            try:
                token = self.bing.get_verification_token(manifest.domain)

                # Add TXT record for verification (BingSiteAuth format)
                self.namecheap.add_record(
                    domain=manifest.domain,
                    record_type="TXT",
                    host="@",
                    value=f"BingSiteAuth {token}",
                    ttl=1800,
                )

                # Attempt verification
                self.bing.verify_site(manifest.domain, max_retries=3, retry_delay=30)

            except BingWebmasterError as e:
                # Verification token retrieval or verification may fail
                # Log but continue - Bing is less critical than GSC
                logger.warning(f"Bing verification issue: {e}")
                self._log_step(
                    manifest.site_id, manifest.domain, phase, "partial",
                    {"site_added": True}, error_message=str(e)
                )
                result.steps[phase] = {"status": "partial", "site_added": True, "error": str(e)}
                return

            self._log_step(manifest.site_id, manifest.domain, phase, "completed")
            result.steps[phase] = {"status": "completed", "site_url": f"https://{manifest.domain}/"}

        except (BingWebmasterError, NamecheapError) as e:
            error_msg = str(e)
            self._log_step(manifest.site_id, manifest.domain, phase, "failed", error_message=error_msg)
            result.steps[phase] = {"status": "failed", "error": error_msg}
            # Bing failure is non-fatal
            logger.warning(f"Bing verification failed (non-fatal): {error_msg}")

    def _provision_indexnow(self, manifest: TenantManifest, result: ProvisioningResult) -> None:
        """Generate IndexNow API key."""
        phase = "indexnow"
        self._log_step(manifest.site_id, manifest.domain, phase, "started")

        try:
            # Generate a unique key for this domain
            key = generate_indexnow_key(length=32)
            result.indexnow_key = key

            self._log_step(
                manifest.site_id, manifest.domain, phase, "completed",
                {"key_length": len(key)}
            )
            result.steps[phase] = {
                "status": "completed",
                "key_path": "/indexnow-key.txt",
            }

        except Exception as e:
            error_msg = str(e)
            self._log_step(manifest.site_id, manifest.domain, phase, "failed", error_message=error_msg)
            result.steps[phase] = {"status": "failed", "error": error_msg}
            # IndexNow failure is non-fatal
            logger.warning(f"IndexNow key generation failed (non-fatal): {error_msg}")

    def _provision_sitemap(self, manifest: TenantManifest, result: ProvisioningResult) -> None:
        """Submit sitemap to search engines."""
        phase = "sitemap"
        self._log_step(manifest.site_id, manifest.domain, phase, "started")

        gsc_submitted = False
        bing_submitted = False
        errors = []

        # Submit to GSC
        try:
            if result.steps.get("gsc", {}).get("status") == "completed":
                self.gsc.submit_sitemap(manifest.domain)
                gsc_submitted = True
            else:
                logger.info("Skipping GSC sitemap submission (GSC not verified)")
        except SearchConsoleError as e:
            errors.append(f"GSC: {e}")
            logger.warning(f"GSC sitemap submission failed: {e}")

        # Submit to Bing
        try:
            if self.bing and result.steps.get("bing", {}).get("status") in ("completed", "partial"):
                self.bing.submit_sitemap(manifest.domain)
                bing_submitted = True
            else:
                logger.info("Skipping Bing sitemap submission (Bing not configured)")
        except BingWebmasterError as e:
            errors.append(f"Bing: {e}")
            logger.warning(f"Bing sitemap submission failed: {e}")

        status = "completed" if (gsc_submitted or bing_submitted) else "partial"
        if not gsc_submitted and not bing_submitted:
            status = "skipped"

        self._log_step(
            manifest.site_id, manifest.domain, phase, status,
            {"gsc": gsc_submitted, "bing": bing_submitted}
        )
        result.steps[phase] = {
            "status": status,
            "gsc_submitted": gsc_submitted,
            "bing_submitted": bing_submitted,
            "errors": errors if errors else None,
        }


def provision_tenant(
    domain: str,
    domain_role: str = "canonical",
    canonical_domain: str | None = None,
    cluster: str | None = None,
    site_id: str | None = None,
    config: Config | None = None,
    **kwargs,
) -> ProvisioningResult:
    """
    Convenience function to provision a single tenant domain.

    Args:
        domain: The domain to provision.
        domain_role: Either 'canonical' or 'redirect_to'.
        canonical_domain: For redirect domains, the target.
        cluster: Content cluster assignment.
        site_id: Optional database site ID.
        config: Optional config (loads from env if not provided).
        **kwargs: Additional arguments passed to TenantProvisioner.provision().

    Returns:
        ProvisioningResult with status and generated keys.

    Example:
        result = provision_tenant(
            domain="mynewsite.com",
            domain_role="canonical",
            cluster="health-performance",
        )
        print(f"Success: {result.success}")
        print(f"IndexNow key: {result.indexnow_key}")
    """
    if config is None:
        config = load_config()

    manifest = TenantManifest(
        domain=domain,
        domain_role=domain_role,
        canonical_domain=canonical_domain,
        cluster=cluster,
        site_id=site_id,
    )

    provisioner = TenantProvisioner(config)
    return provisioner.provision(manifest, **kwargs)
