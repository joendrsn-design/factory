"""
Site onboarding CLI - main orchestration logic.

This module contains the core onboarding flow. The entry point
(onboard_site.py) imports and calls main() from here.
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from .config import Config, load_config
from .errors import (
    OnboardingError,
    ConfigError,
    PreflightError,
    DNSPropagationError,
    VercelError,
)
from .namecheap import NamecheapClient
from .vercel import VercelClient
from .search_console import SearchConsoleClient
from .dns_utils import wait_for_propagation, check_current_records

logger = logging.getLogger("article_factory.onboarding")


@dataclass
class RunMetadata:
    """Metadata collected during the onboarding run."""
    domain: str
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    vercel_added: bool = False
    dns_configured: bool = False
    gsc_verified: bool = False
    gsc_property_added: bool = False
    verification_token: str = ""
    errors: list[str] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Onboard a new domain: Vercel + DNS + Search Console",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python onboard_site.py --domain=example.com --dry-run
    python onboard_site.py --domain=example.com
    python onboard_site.py --domain=example.com --skip-vercel --skip-dns
    python onboard_site.py --domain=example.com --verbose
        """,
    )

    parser.add_argument(
        "--domain",
        required=True,
        help="Domain to onboard (e.g., example.com)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without making changes",
    )
    parser.add_argument(
        "--skip-vercel",
        action="store_true",
        help="Skip Vercel domain registration",
    )
    parser.add_argument(
        "--skip-dns",
        action="store_true",
        help="Skip DNS configuration",
    )
    parser.add_argument(
        "--skip-search-console",
        action="store_true",
        help="Skip Search Console verification",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    return parser.parse_args()


def setup_logging(verbose: bool) -> None:
    """Configure logging for the onboarding run."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def log_header(domain: str, dry_run: bool) -> None:
    """Print the onboarding header."""
    mode = " (DRY RUN)" if dry_run else ""
    print(f"\n{'='*60}")
    print(f"  Site Onboarding{mode}")
    print(f"  Domain: {domain}")
    print(f"{'='*60}\n")


# ─────────────────────────────────────────────────────────────
# Preflight Checks
# ─────────────────────────────────────────────────────────────

def run_preflight(
    domain: str,
    vercel: VercelClient,
    namecheap: NamecheapClient,
    gsc: SearchConsoleClient | None,
    skip_vercel: bool,
    skip_dns: bool,
    skip_search_console: bool,
) -> None:
    """
    Run preflight checks before starting onboarding.

    Verifies:
    - Domain exists in Namecheap account
    - Domain is NOT already fully configured in Vercel (warning only)
    - Domain is NOT already a verified Search Console property (warning only)
    """
    logger.info("Running preflight checks...")

    # Check domain ownership at Namecheap
    if not skip_dns:
        logger.info(f"  Checking Namecheap ownership: {domain}")
        if not namecheap.domain_exists(domain):
            raise PreflightError(
                f"Domain not found in Namecheap account: {domain}\n"
                f"  Ensure the domain is registered under this account."
            )
        logger.info(f"  OK: Domain exists in Namecheap")

    # Check if already in Vercel (warning, not error - for idempotency)
    if not skip_vercel:
        logger.info(f"  Checking Vercel status: {domain}")
        if vercel.domain_exists(domain):
            if vercel.is_domain_active(domain):
                logger.warning(f"  Domain already active in Vercel (will skip add)")
            else:
                logger.info(f"  Domain exists in Vercel but not yet active")
        else:
            logger.info(f"  OK: Domain not yet in Vercel")

    # Check Search Console (warning, not error - for idempotency)
    if not skip_search_console:
        logger.info(f"  Checking Search Console: {domain}")
        if gsc.property_exists(domain):
            logger.warning(f"  Property already exists in Search Console")
        else:
            logger.info(f"  OK: Property not yet in Search Console")

    logger.info("Preflight checks passed.\n")


# ─────────────────────────────────────────────────────────────
# Phase 1: Vercel
# ─────────────────────────────────────────────────────────────

def phase_vercel(
    domain: str,
    vercel: VercelClient,
    dry_run: bool,
) -> list[dict[str, str]]:
    """
    Add domain to Vercel and get required DNS records.

    Returns:
        List of DNS records needed (type, name, value).
    """
    logger.info("Phase 1: Vercel Domain Registration")

    if dry_run:
        print(f"  [DRY RUN] Would add domain to Vercel: {domain}")
        print(f"  [DRY RUN] Would retrieve required DNS records")
        # Return standard Vercel DNS requirements for dry run
        return [
            {"type": "A", "name": "@", "value": "76.76.21.21"},
            {"type": "CNAME", "name": "www", "value": "cname.vercel-dns.com"},
        ]

    # Check if already active
    if vercel.is_domain_active(domain):
        logger.info(f"  Domain already active, skipping add")
        return vercel.get_required_dns_records(domain)

    # Add domain
    vercel.add_domain(domain)

    # Get required DNS records
    records = vercel.get_required_dns_records(domain)
    logger.info(f"  Required DNS records: {len(records)}")
    for rec in records:
        logger.info(f"    {rec['type']} {rec['name']} -> {rec['value']}")

    return records


# ─────────────────────────────────────────────────────────────
# Phase 2: DNS Configuration
# ─────────────────────────────────────────────────────────────

def phase_dns(
    domain: str,
    namecheap: NamecheapClient,
    dns_records: list[dict[str, str]],
    dry_run: bool,
) -> None:
    """Configure DNS records at Namecheap."""
    logger.info("Phase 2: DNS Configuration")

    if dry_run:
        print(f"  [DRY RUN] Would configure DNS records for {domain}:")
        for rec in dns_records:
            print(f"    {rec['type']} {rec['name']} -> {rec['value']}")
        return

    for rec in dns_records:
        namecheap.add_record(
            domain=domain,
            record_type=rec["type"],
            host=rec["name"],
            value=rec["value"],
        )

    logger.info("  DNS records configured")


def phase_dns_propagation(
    domain: str,
    dns_records: list[dict[str, str]],
    dry_run: bool,
    timeout_seconds: int = 300,
) -> None:
    """Wait for DNS records to propagate."""
    logger.info("Phase 2b: DNS Propagation")

    if dry_run:
        print(f"  [DRY RUN] Would wait for DNS propagation (up to {timeout_seconds}s)")
        return

    # Wait for A record on apex domain
    a_records = [r for r in dns_records if r["type"].upper() == "A"]
    if a_records:
        rec = a_records[0]
        wait_for_propagation(
            domain=domain,
            record_type="A",
            expected_value=rec["value"],
            timeout_seconds=timeout_seconds,
        )


# ─────────────────────────────────────────────────────────────
# Phase 3: Wait for Vercel Active
# ─────────────────────────────────────────────────────────────

def phase_vercel_active(
    domain: str,
    vercel: VercelClient,
    dry_run: bool,
    timeout_seconds: int = 600,
) -> None:
    """Wait for Vercel to confirm domain is active."""
    logger.info("Phase 3: Vercel Domain Activation")

    if dry_run:
        print(f"  [DRY RUN] Would wait for Vercel activation (up to {timeout_seconds}s)")
        return

    vercel.wait_for_active(domain, timeout_seconds=timeout_seconds)


# ─────────────────────────────────────────────────────────────
# Phase 4: Search Console
# ─────────────────────────────────────────────────────────────

def phase_search_console(
    domain: str,
    namecheap: NamecheapClient,
    gsc: SearchConsoleClient,
    dry_run: bool,
    metadata: RunMetadata,
) -> None:
    """
    Verify domain and add Search Console property.

    Steps:
    1. Get verification token
    2. Add TXT record to DNS
    3. Wait for TXT propagation
    4. Verify domain
    5. Add sc-domain property
    """
    logger.info("Phase 4: Search Console Verification")

    # Step 1: Get verification token
    logger.info("  Step 4.1: Getting verification token...")
    if dry_run:
        print(f"  [DRY RUN] Would request verification token for {domain}")
        token = "google-site-verification=DRY_RUN_TOKEN"
    else:
        # Check if already verified
        if gsc.is_verified(domain):
            logger.info(f"  Domain already verified, skipping to property add")
            metadata.gsc_verified = True
        else:
            token = gsc.get_verification_token(domain)
            metadata.verification_token = token

    # Step 2: Add TXT record
    if not metadata.gsc_verified:
        logger.info("  Step 4.2: Adding TXT record...")
        if dry_run:
            print(f"  [DRY RUN] Would add TXT record: @ -> {token}")
        else:
            namecheap.add_record(
                domain=domain,
                record_type="TXT",
                host="@",
                value=metadata.verification_token,
            )

        # Step 3: Wait for TXT propagation
        logger.info("  Step 4.3: Waiting for TXT propagation...")
        if dry_run:
            print(f"  [DRY RUN] Would wait for TXT propagation")
        else:
            wait_for_propagation(
                domain=domain,
                record_type="TXT",
                expected_value=metadata.verification_token,
                timeout_seconds=300,
            )

        # Step 4: Verify domain
        logger.info("  Step 4.4: Verifying domain ownership...")
        if dry_run:
            print(f"  [DRY RUN] Would verify domain via Site Verification API")
        else:
            gsc.verify_domain(domain)
            metadata.gsc_verified = True

    # Step 5: Add property
    logger.info("  Step 4.5: Adding Search Console property...")
    if dry_run:
        print(f"  [DRY RUN] Would add sc-domain:{domain} property")
    else:
        # Check if property already exists
        if gsc.property_exists(domain):
            logger.info(f"  Property already exists")
        else:
            gsc.add_property(domain)
        metadata.gsc_property_added = True


# ─────────────────────────────────────────────────────────────
# Phase 5: Write YAML
# ─────────────────────────────────────────────────────────────

def write_site_yaml(
    domain: str,
    metadata: RunMetadata,
    dry_run: bool,
    config_dir: str = "config/sites",
) -> Path | None:
    """
    Write site configuration YAML file.

    Creates a new YAML file in config/sites/<domain>.yaml with
    metadata from the onboarding run.
    """
    logger.info("Phase 5: Writing Site YAML")

    # Sanitize domain for filename
    filename = domain.replace(".", "-") + ".yaml"
    yaml_path = Path(config_dir) / filename

    site_config = {
        "site_id": domain.replace(".", "-"),
        "domain": domain,
        "site_name": domain.split(".")[0].title(),
        "onboarding": {
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "vercel_configured": metadata.vercel_added or metadata.dns_configured,
            "gsc_verified": metadata.gsc_verified,
            "gsc_property_added": metadata.gsc_property_added,
            "verification_token": metadata.verification_token,
        },
    }

    if dry_run:
        print(f"  [DRY RUN] Would write site config to: {yaml_path}")
        print(f"  [DRY RUN] Config preview:")
        print(yaml.dump(site_config, default_flow_style=False, indent=2))
        return None

    yaml_path.parent.mkdir(parents=True, exist_ok=True)

    # Don't overwrite existing config
    if yaml_path.exists():
        logger.warning(f"  Site config already exists: {yaml_path}")
        logger.warning(f"  Skipping write to avoid overwriting")
        return yaml_path

    with open(yaml_path, "w") as f:
        yaml.dump(site_config, f, default_flow_style=False, indent=2)

    logger.info(f"  Wrote site config: {yaml_path}")
    return yaml_path


# ─────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────

def print_summary(domain: str, metadata: RunMetadata, dry_run: bool) -> None:
    """Print the onboarding summary and next steps."""
    print(f"\n{'='*60}")

    if dry_run:
        print("  DRY RUN COMPLETE")
        print(f"  No changes were made for: {domain}")
    else:
        print("  ONBOARDING COMPLETE")
        print(f"  Domain: {domain}")
        print(f"\n  Status:")
        print(f"    Vercel:         {'OK' if metadata.vercel_added or metadata.dns_configured else 'Skipped'}")
        print(f"    DNS:            {'OK' if metadata.dns_configured else 'Skipped'}")
        print(f"    GSC Verified:   {'OK' if metadata.gsc_verified else 'Skipped'}")
        print(f"    GSC Property:   {'OK' if metadata.gsc_property_added else 'Skipped'}")

    print(f"\n  URLs:")
    print(f"    Site:           https://{domain}")
    print(f"    Vercel:         https://vercel.com/projects")
    print(f"    Search Console: https://search.google.com/search-console/welcome")

    if not dry_run and metadata.gsc_property_added:
        print(f"\n  MANUAL STEP REQUIRED:")
        print(f"    The service account is now Owner of sc-domain:{domain}")
        print(f"    To grant yourself Owner access:")
        print(f"    1. Go to Search Console -> Settings -> Users and permissions")
        print(f"    2. Add your Google account as Owner")

    print(f"{'='*60}\n")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> int:
    """Main entry point for the onboarding CLI."""
    args = parse_args()
    setup_logging(args.verbose)
    log_header(args.domain, args.dry_run)

    # Load configuration
    try:
        config = load_config()
    except ConfigError as e:
        logger.error(f"Configuration error:\n{e}")
        return 1

    # Initialize metadata
    metadata = RunMetadata(domain=args.domain)

    # Initialize clients (GSC client only if needed)
    vercel = VercelClient(config)
    namecheap = NamecheapClient(config)
    gsc = None if args.skip_search_console else SearchConsoleClient(config.google_sa_json_path)

    try:
        # Preflight checks
        run_preflight(
            args.domain,
            vercel,
            namecheap,
            gsc,
            skip_vercel=args.skip_vercel,
            skip_dns=args.skip_dns,
            skip_search_console=args.skip_search_console,
        )

        dns_records: list[dict[str, str]] = []

        # Phase 1: Vercel
        if not args.skip_vercel:
            dns_records = phase_vercel(args.domain, vercel, args.dry_run)
            metadata.vercel_added = True
        else:
            logger.info("Skipping Vercel (--skip-vercel)")
            # Use default Vercel DNS records
            dns_records = [
                {"type": "A", "name": "@", "value": "76.76.21.21"},
                {"type": "CNAME", "name": "www", "value": "cname.vercel-dns.com"},
            ]

        # Phase 2: DNS
        if not args.skip_dns:
            phase_dns(args.domain, namecheap, dns_records, args.dry_run)
            phase_dns_propagation(args.domain, dns_records, args.dry_run)
            metadata.dns_configured = True
        else:
            logger.info("Skipping DNS (--skip-dns)")

        # Phase 3: Wait for Vercel active
        if not args.skip_vercel and not args.skip_dns:
            phase_vercel_active(args.domain, vercel, args.dry_run)
        else:
            logger.info("Skipping Vercel activation wait")

        # Phase 4: Search Console
        if not args.skip_search_console:
            phase_search_console(args.domain, namecheap, gsc, args.dry_run, metadata)
        else:
            logger.info("Skipping Search Console (--skip-search-console)")

        # Phase 5: Write YAML
        write_site_yaml(args.domain, metadata, args.dry_run)

        # Summary
        print_summary(args.domain, metadata, args.dry_run)
        return 0

    except PreflightError as e:
        logger.error(f"Preflight check failed:\n{e}")
        return 1
    except DNSPropagationError as e:
        logger.error(f"DNS propagation failed:\n{e}")
        return 1
    except VercelError as e:
        logger.error(f"Vercel error:\n{e}")
        return 1
    except OnboardingError as e:
        logger.error(f"Onboarding error:\n{e}")
        return 1
    except KeyboardInterrupt:
        logger.warning("\nOnboarding interrupted by user")
        return 130
