"""End-to-end onboarding: domain → DNS → Vercel → verified → indexed.

One command takes a domain from "just bought" to "live and indexed".

Usage:
    python onboard_site.py <slug>
    python onboard_site.py <slug> --skip-index
    python onboard_site.py <slug> --skip-dns
    python onboard_site.py --domain example.com  # direct domain, no registry lookup
"""
from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

import vercel_domains
import namecheap_dns
import search_indexing
from sites_registry import get_site, get_site_by_domain

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("article_factory.onboard_site")


def onboard(
    slug: Optional[str] = None,
    domain: Optional[str] = None,
    skip_dns: bool = False,
    skip_index: bool = False,
    skip_verification: bool = False,
    gsc_txt: Optional[str] = None,
) -> bool:
    """
    Run full onboarding flow for a site.

    Args:
        slug: Site slug from registry (mutually exclusive with domain)
        domain: Direct domain override (skips registry lookup)
        skip_dns: Skip Namecheap DNS configuration
        skip_index: Skip search engine submission
        skip_verification: Skip waiting for Vercel verification
        gsc_txt: Optional Google Search Console TXT verification value

    Returns:
        True if successful, False otherwise
    """
    # Resolve domain
    if slug:
        try:
            site = get_site(slug)
            domain = site["domain"]
            site_name = site.get("site_name", slug)
        except KeyError:
            logger.error(f"Site not found in registry: {slug}")
            return False
    elif domain:
        site = get_site_by_domain(domain)
        site_name = site.get("site_name", domain) if site else domain
        slug = site.get("site_id", domain) if site else domain
    else:
        logger.error("Either --slug or --domain is required")
        return False

    logger.info(f"{'='*60}")
    logger.info(f"  Onboarding: {site_name} ({domain})")
    logger.info(f"{'='*60}")

    steps_total = 4
    step = 0

    # Step 1: Configure Namecheap DNS
    step += 1
    if skip_dns:
        logger.info(f"[{step}/{steps_total}] Skipping DNS configuration (--skip-dns)")
    else:
        logger.info(f"[{step}/{steps_total}] Configuring Namecheap DNS...")
        try:
            namecheap_dns.configure_for_vercel(domain, gsc_verification_txt=gsc_txt)
        except Exception as e:
            logger.error(f"DNS configuration failed: {e}")
            logger.info("  Hint: Check NAMECHEAP_CLIENT_IP is whitelisted in Namecheap API settings")
            return False

    # Step 2: Register with Vercel
    step += 1
    logger.info(f"[{step}/{steps_total}] Registering with Vercel...")
    try:
        result = vercel_domains.add_domain(domain)
        if result.get("already_existed"):
            logger.info(f"  Domain already registered with Vercel")
        else:
            logger.info(f"  Domain added, verified={result.get('verified', False)}")
    except Exception as e:
        logger.error(f"Vercel registration failed: {e}")
        return False

    # Step 3: Wait for verification
    step += 1
    if skip_verification:
        logger.info(f"[{step}/{steps_total}] Skipping verification wait (--skip-verification)")
    else:
        logger.info(f"[{step}/{steps_total}] Waiting for SSL + verification (up to 5 min)...")
        if not vercel_domains.wait_for_verification(domain, timeout_seconds=300):
            logger.error(f"Verification failed for {domain}")
            logger.info("  Hint: Check DNS propagation at https://dnschecker.org")
            return False

    # Step 4: Submit to search engines
    step += 1
    if skip_index:
        logger.info(f"[{step}/{steps_total}] Skipping search indexing (--skip-index)")
    else:
        logger.info(f"[{step}/{steps_total}] Submitting to GSC + Bing...")
        results = search_indexing.index_everywhere(domain)
        logger.info(f"  GSC: {results['gsc']}")
        logger.info(f"  Bing: {results['bing']}")

    logger.info(f"")
    logger.info(f"✓ {domain} is live!")
    logger.info(f"  → https://{domain}")
    return True


def onboard_all(
    skip_dns: bool = False,
    skip_index: bool = False,
    skip_verification: bool = False,
) -> dict:
    """
    Onboard all sites from the registry.

    Returns:
        Dict mapping site_id to success boolean
    """
    from sites_registry import all_sites

    results = {}
    sites = all_sites()

    logger.info(f"Onboarding {len(sites)} sites...")
    logger.info("")

    for site in sites:
        slug = site["site_id"]
        domain = site["domain"]

        if not domain:
            logger.warning(f"Skipping {slug} — no domain configured")
            results[slug] = False
            continue

        success = onboard(
            slug=slug,
            skip_dns=skip_dns,
            skip_index=skip_index,
            skip_verification=skip_verification,
        )
        results[slug] = success

        if not success:
            logger.warning(f"Failed to onboard {slug}, continuing with others...")
        logger.info("")

    # Summary
    succeeded = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)

    logger.info(f"{'='*60}")
    logger.info(f"  Onboarding Complete")
    logger.info(f"  Succeeded: {succeeded}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"{'='*60}")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Onboard sites: DNS → Vercel → verification → indexing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python onboard_site.py dailymarcus
    python onboard_site.py dailymarcus --skip-index
    python onboard_site.py --domain example.com --skip-dns
    python onboard_site.py --all --skip-index
        """
    )

    parser.add_argument(
        "slug",
        nargs="?",
        help="Site slug from sites_registry"
    )
    parser.add_argument(
        "--domain",
        help="Direct domain (skips registry lookup)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Onboard all sites from registry"
    )
    parser.add_argument(
        "--skip-dns",
        action="store_true",
        help="Skip Namecheap DNS configuration"
    )
    parser.add_argument(
        "--skip-index",
        action="store_true",
        help="Skip search engine submission"
    )
    parser.add_argument(
        "--skip-verification",
        action="store_true",
        help="Skip waiting for Vercel verification"
    )
    parser.add_argument(
        "--gsc-txt",
        help="Google Search Console TXT verification value to add to DNS"
    )

    args = parser.parse_args()

    if args.all:
        results = onboard_all(
            skip_dns=args.skip_dns,
            skip_index=args.skip_index,
            skip_verification=args.skip_verification,
        )
        failed = sum(1 for v in results.values() if not v)
        sys.exit(1 if failed > 0 else 0)
    elif args.slug or args.domain:
        success = onboard(
            slug=args.slug,
            domain=args.domain,
            skip_dns=args.skip_dns,
            skip_index=args.skip_index,
            skip_verification=args.skip_verification,
            gsc_txt=args.gsc_txt,
        )
        sys.exit(0 if success else 1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
