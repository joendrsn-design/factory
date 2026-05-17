"""
One-time audit script to detect actual provisioning state of live sites.

Checks:
- DNS: Does the domain resolve to Vercel IPs? -> dns_status = 'configured'
- Vercel: Is the domain attached to Vercel? -> vercel_status = 'configured'
- GSC: Is there a verified property? -> gsc_status = 'verified'
- Bing/IndexNow: Leave as not_started, let cron pick up

Run with: python scripts/audit_live_sites.py [--dry-run]
"""
import argparse
import logging
import os
import socket
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Vercel IP addresses (as of 2024)
VERCEL_IPS = {
    "76.76.21.21",
    "76.76.21.22",
    "76.76.21.93",
    "76.76.21.123",
}

# Domains attached to Vercel (from `vercel domains ls`)
VERCEL_DOMAINS = {
    "dailyacquinas.store", "dailybible.biz", "helloyou.site", "menshormones.org",
    "peptidehub.fit", "nootropics.fit", "nootropiclab.org", "metabolichealth.fit",
    "longevitystack.fit", "gutprotocol.org", "gihealth.fit", "dailytao.net",
    "dailyproverbs.org", "dailylatin.org", "dailybible.fit", "coldtherapylab.fit",
    "dailyseneca.org", "humanlongevity.fit", "labpanel.org", "dailyaristotle.org",
    "dailyaquinas.org", "herhormones.fit", "menshealthprotocol.fit", "sleepdepthlab.com",
    "ripthroughtherange.com", "metabolicshift.com", "lamphilllabs.com",
    "hormoneclearinghouse.com", "dailymasterpiece.com", "dailymarcus.com",
    "carnivore.info", "magpiediagnostics.com", "lamphill.org", "betterquotidian.com",
    "theinsidebar.com",
}

# Sites with GSC verified (from factory config)
GSC_VERIFIED = {
    "dailybible.biz",
    "gutprotocol.org",
    "helloyou.site",
    "lamphill.org",
}


def check_dns_vercel(domain: str) -> bool:
    """Check if domain DNS points to Vercel."""
    try:
        ips = socket.gethostbyname_ex(domain)[2]
        return any(ip in VERCEL_IPS for ip in ips)
    except socket.gaierror:
        return False


def audit_sites(dry_run: bool = False):
    """Audit all sites and update their provisioning status."""
    from supabase import create_client

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not supabase_key:
        logger.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required")
        return

    client = create_client(supabase_url, supabase_key)

    # Get all canonical sites
    result = client.table("sites").select(
        "domain, dns_status, vercel_status, gsc_status"
    ).eq("domain_role", "canonical").execute()

    sites = result.data
    logger.info(f"Auditing {len(sites)} canonical sites")

    now = datetime.now(timezone.utc).isoformat()
    updates = []

    for site in sites:
        domain = site["domain"]
        changes = {}

        # Check DNS
        if site.get("dns_status") == "not_started":
            if check_dns_vercel(domain):
                changes["dns_status"] = "configured"
                changes["dns_configured_at"] = now
                logger.info(f"  {domain}: DNS -> configured (resolves to Vercel)")

        # Check Vercel
        if site.get("vercel_status") == "not_started":
            if domain in VERCEL_DOMAINS:
                changes["vercel_status"] = "configured"
                changes["vercel_configured_at"] = now
                logger.info(f"  {domain}: Vercel -> configured (in Vercel domains)")

        # Check GSC
        if site.get("gsc_status") == "not_started":
            if domain in GSC_VERIFIED:
                changes["gsc_status"] = "verified"
                changes["gsc_verified_at"] = now
                logger.info(f"  {domain}: GSC -> verified (in factory config)")

        if changes:
            updates.append((domain, changes))

    # Apply updates
    if updates:
        logger.info(f"\n{'[DRY RUN] ' if dry_run else ''}Applying {len(updates)} updates")
        for domain, changes in updates:
            if not dry_run:
                client.table("sites").update(changes).eq("domain", domain).execute()
            logger.info(f"  {domain}: {changes}")
    else:
        logger.info("No updates needed")

    # Summary
    dns_configured = sum(1 for d, c in updates if "dns_status" in c)
    vercel_configured = sum(1 for d, c in updates if "vercel_status" in c)
    gsc_verified = sum(1 for d, c in updates if "gsc_status" in c)

    logger.info(f"\nSummary: DNS={dns_configured}, Vercel={vercel_configured}, GSC={gsc_verified}")


def main():
    parser = argparse.ArgumentParser(description="Audit live site provisioning state")
    parser.add_argument("--dry-run", action="store_true", help="Don't update database")
    args = parser.parse_args()

    audit_sites(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
