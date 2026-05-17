"""Namecheap DNS API client — sets A record + www URL redirect.

Namecheap's API is XML-based and uses setHosts which REPLACES all records
at once — so we always GET first, merge our changes, then PUT back.
"""
from __future__ import annotations

import os
import logging
from typing import Literal
from xml.etree import ElementTree as ET

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("article_factory.namecheap_dns")

# Vercel's apex IP for A records
VERCEL_APEX_IP = "76.76.21.21"

RecordType = Literal["A", "CNAME", "URL", "URL301", "TXT", "MX"]


def _api_base() -> str:
    """Return API base URL (sandbox or production)."""
    if os.environ.get("NAMECHEAP_SANDBOX", "false").lower() == "true":
        return "https://api.sandbox.namecheap.com/xml.response"
    return "https://api.namecheap.com/xml.response"


def _base_params() -> dict[str, str]:
    """Build base parameters for all Namecheap API calls."""
    required = ["NAMECHEAP_API_USER", "NAMECHEAP_API_KEY", "NAMECHEAP_USERNAME", "NAMECHEAP_CLIENT_IP"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise RuntimeError(f"Missing Namecheap env vars: {missing}")

    return {
        "ApiUser": os.environ["NAMECHEAP_API_USER"],
        "ApiKey": os.environ["NAMECHEAP_API_KEY"],
        "UserName": os.environ["NAMECHEAP_USERNAME"],
        "ClientIp": os.environ["NAMECHEAP_CLIENT_IP"],
    }


def _split_domain(domain: str) -> tuple[str, str]:
    """
    Split domain into SLD and TLD.

    Examples:
        example.com → ('example', 'com')
        example.co.uk → ('example', 'co.uk')
        example.info → ('example', 'info')
    """
    parts = domain.lower().strip().split(".")
    if len(parts) < 2:
        raise ValueError(f"Invalid domain: {domain}")

    # Handle common two-part TLDs
    two_part_tlds = {"co.uk", "org.uk", "com.au", "co.nz", "co.jp"}
    if len(parts) >= 3:
        potential_tld = ".".join(parts[-2:])
        if potential_tld in two_part_tlds:
            return parts[-3], potential_tld

    # Standard split: everything before last dot is SLD, last part is TLD
    return ".".join(parts[:-1]), parts[-1]


def _check_response(root: ET.Element, action: str) -> None:
    """Check Namecheap API response for errors."""
    status = root.attrib.get("Status")
    if status != "OK":
        # Try to extract error message
        ns = "{http://api.namecheap.com/xml.response}"
        errors = root.find(f".//{ns}Errors")
        if errors is not None:
            error_msgs = []
            for err in errors:
                error_msgs.append(err.text or err.attrib.get("Number", "Unknown error"))
            msg = "; ".join(error_msgs)
        else:
            msg = ET.tostring(root, encoding="unicode")[:500]
        raise RuntimeError(f"Namecheap {action} failed: {msg}")


def get_hosts(domain: str) -> list[dict]:
    """
    Get all current DNS records for a domain.

    Args:
        domain: The domain to query (e.g., "example.com")

    Returns:
        List of record dicts with keys: name, type, address, ttl, priority
    """
    sld, tld = _split_domain(domain)
    params = {
        **_base_params(),
        "Command": "namecheap.domains.dns.getHosts",
        "SLD": sld,
        "TLD": tld,
    }

    r = requests.get(_api_base(), params=params, timeout=30)
    r.raise_for_status()

    root = ET.fromstring(r.text)
    _check_response(root, "getHosts")

    ns = "{http://api.namecheap.com/xml.response}"
    hosts = []
    for host in root.iter(f"{ns}host"):
        hosts.append({
            "name": host.attrib.get("Name", "@"),
            "type": host.attrib.get("Type", "A"),
            "address": host.attrib.get("Address", ""),
            "ttl": host.attrib.get("TTL", "1800"),
            "priority": host.attrib.get("MXPref", "10"),
        })

    logger.debug(f"Got {len(hosts)} DNS records for {domain}")
    return hosts


def set_hosts(domain: str, records: list[dict]) -> None:
    """
    Replace all DNS records for a domain.

    WARNING: This replaces ALL records. Always call get_hosts() first
    and merge your changes with existing records you want to keep.

    Args:
        domain: The domain to update
        records: List of record dicts with keys: name, type, address, ttl, priority
    """
    sld, tld = _split_domain(domain)
    params = {
        **_base_params(),
        "Command": "namecheap.domains.dns.setHosts",
        "SLD": sld,
        "TLD": tld,
    }

    for i, rec in enumerate(records, start=1):
        params[f"HostName{i}"] = rec["name"]
        params[f"RecordType{i}"] = rec["type"]
        params[f"Address{i}"] = rec["address"]
        params[f"TTL{i}"] = str(rec.get("ttl", "1800"))
        if rec["type"] == "MX":
            params[f"MXPref{i}"] = str(rec.get("priority", "10"))

    r = requests.post(_api_base(), params=params, timeout=30)
    r.raise_for_status()

    root = ET.fromstring(r.text)
    _check_response(root, "setHosts")
    logger.info(f"Set {len(records)} DNS records for {domain}")


def configure_for_vercel(domain: str, gsc_verification_txt: str | None = None) -> None:
    """
    Idempotent: ensures the domain has the right records for Vercel hosting.

    Sets:
        - A @ → 76.76.21.21 (Vercel apex IP)
        - URL301 www → https://{domain} (redirect www to apex)
        - Optional TXT @ for Google Search Console verification

    Preserves:
        - All MX records (email)
        - Other records not on @ or www

    Args:
        domain: The domain to configure
        gsc_verification_txt: Optional Google Search Console TXT verification value
    """
    existing = get_hosts(domain)
    logger.info(f"Found {len(existing)} existing records for {domain}")

    # Keep MX records and records not on @ or www
    keep = []
    for r in existing:
        if r["type"] == "MX":
            keep.append(r)
        elif r["name"] not in ("@", "www") and r["type"] not in ("A", "CNAME", "URL", "URL301"):
            keep.append(r)
        # Also keep TXT records that aren't GSC verification
        elif r["type"] == "TXT" and r["name"] == "@":
            # Keep existing TXT unless it's a GSC record we're replacing
            if gsc_verification_txt is None or not r["address"].startswith("google-site-verification"):
                keep.append(r)

    # Build new record set
    new_records = keep + [
        {"name": "@", "type": "A", "address": VERCEL_APEX_IP, "ttl": "1800"},
        {"name": "www", "type": "URL301", "address": f"https://{domain}", "ttl": "1800"},
    ]

    if gsc_verification_txt:
        new_records.append({
            "name": "@",
            "type": "TXT",
            "address": gsc_verification_txt,
            "ttl": "1800",
        })

    set_hosts(domain, new_records)
    logger.info(f"✓ Configured {domain} for Vercel (apex A + www→apex 301)")


def add_txt_record(domain: str, name: str, value: str) -> None:
    """
    Add a TXT record to a domain (merges with existing records).

    Args:
        domain: The domain to update
        name: Record name (e.g., "@" or "_dmarc")
        value: TXT record value
    """
    existing = get_hosts(domain)

    # Remove any existing TXT record with the same name and value
    filtered = [r for r in existing if not (r["type"] == "TXT" and r["name"] == name and r["address"] == value)]

    # Add new TXT record
    filtered.append({
        "name": name,
        "type": "TXT",
        "address": value,
        "ttl": "1800",
    })

    set_hosts(domain, filtered)
    logger.info(f"Added TXT record {name}={value[:50]}... to {domain}")


# CLI for testing
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="Namecheap DNS CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # get
    get_parser = subparsers.add_parser("get", help="Get DNS records for a domain")
    get_parser.add_argument("domain", help="Domain to query")

    # configure
    config_parser = subparsers.add_parser("configure", help="Configure domain for Vercel")
    config_parser.add_argument("domain", help="Domain to configure")
    config_parser.add_argument("--gsc-txt", help="Google Search Console TXT verification value")

    # add-txt
    txt_parser = subparsers.add_parser("add-txt", help="Add a TXT record")
    txt_parser.add_argument("domain", help="Domain to update")
    txt_parser.add_argument("name", help="Record name (e.g., @ or _dmarc)")
    txt_parser.add_argument("value", help="TXT record value")

    args = parser.parse_args()

    if args.command == "get":
        records = get_hosts(args.domain)
        print(f"\n{'NAME':<15} {'TYPE':<10} {'ADDRESS':<50} {'TTL'}")
        print("-" * 85)
        for r in records:
            addr = r["address"][:47] + "..." if len(r["address"]) > 50 else r["address"]
            print(f"{r['name']:<15} {r['type']:<10} {addr:<50} {r['ttl']}")
        print(f"\nTotal: {len(records)} record(s)")

    elif args.command == "configure":
        configure_for_vercel(args.domain, gsc_verification_txt=args.gsc_txt)
        print(f"✓ {args.domain} configured for Vercel")

    elif args.command == "add-txt":
        add_txt_record(args.domain, args.name, args.value)
        print(f"✓ Added TXT record to {args.domain}")
