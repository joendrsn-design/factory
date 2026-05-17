"""Single source of truth for the list of Site Empire tenants.

Reads from existing config/sites/*.yaml files to provide a unified registry.
"""
from __future__ import annotations

import os
import logging
from pathlib import Path
from functools import lru_cache
from typing import Optional

import yaml
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("article_factory.sites_registry")

SITES_CONFIG_DIR = os.environ.get("SITES_CONFIG_DIR", "config/sites")


def _load_site_yaml(path: Path) -> dict:
    """Load a single site YAML file."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data


@lru_cache(maxsize=1)
def all_sites() -> tuple[dict, ...]:
    """
    Returns all active sites from config/sites/*.yaml.

    Returns: tuple of dicts with keys: site_id, slug, domain, tier, niche, ...
    Tiers: 'flagship', 'standard', 'daily', 'micro', 'utility', 'geo'

    Note: Returns tuple for hashability (lru_cache compatibility).
    """
    config_dir = Path(SITES_CONFIG_DIR)
    if not config_dir.exists():
        logger.warning(f"Sites config directory not found: {config_dir}")
        return tuple()

    sites = []
    for yaml_file in sorted(config_dir.glob("*.yaml")):
        # Skip archived sites
        if yaml_file.parent.name == "_archived":
            continue

        try:
            data = _load_site_yaml(yaml_file)
            # Normalize: ensure both site_id and slug are present
            site_id = data.get("site_id", yaml_file.stem)
            sites.append({
                "site_id": site_id,
                "slug": site_id,  # alias for compatibility
                "domain": data.get("domain", ""),
                "tier": data.get("tier", "standard"),
                "niche": data.get("niche", ""),
                "site_name": data.get("site_name", site_id),
                "template": data.get("template", "magazine"),
                "categories": data.get("categories", []),
            })
        except Exception as e:
            logger.error(f"Failed to load {yaml_file}: {e}")
            continue

    logger.debug(f"Loaded {len(sites)} sites from {config_dir}")
    return tuple(sites)


def all_sites_list() -> list[dict]:
    """Returns all_sites() as a mutable list."""
    return list(all_sites())


def get_site(slug: str) -> dict:
    """
    Get a site by its slug/site_id.

    Args:
        slug: The site_id or slug to look up

    Returns:
        Site dict with keys: site_id, slug, domain, tier, niche, etc.

    Raises:
        KeyError: If no site with the given slug exists
    """
    for site in all_sites():
        if site["site_id"] == slug or site["slug"] == slug:
            return dict(site)  # Return a copy
    raise KeyError(f"No site with slug={slug}")


def get_site_by_domain(domain: str) -> Optional[dict]:
    """
    Get a site by its domain.

    Args:
        domain: The domain to look up (e.g., "lamphill.org")

    Returns:
        Site dict or None if not found
    """
    domain = domain.lower().strip()
    for site in all_sites():
        if site["domain"].lower() == domain:
            return dict(site)
    return None


def clear_cache() -> None:
    """Clear the sites cache (useful after config changes)."""
    all_sites.cache_clear()
    logger.info("Sites registry cache cleared")


# CLI for testing
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="Sites registry CLI")
    parser.add_argument("command", choices=["list", "get"], help="Command to run")
    parser.add_argument("--slug", help="Site slug for 'get' command")
    parser.add_argument("--domain", help="Domain for lookup")
    args = parser.parse_args()

    if args.command == "list":
        sites = all_sites()
        print(f"\n{'SITE ID':<25} {'DOMAIN':<30} {'TIER':<12} {'NICHE'}")
        print("-" * 80)
        for s in sites:
            print(f"{s['site_id']:<25} {s['domain']:<30} {s['tier']:<12} {s['niche']}")
        print(f"\nTotal: {len(sites)} site(s)")

    elif args.command == "get":
        if args.domain:
            site = get_site_by_domain(args.domain)
            if site:
                print(site)
            else:
                print(f"No site found for domain: {args.domain}")
        elif args.slug:
            try:
                site = get_site(args.slug)
                print(site)
            except KeyError as e:
                print(e)
        else:
            print("Error: --slug or --domain required for 'get' command")
