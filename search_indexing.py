"""Auto-register sites with Google Search Console + Bing Webmaster Tools.

Handles property creation and sitemap submission for SEO visibility.
"""
from __future__ import annotations

import os
import logging
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("article_factory.search_indexing")


# ---------- Google Search Console ----------

def _gsc_service():
    """
    Build Google Search Console API service.

    Requires GSC_SERVICE_ACCOUNT_JSON environment variable pointing to
    a service account JSON file with Search Console API access.
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        raise RuntimeError(
            "Google API client not installed. Run: pip install google-api-python-client google-auth"
        )

    json_path = os.environ.get("GSC_SERVICE_ACCOUNT_JSON")
    if not json_path:
        raise RuntimeError("GSC_SERVICE_ACCOUNT_JSON environment variable not set")

    if not os.path.exists(json_path):
        raise RuntimeError(f"GSC service account file not found: {json_path}")

    creds = service_account.Credentials.from_service_account_file(
        json_path,
        scopes=["https://www.googleapis.com/auth/webmasters"],
    )
    return build("searchconsole", "v1", credentials=creds)


def gsc_add_property(domain: str) -> None:
    """
    Add a domain property (sc-domain:example.com) to Google Search Console.

    Note: Requires DNS TXT verification. The service account must be added
    as an owner of the property, or you need to verify via DNS first.

    Args:
        domain: The domain to add (e.g., "example.com")
    """
    service = _gsc_service()
    site_url = f"sc-domain:{domain}"

    try:
        service.sites().add(siteUrl=site_url).execute()
        logger.info(f"Added {site_url} to Google Search Console")
    except Exception as e:
        error_str = str(e).lower()
        if "already" in error_str or "exists" in error_str:
            logger.info(f"{site_url} already in GSC")
        elif "forbidden" in error_str or "403" in error_str:
            logger.warning(
                f"GSC permission denied for {domain}. "
                "Ensure the service account is added as an owner in GSC."
            )
            raise
        else:
            raise


def gsc_get_verification_token(domain: str) -> Optional[str]:
    """
    Get the DNS TXT record value for domain verification.

    Note: This requires the Site Verification API, not the Search Console API.
    For initial setup, it's easier to get this from the GSC UI.

    Args:
        domain: The domain to get verification for

    Returns:
        TXT record value or None if not available
    """
    # The Search Console API doesn't directly provide verification tokens.
    # You need the Site Verification API for that:
    # https://developers.google.com/site-verification/v1/getting_started
    #
    # For simplicity, this returns None and logs instructions.
    logger.info(
        f"To verify {domain} in GSC:\n"
        f"  1. Go to https://search.google.com/search-console\n"
        f"  2. Add property: sc-domain:{domain}\n"
        f"  3. Copy the TXT record value\n"
        f"  4. Add it to DNS: namecheap_dns.add_txt_record('{domain}', '@', '<value>')"
    )
    return None


def gsc_submit_sitemap(domain: str, sitemap_path: str = "sitemap.xml") -> None:
    """
    Submit a sitemap to Google Search Console.

    Args:
        domain: The domain (must already be verified in GSC)
        sitemap_path: Path to sitemap (default: sitemap.xml)
    """
    service = _gsc_service()
    site_url = f"sc-domain:{domain}"
    sitemap_url = f"https://{domain}/{sitemap_path}"

    try:
        service.sitemaps().submit(siteUrl=site_url, feedpath=sitemap_url).execute()
        logger.info(f"Submitted sitemap {sitemap_url} to GSC")
    except Exception as e:
        if "not found" in str(e).lower() or "404" in str(e):
            logger.error(f"Property {site_url} not found in GSC. Verify domain first.")
        raise


def gsc_list_sitemaps(domain: str) -> list[dict]:
    """
    List all sitemaps for a domain in GSC.

    Args:
        domain: The domain to query

    Returns:
        List of sitemap info dicts
    """
    service = _gsc_service()
    site_url = f"sc-domain:{domain}"

    try:
        response = service.sitemaps().list(siteUrl=site_url).execute()
        return response.get("sitemap", [])
    except Exception as e:
        logger.error(f"Failed to list sitemaps for {domain}: {e}")
        return []


# ---------- Bing Webmaster Tools ----------

BING_API = "https://ssl.bing.com/webmaster/api.svc/json"


def _bing_api_key() -> str:
    """Get Bing Webmaster API key from environment."""
    key = os.environ.get("BING_WEBMASTER_API_KEY")
    if not key:
        raise RuntimeError("BING_WEBMASTER_API_KEY environment variable not set")
    return key


def bing_add_site(domain: str) -> None:
    """
    Add a site to Bing Webmaster Tools.

    Args:
        domain: The domain to add (e.g., "example.com")
    """
    api_key = _bing_api_key()
    site_url = f"https://{domain}/"

    try:
        r = requests.post(
            f"{BING_API}/AddSite",
            params={"apikey": api_key},
            json={"siteUrl": site_url},
            timeout=30,
        )

        # Bing returns 200 even on errors, check response content
        if r.status_code == 200:
            try:
                data = r.json()
                # Bing returns the URL on success, or error info
                if isinstance(data, str) and domain in data:
                    logger.info(f"Added {domain} to Bing Webmaster Tools")
                    return
                elif isinstance(data, dict) and data.get("ErrorCode"):
                    error_code = data.get("ErrorCode")
                    if error_code == 4:  # Site already exists
                        logger.info(f"{domain} already in Bing Webmaster Tools")
                        return
                    else:
                        raise RuntimeError(f"Bing error {error_code}: {data.get('Message')}")
            except ValueError:
                # Not JSON, might be success
                pass

        r.raise_for_status()
        logger.info(f"Added {domain} to Bing Webmaster Tools")

    except requests.RequestException as e:
        logger.error(f"Failed to add {domain} to Bing: {e}")
        raise


def bing_submit_sitemap(domain: str, sitemap_path: str = "sitemap.xml") -> None:
    """
    Submit a sitemap to Bing Webmaster Tools.

    Args:
        domain: The domain (must already be added to Bing)
        sitemap_path: Path to sitemap (default: sitemap.xml)
    """
    api_key = _bing_api_key()
    site_url = f"https://{domain}/"
    sitemap_url = f"https://{domain}/{sitemap_path}"

    try:
        r = requests.post(
            f"{BING_API}/SubmitFeed",
            params={"apikey": api_key},
            json={"siteUrl": site_url, "feedUrl": sitemap_url},
            timeout=30,
        )
        r.raise_for_status()
        logger.info(f"Submitted sitemap {sitemap_url} to Bing")
    except requests.RequestException as e:
        logger.error(f"Failed to submit sitemap to Bing for {domain}: {e}")
        raise


def bing_get_site_info(domain: str) -> Optional[dict]:
    """
    Get site info from Bing Webmaster Tools.

    Args:
        domain: The domain to query

    Returns:
        Site info dict or None if not found
    """
    api_key = _bing_api_key()
    site_url = f"https://{domain}/"

    try:
        r = requests.get(
            f"{BING_API}/GetSiteList",
            params={"apikey": api_key},
            timeout=30,
        )
        r.raise_for_status()
        sites = r.json()

        for site in sites if isinstance(sites, list) else []:
            if domain in site.get("Url", ""):
                return site

        return None
    except Exception as e:
        logger.error(f"Failed to get Bing site info for {domain}: {e}")
        return None


# ---------- Combined ----------

def index_everywhere(domain: str, skip_gsc: bool = False, skip_bing: bool = False) -> dict:
    """
    Full SEO submission: GSC + Bing properties and sitemaps.

    Args:
        domain: The domain to index
        skip_gsc: Skip Google Search Console
        skip_bing: Skip Bing Webmaster Tools

    Returns:
        Dict with 'gsc' and 'bing' status ('ok', 'skipped', or error message)
    """
    results = {"gsc": "skipped", "bing": "skipped"}

    if not skip_gsc:
        try:
            gsc_add_property(domain)
            gsc_submit_sitemap(domain)
            results["gsc"] = "ok"
        except Exception as e:
            results["gsc"] = str(e)
            logger.error(f"GSC failed for {domain}: {e}")

    if not skip_bing:
        try:
            bing_add_site(domain)
            bing_submit_sitemap(domain)
            results["bing"] = "ok"
        except Exception as e:
            results["bing"] = str(e)
            logger.error(f"Bing failed for {domain}: {e}")

    return results


# CLI for testing
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="Search indexing CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # index
    index_parser = subparsers.add_parser("index", help="Index a domain everywhere")
    index_parser.add_argument("domain", help="Domain to index")
    index_parser.add_argument("--skip-gsc", action="store_true", help="Skip Google Search Console")
    index_parser.add_argument("--skip-bing", action="store_true", help="Skip Bing Webmaster Tools")

    # gsc-add
    gsc_add_parser = subparsers.add_parser("gsc-add", help="Add domain to GSC")
    gsc_add_parser.add_argument("domain", help="Domain to add")

    # gsc-sitemap
    gsc_sitemap_parser = subparsers.add_parser("gsc-sitemap", help="Submit sitemap to GSC")
    gsc_sitemap_parser.add_argument("domain", help="Domain")
    gsc_sitemap_parser.add_argument("--path", default="sitemap.xml", help="Sitemap path")

    # bing-add
    bing_add_parser = subparsers.add_parser("bing-add", help="Add domain to Bing")
    bing_add_parser.add_argument("domain", help="Domain to add")

    # bing-sitemap
    bing_sitemap_parser = subparsers.add_parser("bing-sitemap", help="Submit sitemap to Bing")
    bing_sitemap_parser.add_argument("domain", help="Domain")
    bing_sitemap_parser.add_argument("--path", default="sitemap.xml", help="Sitemap path")

    args = parser.parse_args()

    if args.command == "index":
        results = index_everywhere(args.domain, skip_gsc=args.skip_gsc, skip_bing=args.skip_bing)
        print(f"\nIndexing results for {args.domain}:")
        print(f"  GSC: {results['gsc']}")
        print(f"  Bing: {results['bing']}")

    elif args.command == "gsc-add":
        gsc_add_property(args.domain)
        print(f"✓ Added {args.domain} to GSC")

    elif args.command == "gsc-sitemap":
        gsc_submit_sitemap(args.domain, args.path)
        print(f"✓ Submitted sitemap for {args.domain}")

    elif args.command == "bing-add":
        bing_add_site(args.domain)
        print(f"✓ Added {args.domain} to Bing")

    elif args.command == "bing-sitemap":
        bing_submit_sitemap(args.domain, args.path)
        print(f"✓ Submitted sitemap for {args.domain}")
