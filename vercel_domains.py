"""Vercel domains API client for Site Empire.

Provides domain registration, verification polling, and management
for the multi-tenant Vercel project.
"""
from __future__ import annotations

import os
import time
import logging
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("article_factory.vercel_domains")

VERCEL_API = "https://api.vercel.com"


def _headers() -> dict[str, str]:
    """Build authorization headers for Vercel API."""
    token = os.environ.get("VERCEL_TOKEN")
    if not token:
        raise RuntimeError("VERCEL_TOKEN environment variable not set")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _team_param() -> dict[str, str]:
    """Return team ID param if configured."""
    team_id = os.environ.get("VERCEL_TEAM_ID")
    return {"teamId": team_id} if team_id else {}


def _project_id() -> str:
    """Get the Vercel project ID from environment."""
    project = os.environ.get("VERCEL_PROJECT_ID")
    if not project:
        raise RuntimeError("VERCEL_PROJECT_ID environment variable not set")
    return project


def add_domain(domain: str) -> dict:
    """
    Register a domain with the Vercel project.

    Idempotent — if domain already exists, returns success with already_existed=True.

    Args:
        domain: The domain to add (e.g., "example.com")

    Returns:
        dict with domain info including 'name', 'verified', etc.
    """
    project = _project_id()
    url = f"{VERCEL_API}/v10/projects/{project}/domains"

    try:
        r = requests.post(
            url,
            params=_team_param(),
            headers=_headers(),
            json={"name": domain},
            timeout=30,
        )

        # 409 = domain already in use (either this project or another)
        if r.status_code == 409:
            data = r.json()
            error = data.get("error", {})
            # Check if it's already on THIS project
            if error.get("code") == "domain_already_in_use":
                existing = error.get("domain", {})
                if existing.get("projectId") == project:
                    logger.info(f"Domain {domain} already registered with project — skipping")
                    return {"name": domain, "already_existed": True, **existing}
                else:
                    # Domain is on a different project
                    raise RuntimeError(
                        f"Domain {domain} is registered to a different project: "
                        f"{existing.get('projectId')}"
                    )
            logger.info(f"Domain {domain} already registered — skipping")
            return {"name": domain, "already_existed": True}

        r.raise_for_status()
        data = r.json()
        logger.info(f"Added {domain} to Vercel project — verified={data.get('verified')}")
        return data

    except requests.RequestException as e:
        logger.error(f"Failed to add domain {domain}: {e}")
        raise


def verify_domain(domain: str) -> dict:
    """
    Trigger Vercel to re-check DNS for a domain.

    Args:
        domain: The domain to verify

    Returns:
        dict with verification status
    """
    project = _project_id()
    url = f"{VERCEL_API}/v9/projects/{project}/domains/{domain}/verify"

    r = requests.post(url, params=_team_param(), headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.json()


def get_domain_status(domain: str) -> dict:
    """
    Check current verification and configuration status for a domain.

    Args:
        domain: The domain to check

    Returns:
        dict with 'verified', 'name', 'configured', etc.
    """
    project = _project_id()
    url = f"{VERCEL_API}/v9/projects/{project}/domains/{domain}"

    r = requests.get(url, params=_team_param(), headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.json()


def wait_for_verification(
    domain: str,
    timeout_seconds: int = 300,
    poll_interval: int = 20,
) -> bool:
    """
    Poll until domain shows verified=true or timeout.

    Args:
        domain: The domain to wait for
        timeout_seconds: Maximum time to wait (default 5 minutes)
        poll_interval: Seconds between checks (default 20)

    Returns:
        True if verified, False if timeout
    """
    deadline = time.time() + timeout_seconds
    attempt = 0

    while time.time() < deadline:
        attempt += 1
        try:
            # Nudge Vercel to re-check DNS
            verify_domain(domain)
            status = get_domain_status(domain)

            if status.get("verified"):
                logger.info(f"✓ {domain} verified after {attempt} attempts")
                return True

            # Log why it's not verified yet
            verification = status.get("verification", [])
            if verification:
                logger.debug(f"  Pending verification: {verification}")

        except requests.HTTPError as e:
            logger.warning(f"Verification check failed for {domain}: {e}")

        logger.info(f"  …waiting on {domain} verification (attempt {attempt})")
        time.sleep(poll_interval)

    logger.error(f"✗ {domain} did not verify within {timeout_seconds}s")
    return False


def remove_domain(domain: str) -> None:
    """
    Detach a domain from the project.

    Args:
        domain: The domain to remove
    """
    project = _project_id()
    url = f"{VERCEL_API}/v9/projects/{project}/domains/{domain}"

    r = requests.delete(url, params=_team_param(), headers=_headers(), timeout=30)
    r.raise_for_status()
    logger.info(f"Removed {domain} from project")


def list_project_domains() -> list[dict]:
    """
    Return all domains attached to the project.

    Returns:
        List of domain dicts with 'name', 'verified', etc.
    """
    project = _project_id()
    url = f"{VERCEL_API}/v9/projects/{project}/domains"
    params = {**_team_param(), "limit": "100"}

    r = requests.get(url, params=params, headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.json().get("domains", [])


# CLI for testing
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="Vercel domains CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # list
    subparsers.add_parser("list", help="List all project domains")

    # add
    add_parser = subparsers.add_parser("add", help="Add a domain")
    add_parser.add_argument("domain", help="Domain to add")

    # remove
    remove_parser = subparsers.add_parser("remove", help="Remove a domain")
    remove_parser.add_argument("domain", help="Domain to remove")

    # status
    status_parser = subparsers.add_parser("status", help="Check domain status")
    status_parser.add_argument("domain", help="Domain to check")

    # verify
    verify_parser = subparsers.add_parser("verify", help="Wait for domain verification")
    verify_parser.add_argument("domain", help="Domain to verify")
    verify_parser.add_argument("--timeout", type=int, default=300, help="Timeout in seconds")

    args = parser.parse_args()

    if args.command == "list":
        domains = list_project_domains()
        print(f"\n{'DOMAIN':<35} {'VERIFIED':<10} {'CONFIGURED'}")
        print("-" * 60)
        for d in domains:
            print(f"{d['name']:<35} {str(d.get('verified', False)):<10} {d.get('configured', False)}")
        print(f"\nTotal: {len(domains)} domain(s)")

    elif args.command == "add":
        result = add_domain(args.domain)
        print(f"Added: {result}")

    elif args.command == "remove":
        remove_domain(args.domain)
        print(f"Removed: {args.domain}")

    elif args.command == "status":
        status = get_domain_status(args.domain)
        print(f"Status: {status}")

    elif args.command == "verify":
        success = wait_for_verification(args.domain, timeout_seconds=args.timeout)
        if success:
            print(f"✓ {args.domain} is verified")
        else:
            print(f"✗ {args.domain} verification timed out")
            exit(1)
