"""
DNS resolution and propagation polling utilities.

Uses dnspython to query specific DNS resolvers and wait for
records to propagate across multiple public resolvers.
"""
from __future__ import annotations

import logging
import time

import dns.resolver
import dns.exception

from .errors import DNSPropagationError

logger = logging.getLogger("article_factory.onboarding.dns_utils")

# Public DNS resolvers to check for propagation
PUBLIC_RESOLVERS = ["8.8.8.8", "1.1.1.1", "9.9.9.9"]


def _create_resolver(resolver_ip: str, timeout: int = 10) -> dns.resolver.Resolver:
    """
    Create a DNS resolver configured for a specific nameserver.

    Args:
        resolver_ip: IP address of the DNS server to query.
        timeout: Query timeout in seconds.

    Returns:
        Configured Resolver instance.
    """
    resolver = dns.resolver.Resolver()
    resolver.nameservers = [resolver_ip]
    resolver.lifetime = timeout
    resolver.timeout = timeout
    return resolver


# IMPORTANT: Do not "simplify" this by using str(rdata) instead of rdata.strings.
# str(rdata) wraps TXT values in literal double quotes (e.g., '"value"'), which
# breaks equality comparisons. This caused a false-timeout bug where Quad9 appeared
# to fail propagation checks. rdata.strings is the canonical dnspython API that
# returns raw bytes without quote wrapping.
def _normalize_txt_value(rdata) -> str:
    """Return the canonical string value of a TXT record.

    Joins multi-segment TXT records (DNS splits values >255 bytes into
    multiple segments) and decodes as UTF-8. Returns the raw value with
    no surrounding quote characters — suitable for direct equality
    comparison against an expected value.
    """
    return b''.join(rdata.strings).decode('utf-8')


def resolve_txt(domain: str, resolver_ip: str, timeout: int = 10) -> list[str]:
    """
    Resolve TXT records for a domain using a specific resolver.

    Args:
        domain: The domain to query.
        resolver_ip: IP of the DNS resolver to use.
        timeout: Query timeout in seconds.

    Returns:
        List of TXT record values (strings).
    """
    resolver = _create_resolver(resolver_ip, timeout)

    try:
        answers = resolver.resolve(domain, "TXT")
        return [_normalize_txt_value(rdata) for rdata in answers]
    except dns.resolver.NXDOMAIN:
        logger.debug(f"TXT lookup: {domain} does not exist (NXDOMAIN) via {resolver_ip}")
        return []
    except dns.resolver.NoAnswer:
        logger.debug(f"TXT lookup: No TXT records for {domain} via {resolver_ip}")
        return []
    except dns.exception.DNSException as e:
        logger.debug(f"TXT lookup failed for {domain} via {resolver_ip}: {e}")
        return []


def resolve_a(domain: str, resolver_ip: str, timeout: int = 10) -> list[str]:
    """
    Resolve A records for a domain using a specific resolver.

    Args:
        domain: The domain to query.
        resolver_ip: IP of the DNS resolver to use.
        timeout: Query timeout in seconds.

    Returns:
        List of IP addresses (strings).
    """
    resolver = _create_resolver(resolver_ip, timeout)

    try:
        answers = resolver.resolve(domain, "A")
        return [rdata.address for rdata in answers]
    except dns.resolver.NXDOMAIN:
        logger.debug(f"A lookup: {domain} does not exist (NXDOMAIN) via {resolver_ip}")
        return []
    except dns.resolver.NoAnswer:
        logger.debug(f"A lookup: No A records for {domain} via {resolver_ip}")
        return []
    except dns.exception.DNSException as e:
        logger.debug(f"A lookup failed for {domain} via {resolver_ip}: {e}")
        return []


def resolve_cname(domain: str, resolver_ip: str, timeout: int = 10) -> str | None:
    """
    Resolve CNAME record for a domain using a specific resolver.

    Args:
        domain: The domain to query.
        resolver_ip: IP of the DNS resolver to use.
        timeout: Query timeout in seconds.

    Returns:
        The CNAME target, or None if no CNAME exists.
    """
    resolver = _create_resolver(resolver_ip, timeout)

    try:
        answers = resolver.resolve(domain, "CNAME")
        # CNAME should have exactly one record
        for rdata in answers:
            return str(rdata.target).rstrip(".")
        return None
    except dns.resolver.NXDOMAIN:
        logger.debug(f"CNAME lookup: {domain} does not exist (NXDOMAIN) via {resolver_ip}")
        return None
    except dns.resolver.NoAnswer:
        logger.debug(f"CNAME lookup: No CNAME for {domain} via {resolver_ip}")
        return None
    except dns.exception.DNSException as e:
        logger.debug(f"CNAME lookup failed for {domain} via {resolver_ip}: {e}")
        return None


def _check_record_at_resolver(
    domain: str,
    record_type: str,
    expected_value: str,
    resolver_ip: str,
) -> bool:
    """
    Check if a specific resolver returns the expected value.

    Args:
        domain: The domain to query.
        record_type: Record type (A, CNAME, TXT).
        expected_value: The value we're waiting for.
        resolver_ip: IP of the DNS resolver to use.

    Returns:
        True if the expected value was found.
    """
    record_type = record_type.upper()

    if record_type == "A":
        values = resolve_a(domain, resolver_ip)
        return expected_value in values

    elif record_type == "CNAME":
        value = resolve_cname(domain, resolver_ip)
        if value is None:
            return False
        # Compare without trailing dots, case-insensitive
        return value.lower().rstrip(".") == expected_value.lower().rstrip(".")

    elif record_type == "TXT":
        values = resolve_txt(domain, resolver_ip)
        # Use exact list membership - expected_value must match one record exactly
        return expected_value in values

    else:
        logger.warning(f"Unsupported record type: {record_type}")
        return False


def wait_for_propagation(
    domain: str,
    record_type: str,
    expected_value: str,
    timeout_seconds: int = 300,
    poll_interval: int = 15,
    min_resolvers: int = 3,
) -> None:
    """
    Wait for a DNS record to propagate to all public resolvers.

    Polls three public DNS resolvers (Google, Cloudflare, Quad9) until
    all of them return the expected value, or timeout is reached.

    Args:
        domain: The domain to check.
        record_type: Record type (A, CNAME, TXT).
        expected_value: The value to wait for.
        timeout_seconds: Maximum time to wait (default 5 minutes).
        poll_interval: Seconds between checks (default 15).

    Raises:
        DNSPropagationError: If timeout is reached before all resolvers agree.
    """
    logger.info(
        f"Waiting for {record_type} record propagation: {domain} -> {expected_value}"
    )

    start_time = time.time()
    resolvers_confirmed: set[str] = set()

    while True:
        elapsed = time.time() - start_time

        # Check each resolver that hasn't confirmed yet
        for resolver_ip in PUBLIC_RESOLVERS:
            if resolver_ip in resolvers_confirmed:
                continue

            if _check_record_at_resolver(domain, record_type, expected_value, resolver_ip):
                resolvers_confirmed.add(resolver_ip)
                logger.info(f"  {resolver_ip}: confirmed ({len(resolvers_confirmed)}/3)")

        # Success if enough resolvers confirmed (default 2/3)
        if len(resolvers_confirmed) >= min_resolvers:
            logger.info(f"DNS propagation complete for {domain} ({int(elapsed)}s, {len(resolvers_confirmed)}/{len(PUBLIC_RESOLVERS)} resolvers)")
            return

        # Check timeout
        if elapsed >= timeout_seconds:
            pending = [r for r in PUBLIC_RESOLVERS if r not in resolvers_confirmed]
            raise DNSPropagationError(
                f"DNS propagation timeout after {timeout_seconds}s for {domain}.\n"
                f"  Record: {record_type} -> {expected_value}\n"
                f"  Confirmed: {list(resolvers_confirmed)} ({len(resolvers_confirmed)}/{min_resolvers} required)\n"
                f"  Still pending: {pending}"
            )

        # Wait before next poll
        remaining = timeout_seconds - elapsed
        wait_time = min(poll_interval, remaining)
        logger.debug(
            f"Propagation check: {len(resolvers_confirmed)}/3 confirmed, "
            f"waiting {wait_time}s ({int(elapsed)}s elapsed)"
        )
        time.sleep(wait_time)


def check_current_records(domain: str, resolver_ip: str = "8.8.8.8") -> dict[str, list[str]]:
    """
    Get current A, CNAME, and TXT records for a domain.

    Useful for debugging and dry-run output.

    Args:
        domain: The domain to query.
        resolver_ip: DNS resolver to use (default: Google).

    Returns:
        Dict with keys "A", "CNAME", "TXT" and their current values.
    """
    return {
        "A": resolve_a(domain, resolver_ip),
        "CNAME": [resolve_cname(domain, resolver_ip) or ""],
        "TXT": resolve_txt(domain, resolver_ip),
    }
