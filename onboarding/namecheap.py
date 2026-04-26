"""
Namecheap API client for DNS record management.

Uses the Namecheap XML API to read and write DNS host records.

CRITICAL: The setHosts command REPLACES the entire DNS record set.
Always fetch existing records first, merge changes, then submit the complete list.
"""
from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from typing import Any

import requests

from .config import Config
from .errors import NamecheapError

logger = logging.getLogger("article_factory.onboarding.namecheap")

API_URL = "https://api.namecheap.com/xml.response"
NAMESPACE = "http://api.namecheap.com/xml.response"


def _strip_namespace(element: ET.Element) -> None:
    """
    Recursively strip the XML namespace from element tags.

    Namecheap uses a default namespace that complicates XPath queries.
    This helper removes it for easier parsing.
    """
    for el in element.iter():
        if el.tag.startswith(f"{{{NAMESPACE}}}"):
            el.tag = el.tag[len(f"{{{NAMESPACE}}}"):]


def _split_domain(domain: str) -> tuple[str, str]:
    """
    Split a domain into SLD (second-level domain) and TLD.

    Example: "example.com" -> ("example", "com")
             "sub.example.co.uk" -> ("sub.example", "co.uk")

    For simplicity, assumes standard TLDs. Multi-part TLDs like .co.uk
    would need a public suffix list for full accuracy.
    """
    parts = domain.lower().split(".")
    if len(parts) < 2:
        raise NamecheapError(f"Invalid domain format: {domain}")

    # Handle common two-part TLDs
    two_part_tlds = {"co.uk", "com.au", "co.nz", "org.uk", "net.au"}
    if len(parts) >= 3:
        potential_tld = f"{parts[-2]}.{parts[-1]}"
        if potential_tld in two_part_tlds:
            return ".".join(parts[:-2]), potential_tld

    return ".".join(parts[:-1]), parts[-1]


class NamecheapClient:
    """
    Client for the Namecheap DNS API.

    Handles authentication, XML parsing, and provides idempotent
    record management methods.
    """

    def __init__(self, config: Config):
        """
        Initialize the Namecheap client.

        Args:
            config: Configuration containing API credentials.
        """
        self.api_user = config.namecheap_api_user
        self.api_key = config.namecheap_api_key
        self.username = config.namecheap_username
        self.client_ip = config.namecheap_client_ip

    def _make_request(self, command: str, extra_params: dict[str, str] | None = None) -> ET.Element:
        """
        Make an authenticated request to the Namecheap API.

        Args:
            command: The API command (e.g., "namecheap.domains.dns.getHosts")
            extra_params: Additional parameters for this specific command.

        Returns:
            Parsed XML root element with namespace stripped.

        Raises:
            NamecheapError: On HTTP errors or API-level errors.
        """
        params = {
            "ApiUser": self.api_user,
            "ApiKey": self.api_key,
            "UserName": self.username,
            "ClientIp": self.client_ip,
            "Command": command,
        }
        if extra_params:
            params.update(extra_params)

        logger.debug(f"Namecheap API: {command}")

        try:
            response = requests.get(API_URL, params=params, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            raise NamecheapError(f"Namecheap API request failed: {e}") from e

        # Parse XML response
        try:
            root = ET.fromstring(response.text)
        except ET.ParseError as e:
            raise NamecheapError(f"Failed to parse Namecheap XML response: {e}") from e

        _strip_namespace(root)

        # Check for API-level errors
        status = root.get("Status", "")
        if status.upper() != "OK":
            errors_elem = root.find(".//Errors")
            if errors_elem is not None:
                error_msgs = [err.text for err in errors_elem.findall("Error") if err.text]
                raise NamecheapError(f"Namecheap API error: {'; '.join(error_msgs)}")
            raise NamecheapError(f"Namecheap API returned status: {status}")

        return root

    def domain_exists(self, domain: str) -> bool:
        """
        Check if a domain exists in this Namecheap account.

        Args:
            domain: The domain to check (e.g., "example.com").

        Returns:
            True if the domain is registered under this account.
        """
        logger.debug(f"Checking if domain exists: {domain}")

        root = self._make_request("namecheap.domains.getList")

        domains_elem = root.find(".//DomainGetListResult")
        if domains_elem is None:
            return False

        domain_lower = domain.lower()
        for domain_elem in domains_elem.findall("Domain"):
            name = domain_elem.get("Name", "").lower()
            if name == domain_lower:
                return True

        return False

    def get_host_records(self, domain: str) -> list[dict[str, Any]]:
        """
        Fetch all DNS host records for a domain.

        Args:
            domain: The domain to query (e.g., "example.com").

        Returns:
            List of record dicts with keys: type, name, address, ttl, mx_pref
        """
        sld, tld = _split_domain(domain)
        logger.debug(f"Fetching DNS records for {domain} (SLD={sld}, TLD={tld})")

        root = self._make_request(
            "namecheap.domains.dns.getHosts",
            {"SLD": sld, "TLD": tld}
        )

        records: list[dict[str, Any]] = []
        hosts_elem = root.find(".//DomainDNSGetHostsResult")

        if hosts_elem is None:
            logger.warning(f"No DomainDNSGetHostsResult found for {domain}")
            return records

        for host in hosts_elem.findall("host"):
            record = {
                "type": host.get("Type", ""),
                "name": host.get("Name", ""),
                "address": host.get("Address", ""),
                "ttl": int(host.get("TTL", "1800")),
                "mx_pref": int(host.get("MXPref", "10")) if host.get("MXPref") else None,
            }
            records.append(record)

        logger.debug(f"Found {len(records)} existing records for {domain}")
        return records

    def set_host_records(self, domain: str, records: list[dict[str, Any]]) -> None:
        """
        Set the complete DNS host record set for a domain.

        WARNING: This REPLACES all existing records. Always fetch existing
        records first with get_host_records() and merge your changes.

        Args:
            domain: The domain to update (e.g., "example.com").
            records: Complete list of records to set.

        Raises:
            NamecheapError: On API errors.
        """
        sld, tld = _split_domain(domain)
        logger.info(f"Setting {len(records)} DNS records for {domain}")

        params = {"SLD": sld, "TLD": tld}

        # Build record parameters (HostName1, RecordType1, Address1, etc.)
        for i, record in enumerate(records, start=1):
            params[f"HostName{i}"] = record["name"]
            params[f"RecordType{i}"] = record["type"]
            params[f"Address{i}"] = record["address"]
            params[f"TTL{i}"] = str(record.get("ttl", 1800))

            if record["type"].upper() == "MX" and record.get("mx_pref") is not None:
                params[f"MXPref{i}"] = str(record["mx_pref"])

        self._make_request("namecheap.domains.dns.setHosts", params)
        logger.info(f"Successfully updated DNS records for {domain}")

    def add_record(
        self,
        domain: str,
        record_type: str,
        host: str,
        value: str,
        ttl: int = 1800,
    ) -> None:
        """
        Add a DNS record idempotently.

        Fetches existing records, checks if this record already exists,
        adds it only if not present, then calls set_host_records with
        the complete list.

        Idempotency check compares (type, name.lower(), address) tuples.

        Args:
            domain: The domain to update (e.g., "example.com").
            record_type: Record type (A, CNAME, TXT, MX, etc.).
            host: Host/name field (e.g., "@", "www", "subdomain").
            value: The record value/address.
            ttl: Time-to-live in seconds (default 1800).
        """
        logger.info(f"Adding {record_type} record: {host}.{domain} -> {value}")

        # Fetch existing records
        existing = self.get_host_records(domain)

        # Check for existing record with same (type, name, address)
        record_key = (record_type.upper(), host.lower(), value)

        for rec in existing:
            existing_key = (rec["type"].upper(), rec["name"].lower(), rec["address"])
            if existing_key == record_key:
                logger.info(f"Record already exists: {record_type} {host} -> {value}")
                return

        # Add the new record
        new_record = {
            "type": record_type,
            "name": host,
            "address": value,
            "ttl": ttl,
            "mx_pref": None,
        }
        existing.append(new_record)

        # Write the complete record set
        self.set_host_records(domain, existing)
        logger.info(f"Successfully added {record_type} record for {host}.{domain}")

    def record_exists(
        self,
        domain: str,
        record_type: str,
        host: str,
        value: str | None = None,
    ) -> bool:
        """
        Check if a record exists (optionally matching a specific value).

        Args:
            domain: The domain to check.
            record_type: Record type to match.
            host: Host/name to match.
            value: Optional value to match. If None, matches any value.

        Returns:
            True if a matching record exists.
        """
        existing = self.get_host_records(domain)

        for rec in existing:
            if rec["type"].upper() == record_type.upper() and rec["name"].lower() == host.lower():
                if value is None or rec["address"] == value:
                    return True

        return False
