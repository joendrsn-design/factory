"""
IndexNow key generation and submission utilities.

IndexNow is a protocol for instant URL indexing supported by Bing, Yandex,
and other search engines. Each domain needs a unique key file deployed at
a known path (e.g., /indexnow-key.txt or /{key}.txt).

Reference: https://www.indexnow.org/documentation
"""
from __future__ import annotations

import logging
import secrets
from typing import Any

import requests

logger = logging.getLogger("article_factory.onboarding.indexnow")

# IndexNow API endpoint (Bing's implementation, also forwards to Yandex)
INDEXNOW_API = "https://api.indexnow.org/indexnow"

# Supported search engines with their own endpoints
SEARCH_ENGINES = {
    "indexnow": "https://api.indexnow.org/indexnow",
    "bing": "https://www.bing.com/indexnow",
    "yandex": "https://yandex.com/indexnow",
}


def generate_key(length: int = 32) -> str:
    """
    Generate a valid IndexNow API key.

    IndexNow keys must be:
    - 8-128 characters long
    - Contain only hex characters (a-f, 0-9)

    Args:
        length: Key length in characters (default: 32, must be 8-128).

    Returns:
        A random hexadecimal string suitable for IndexNow.

    Raises:
        ValueError: If length is outside valid range.
    """
    if not 8 <= length <= 128:
        raise ValueError(f"IndexNow key length must be 8-128, got {length}")

    # Generate random bytes and convert to hex
    # Each byte becomes 2 hex chars, so we need length // 2 bytes
    num_bytes = (length + 1) // 2
    key = secrets.token_hex(num_bytes)[:length]

    logger.debug(f"Generated IndexNow key: {key[:8]}...")
    return key


def get_key_file_path(key: str) -> str:
    """
    Get the standard file path for an IndexNow key.

    The key file can be served at either:
    - /{key}.txt (key as filename)
    - /indexnow-key.txt (fixed filename, key in content)

    We use the fixed filename approach for simplicity.

    Args:
        key: The IndexNow API key.

    Returns:
        The URL path where the key file should be accessible.
    """
    return "/indexnow-key.txt"


def get_key_file_content(key: str) -> str:
    """
    Get the content for the IndexNow key file.

    The file should contain only the key, no whitespace or other content.

    Args:
        key: The IndexNow API key.

    Returns:
        The exact content to serve at the key file URL.
    """
    return key


def submit_url(
    url: str,
    key: str,
    key_location: str | None = None,
    search_engine: str = "indexnow",
) -> bool:
    """
    Submit a URL for instant indexing via IndexNow.

    Args:
        url: The full URL to submit (e.g., "https://example.com/article").
        key: The IndexNow API key for this domain.
        key_location: Optional URL where the key file is hosted.
                     If not provided, derived from the URL domain.
        search_engine: Which search engine to submit to (default: indexnow).
                      Options: indexnow, bing, yandex

    Returns:
        True if submission was accepted (2xx response).

    Note:
        IndexNow has no authentication beyond the key file verification.
        The search engine will verify that the key file exists at the
        specified location before processing the URL.
    """
    # Extract host from URL for keyLocation
    from urllib.parse import urlparse
    parsed = urlparse(url)
    host = parsed.netloc

    if key_location is None:
        key_location = f"https://{host}/indexnow-key.txt"

    endpoint = SEARCH_ENGINES.get(search_engine, INDEXNOW_API)

    params = {
        "url": url,
        "key": key,
        "keyLocation": key_location,
    }

    logger.info(f"Submitting URL to IndexNow: {url}")

    try:
        response = requests.get(endpoint, params=params, timeout=30)

        if response.status_code in (200, 202):
            logger.info(f"URL submitted successfully: {url}")
            return True

        # 400 = Invalid request
        # 403 = Key not valid for this URL
        # 422 = URL doesn't belong to host
        # 429 = Too many requests
        logger.warning(
            f"IndexNow submission failed ({response.status_code}): {url}\n"
            f"  Response: {response.text[:200]}"
        )
        return False

    except requests.RequestException as e:
        logger.warning(f"IndexNow submission error: {e}")
        return False


def submit_urls(
    urls: list[str],
    key: str,
    host: str,
    key_location: str | None = None,
    search_engine: str = "indexnow",
) -> dict[str, Any]:
    """
    Submit multiple URLs for instant indexing via IndexNow batch API.

    More efficient than individual submissions when indexing many URLs.

    Args:
        urls: List of full URLs to submit (max 10,000 per request).
        key: The IndexNow API key for this domain.
        host: The domain host (e.g., "example.com").
        key_location: Optional URL where the key file is hosted.
        search_engine: Which search engine to submit to.

    Returns:
        Dict with submission results:
        - submitted: Number of URLs submitted
        - success: True if batch was accepted
        - error: Error message if failed
    """
    if not urls:
        return {"submitted": 0, "success": True, "error": None}

    if len(urls) > 10000:
        logger.warning(f"IndexNow batch limited to 10,000 URLs, got {len(urls)}")
        urls = urls[:10000]

    if key_location is None:
        key_location = f"https://{host}/indexnow-key.txt"

    endpoint = SEARCH_ENGINES.get(search_engine, INDEXNOW_API)

    payload = {
        "host": host,
        "key": key,
        "keyLocation": key_location,
        "urlList": urls,
    }

    logger.info(f"Submitting {len(urls)} URLs to IndexNow for {host}")

    try:
        response = requests.post(
            endpoint,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=60,
        )

        if response.status_code in (200, 202):
            logger.info(f"Batch submitted successfully: {len(urls)} URLs")
            return {
                "submitted": len(urls),
                "success": True,
                "error": None,
            }

        error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
        logger.warning(f"IndexNow batch submission failed: {error_msg}")
        return {
            "submitted": 0,
            "success": False,
            "error": error_msg,
        }

    except requests.RequestException as e:
        logger.warning(f"IndexNow batch submission error: {e}")
        return {
            "submitted": 0,
            "success": False,
            "error": str(e),
        }
