"""
Unsplash API Client — Source high-quality images for articles.

Uses Unsplash API to search and download images with proper attribution.
Free tier: 50 requests/hour.
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("article_factory.media.unsplash")

UNSPLASH_API_URL = "https://api.unsplash.com"

# Rate limiting
RATE_LIMIT_REQUESTS = 50
RATE_LIMIT_WINDOW = 3600  # 1 hour


@dataclass
class UnsplashPhoto:
    """Represents an Unsplash photo with metadata."""
    id: str
    url: str
    download_url: str
    width: int
    height: int
    description: str
    alt_description: str
    photographer: str
    photographer_url: str
    photographer_username: str

    @property
    def attribution(self) -> str:
        """Generate attribution text per Unsplash guidelines."""
        return f"Photo by {self.photographer} on Unsplash"

    @property
    def attribution_html(self) -> str:
        """Generate HTML attribution link."""
        return (
            f'Photo by <a href="{self.photographer_url}?utm_source=site_empire&utm_medium=referral">'
            f'{self.photographer}</a> on '
            f'<a href="https://unsplash.com/?utm_source=site_empire&utm_medium=referral">Unsplash</a>'
        )


class UnsplashClient:
    """
    Client for Unsplash API.

    Usage:
        client = UnsplashClient()
        photos = client.search("mountain landscape", orientation="landscape")
        if photos:
            image_bytes, photo = client.download(photos[0].id)
    """

    def __init__(self):
        self.access_key = os.getenv("UNSPLASH_ACCESS_KEY")
        self._request_count = 0
        self._window_start = time.time()

        if not self.access_key:
            logger.warning("UNSPLASH_ACCESS_KEY not set - image sourcing unavailable")

    def _check_rate_limit(self) -> bool:
        """Check if we're within rate limits. Returns True if OK to proceed."""
        now = time.time()

        # Reset window if expired
        if now - self._window_start > RATE_LIMIT_WINDOW:
            self._request_count = 0
            self._window_start = now

        if self._request_count >= RATE_LIMIT_REQUESTS:
            remaining = RATE_LIMIT_WINDOW - (now - self._window_start)
            logger.warning(f"Rate limit reached. Resets in {remaining:.0f}s")
            return False

        return True

    def _request(self, endpoint: str, params: dict = None) -> Optional[dict]:
        """Make authenticated request to Unsplash API."""
        if not self.access_key:
            logger.error("Cannot make request - UNSPLASH_ACCESS_KEY not set")
            return None

        if not self._check_rate_limit():
            return None

        self._request_count += 1

        try:
            response = requests.get(
                f"{UNSPLASH_API_URL}{endpoint}",
                params=params or {},
                headers={
                    "Authorization": f"Client-ID {self.access_key}",
                    "Accept-Version": "v1",
                },
                timeout=30,
            )

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 403:
                logger.error("Unsplash API: Rate limit exceeded or invalid key")
            else:
                logger.error(f"Unsplash API error: {response.status_code} - {response.text}")

            return None

        except Exception as e:
            logger.error(f"Unsplash API request failed: {e}")
            return None

    def search(
        self,
        query: str,
        orientation: str = "landscape",
        per_page: int = 10,
        content_filter: str = "high",
    ) -> list[UnsplashPhoto]:
        """
        Search for photos matching query.

        Args:
            query: Search terms (e.g., "mountain landscape", "coffee beans")
            orientation: 'landscape', 'portrait', or 'squarish'
            per_page: Number of results (max 30)
            content_filter: 'low' or 'high' (high = safer content)

        Returns:
            List of UnsplashPhoto objects, sorted by relevance.
        """
        data = self._request("/search/photos", {
            "query": query,
            "orientation": orientation,
            "per_page": min(per_page, 30),
            "content_filter": content_filter,
        })

        if not data or "results" not in data:
            return []

        photos = []
        for item in data["results"]:
            try:
                photo = UnsplashPhoto(
                    id=item["id"],
                    url=item["urls"]["regular"],
                    download_url=item["links"]["download"],
                    width=item["width"],
                    height=item["height"],
                    description=item.get("description") or "",
                    alt_description=item.get("alt_description") or "",
                    photographer=item["user"]["name"],
                    photographer_url=item["user"]["links"]["html"],
                    photographer_username=item["user"]["username"],
                )
                photos.append(photo)
            except KeyError as e:
                logger.warning(f"Skipping malformed photo result: {e}")
                continue

        logger.debug(f"Found {len(photos)} photos for query '{query}'")
        return photos

    def get_photo(self, photo_id: str) -> Optional[UnsplashPhoto]:
        """Get a specific photo by ID."""
        data = self._request(f"/photos/{photo_id}")

        if not data:
            return None

        try:
            return UnsplashPhoto(
                id=data["id"],
                url=data["urls"]["regular"],
                download_url=data["links"]["download"],
                width=data["width"],
                height=data["height"],
                description=data.get("description") or "",
                alt_description=data.get("alt_description") or "",
                photographer=data["user"]["name"],
                photographer_url=data["user"]["links"]["html"],
                photographer_username=data["user"]["username"],
            )
        except KeyError as e:
            logger.error(f"Failed to parse photo {photo_id}: {e}")
            return None

    def download(self, photo_id: str) -> tuple[Optional[bytes], Optional[UnsplashPhoto]]:
        """
        Download a photo and trigger Unsplash download tracking.

        Per Unsplash API guidelines, we must use the download endpoint
        to properly track downloads for photographer credit.

        Args:
            photo_id: Unsplash photo ID

        Returns:
            Tuple of (image_bytes, photo_metadata) or (None, None) on failure
        """
        # Get photo metadata first
        photo = self.get_photo(photo_id)
        if not photo:
            return None, None

        # Trigger download tracking (required by Unsplash TOS)
        download_location = self._request(f"/photos/{photo_id}/download")
        if not download_location:
            logger.warning(f"Failed to trigger download tracking for {photo_id}")
            # Continue anyway - we can still download the image

        # Download the actual image
        try:
            # Use the regular URL for a reasonable size (1080px width)
            response = requests.get(photo.url, timeout=60)
            if response.status_code == 200:
                logger.info(f"Downloaded photo {photo_id} ({len(response.content)} bytes)")
                return response.content, photo
            else:
                logger.error(f"Failed to download photo: {response.status_code}")
                return None, None
        except Exception as e:
            logger.error(f"Failed to download photo {photo_id}: {e}")
            return None, None

    def search_and_download(
        self,
        query: str,
        orientation: str = "landscape",
    ) -> tuple[Optional[bytes], Optional[UnsplashPhoto]]:
        """
        Convenience method: search and download the best match.

        Args:
            query: Search terms
            orientation: Image orientation

        Returns:
            Tuple of (image_bytes, photo_metadata) or (None, None)
        """
        photos = self.search(query, orientation=orientation, per_page=5)
        if not photos:
            logger.warning(f"No photos found for query '{query}'")
            return None, None

        # Take the first (most relevant) result
        return self.download(photos[0].id)


# CLI for testing
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="Unsplash Client CLI")
    parser.add_argument("--search", help="Search query")
    parser.add_argument("--download", help="Photo ID to download")
    parser.add_argument("--orientation", default="landscape", help="Orientation filter")
    parser.add_argument("--output", help="Output file for download")
    args = parser.parse_args()

    client = UnsplashClient()

    if args.search:
        photos = client.search(args.search, orientation=args.orientation)
        print(f"\nFound {len(photos)} photos:\n")
        for i, photo in enumerate(photos[:5], 1):
            print(f"{i}. {photo.id}")
            print(f"   {photo.alt_description or photo.description or '(no description)'}")
            print(f"   {photo.width}x{photo.height}")
            print(f"   {photo.attribution}")
            print()

    if args.download:
        image_bytes, photo = client.download(args.download)
        if image_bytes:
            output = args.output or f"{args.download}.jpg"
            with open(output, "wb") as f:
                f.write(image_bytes)
            print(f"Downloaded to {output}")
            print(f"Attribution: {photo.attribution}")
