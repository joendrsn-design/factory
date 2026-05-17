"""
Image Pipeline — Orchestrates image sourcing, optimization, and upload.

Full flow: Unsplash search → Download → Optimize → R2 upload → Return metadata
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Optional

import requests
from dotenv import load_dotenv

from .unsplash import UnsplashClient, UnsplashPhoto
from .optimizer import ImageOptimizer, OptimizedImage
from .storage import R2Storage

load_dotenv()

logger = logging.getLogger("article_factory.media.pipeline")


@dataclass
class ArticleImage:
    """Complete metadata for an article image."""
    url: str
    alt_text: str
    width: int
    height: int
    format: str
    file_size_bytes: int
    hash: str
    role: str = "hero"

    # Source attribution (Unsplash)
    source_url: Optional[str] = None
    source_id: Optional[str] = None
    photographer: Optional[str] = None
    photographer_url: Optional[str] = None
    attribution: Optional[str] = None


class ImagePipeline:
    """
    End-to-end image pipeline for articles.

    Usage:
        pipeline = ImagePipeline()
        image = pipeline.source_hero_image(
            title="NAD+ Boosters for Longevity",
            topic="NAD+ supplementation mitochondrial health",
            site_slug="lamphill",
            article_slug="nad-boosters-longevity",
        )
    """

    def __init__(self):
        self.unsplash = UnsplashClient()
        self.optimizer = ImageOptimizer()
        self.storage = R2Storage()

        # Check for Supabase access (for deduplication)
        self.supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    def _extract_keywords(self, title: str, topic: str) -> str:
        """Extract search keywords from title and topic."""
        # Combine and clean
        text = f"{title} {topic}"

        # Remove common words that don't help image search
        stopwords = {
            "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
            "of", "with", "by", "from", "as", "is", "was", "are", "were", "been",
            "be", "have", "has", "had", "do", "does", "did", "will", "would",
            "could", "should", "may", "might", "must", "shall", "can", "need",
            "this", "that", "these", "those", "it", "its", "vs", "versus",
            "how", "what", "why", "when", "where", "which", "who", "whom",
            "your", "you", "our", "their", "my", "his", "her",
        }

        words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
        keywords = [w for w in words if w not in stopwords]

        # Take first 5 unique keywords
        seen = set()
        unique = []
        for w in keywords:
            if w not in seen:
                seen.add(w)
                unique.append(w)
            if len(unique) >= 5:
                break

        return " ".join(unique)

    def _generate_alt_text(
        self,
        title: str,
        photo: Optional[UnsplashPhoto],
    ) -> str:
        """Generate descriptive alt text for image."""
        # Prefer Unsplash's alt_description if available
        if photo and photo.alt_description:
            return photo.alt_description

        # Fall back to title-based alt text
        # Remove common title patterns that don't describe images
        alt = title
        alt = re.sub(r'^(How to|Why|What|When|The|A|An)\s+', '', alt, flags=re.IGNORECASE)
        alt = re.sub(r'\s*[:\-–—]\s*.*$', '', alt)  # Remove subtitle
        alt = alt.strip()

        # Ensure it's descriptive
        if len(alt) < 10:
            alt = f"Illustration for {title}"

        return alt[:200]  # Max 200 chars

    def _check_hash_exists(self, image_hash: str) -> Optional[str]:
        """Check if image hash already exists in database. Returns URL if found."""
        if not self.supabase_url or not self.supabase_key:
            return None

        try:
            response = requests.get(
                f"{self.supabase_url}/rest/v1/article_images",
                headers={
                    "apikey": self.supabase_key,
                    "Authorization": f"Bearer {self.supabase_key}",
                },
                params={
                    "select": "r2_uri",
                    "r2_hash": f"eq.{image_hash}",
                    "limit": 1,
                },
                timeout=10,
            )

            if response.status_code == 200 and response.json():
                return response.json()[0]["r2_uri"]
        except Exception as e:
            logger.warning(f"Failed to check hash existence: {e}")

        return None

    def _save_image_record(
        self,
        article_id: str,
        image: ArticleImage,
    ) -> bool:
        """Save image metadata to database."""
        if not self.supabase_url or not self.supabase_key:
            logger.warning("Supabase not configured - skipping image record save")
            return False

        try:
            response = requests.post(
                f"{self.supabase_url}/rest/v1/article_images",
                headers={
                    "apikey": self.supabase_key,
                    "Authorization": f"Bearer {self.supabase_key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                json={
                    "article_id": article_id,
                    "role": image.role,
                    "r2_uri": image.url,
                    "r2_hash": image.hash,
                    "source_url": image.source_url,
                    "source_id": image.source_id,
                    "photographer": image.photographer,
                    "photographer_url": image.photographer_url,
                    "alt_text": image.alt_text,
                    "width": image.width,
                    "height": image.height,
                    "file_size_bytes": image.file_size_bytes,
                    "format": image.format,
                },
                timeout=10,
            )

            if response.status_code in (200, 201):
                logger.info(f"Saved image record for article {article_id}")
                return True
            else:
                logger.error(f"Failed to save image record: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Failed to save image record: {e}")
            return False

    def source_hero_image(
        self,
        title: str,
        topic: str,
        site_slug: str,
        article_slug: str,
        article_id: Optional[str] = None,
        orientation: str = "landscape",
    ) -> Optional[ArticleImage]:
        """
        Source, optimize, and upload a hero image for an article.

        Args:
            title: Article title
            topic: Article topic/keywords
            site_slug: Site identifier
            article_slug: Article slug
            article_id: Article UUID (for database record)
            orientation: Image orientation ('landscape', 'portrait', 'squarish')

        Returns:
            ArticleImage with complete metadata, or None on failure
        """
        logger.info(f"Sourcing hero image for: {title[:50]}...")

        # Extract search keywords
        keywords = self._extract_keywords(title, topic)
        logger.debug(f"Search keywords: {keywords}")

        # Search and download from Unsplash
        image_bytes, photo = self.unsplash.search_and_download(
            query=keywords,
            orientation=orientation,
        )

        if not image_bytes:
            logger.error("Failed to source image from Unsplash")
            return None

        # Optimize
        try:
            optimized = self.optimizer.optimize(image_bytes)
        except Exception as e:
            logger.error(f"Failed to optimize image: {e}")
            return None

        # Check for duplicate (deduplication by hash)
        existing_url = self._check_hash_exists(optimized.hash)
        if existing_url:
            logger.info(f"Image already exists at: {existing_url}")
            # Still return the image metadata but skip upload
            return ArticleImage(
                url=existing_url,
                alt_text=self._generate_alt_text(title, photo),
                width=optimized.width,
                height=optimized.height,
                format=optimized.format,
                file_size_bytes=optimized.optimized_size,
                hash=optimized.hash,
                role="hero",
                source_url=photo.url if photo else None,
                source_id=photo.id if photo else None,
                photographer=photo.photographer if photo else None,
                photographer_url=photo.photographer_url if photo else None,
                attribution=photo.attribution if photo else None,
            )

        # Upload to R2
        try:
            format_ext = "jpg" if optimized.format == "jpeg" else optimized.format
            url = self.storage.upload(
                image_bytes=optimized.data,
                site_slug=site_slug,
                article_slug=article_slug,
                image_slug="hero",
                format=format_ext,
            )
        except Exception as e:
            logger.error(f"Failed to upload image: {e}")
            return None

        # Build result
        image = ArticleImage(
            url=url,
            alt_text=self._generate_alt_text(title, photo),
            width=optimized.width,
            height=optimized.height,
            format=optimized.format,
            file_size_bytes=optimized.optimized_size,
            hash=optimized.hash,
            role="hero",
            source_url=photo.url if photo else None,
            source_id=photo.id if photo else None,
            photographer=photo.photographer if photo else None,
            photographer_url=photo.photographer_url if photo else None,
            attribution=photo.attribution if photo else None,
        )

        # Save to database if article_id provided
        if article_id:
            self._save_image_record(article_id, image)

        logger.info(f"Hero image ready: {url}")
        return image


# CLI for testing
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="Image Pipeline CLI")
    parser.add_argument("--title", required=True, help="Article title")
    parser.add_argument("--topic", default="", help="Article topic")
    parser.add_argument("--site", required=True, help="Site slug")
    parser.add_argument("--article", required=True, help="Article slug")
    args = parser.parse_args()

    pipeline = ImagePipeline()
    image = pipeline.source_hero_image(
        title=args.title,
        topic=args.topic or args.title,
        site_slug=args.site,
        article_slug=args.article,
    )

    if image:
        print(f"\nHero image sourced successfully:")
        print(f"  URL: {image.url}")
        print(f"  Alt: {image.alt_text}")
        print(f"  Size: {image.width}x{image.height}")
        print(f"  File: {image.file_size_bytes // 1024}KB")
        print(f"  Hash: {image.hash[:16]}...")
        if image.attribution:
            print(f"  Attribution: {image.attribution}")
    else:
        print("Failed to source hero image")
