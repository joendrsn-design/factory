"""
R2 Storage — Upload and manage images in Cloudflare R2.

Uses S3-compatible API via boto3.
Path structure: articles/{site_slug}/{article_slug}/{image_slug}.{ext}
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("article_factory.media.storage")

# R2 configuration (all values must come from environment)
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "site-empire-assets")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "https://pub-8b0befc5ff9b447db8eddf980d003500.r2.dev")


class R2Storage:
    """
    Cloudflare R2 storage client for article images.

    Usage:
        storage = R2Storage()
        url = storage.upload(
            image_bytes,
            site_slug="lamphill",
            article_slug="nad-boosters",
            image_slug="hero",
        )
        # Returns: https://pub-xxx.r2.dev/articles/lamphill/nad-boosters/hero.jpg
    """

    def __init__(self):
        if not R2_ACCOUNT_ID or not R2_ACCESS_KEY_ID or not R2_SECRET_ACCESS_KEY:
            raise ValueError(
                "R2 credentials not configured. Set R2_ACCOUNT_ID, "
                "R2_ACCESS_KEY_ID, and R2_SECRET_ACCESS_KEY environment variables."
            )

        self.bucket_name = R2_BUCKET_NAME
        self.public_url = R2_PUBLIC_URL.rstrip("/")

        # Initialize S3 client for R2
        self.client = boto3.client(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
            ),
            region_name="auto",
        )

        logger.debug(f"R2Storage initialized for bucket: {self.bucket_name}")

    def _build_key(
        self,
        site_slug: str,
        article_slug: str,
        image_slug: str,
        format: str = "jpg",
    ) -> str:
        """Build R2 object key from components."""
        # Sanitize slugs
        site_slug = site_slug.lower().replace(" ", "-")
        article_slug = article_slug.lower().replace(" ", "-")
        image_slug = image_slug.lower().replace(" ", "-")

        return f"articles/{site_slug}/{article_slug}/{image_slug}.{format}"

    def _get_content_type(self, format: str) -> str:
        """Get MIME type for format."""
        types = {
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "webp": "image/webp",
            "png": "image/png",
        }
        return types.get(format.lower(), "image/jpeg")

    def upload(
        self,
        image_bytes: bytes,
        site_slug: str,
        article_slug: str,
        image_slug: str = "hero",
        format: str = "jpg",
    ) -> str:
        """
        Upload image to R2.

        Args:
            image_bytes: Optimized image data
            site_slug: Site identifier (e.g., "lamphill")
            article_slug: Article slug (e.g., "nad-boosters")
            image_slug: Image identifier (e.g., "hero", "inline-1")
            format: File format extension

        Returns:
            Public URL of uploaded image
        """
        key = self._build_key(site_slug, article_slug, image_slug, format)
        content_type = self._get_content_type(format)

        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=image_bytes,
                ContentType=content_type,
                CacheControl="public, max-age=31536000",  # 1 year cache
            )

            public_url = f"{self.public_url}/{key}"
            logger.info(f"Uploaded: {key} ({len(image_bytes)} bytes)")
            return public_url

        except Exception as e:
            logger.error(f"Failed to upload {key}: {e}")
            raise

    def exists(self, key: str) -> bool:
        """Check if an object exists in R2."""
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except self.client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def delete(self, key: str) -> bool:
        """Delete an object from R2."""
        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"Deleted: {key}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete {key}: {e}")
            return False

    def delete_article_images(self, site_slug: str, article_slug: str) -> int:
        """
        Delete all images for an article.

        Args:
            site_slug: Site identifier
            article_slug: Article slug

        Returns:
            Number of objects deleted
        """
        prefix = f"articles/{site_slug}/{article_slug}/"

        try:
            # List objects with prefix
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
            )

            if "Contents" not in response:
                return 0

            # Delete each object
            deleted = 0
            for obj in response["Contents"]:
                if self.delete(obj["Key"]):
                    deleted += 1

            logger.info(f"Deleted {deleted} images for {site_slug}/{article_slug}")
            return deleted

        except Exception as e:
            logger.error(f"Failed to delete article images: {e}")
            return 0

    def list_article_images(self, site_slug: str, article_slug: str) -> list[str]:
        """List all image URLs for an article."""
        prefix = f"articles/{site_slug}/{article_slug}/"

        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
            )

            if "Contents" not in response:
                return []

            return [f"{self.public_url}/{obj['Key']}" for obj in response["Contents"]]

        except Exception as e:
            logger.error(f"Failed to list article images: {e}")
            return []

    def get_url(
        self,
        site_slug: str,
        article_slug: str,
        image_slug: str = "hero",
        format: str = "jpg",
    ) -> str:
        """Get the public URL for an image (without checking existence)."""
        key = self._build_key(site_slug, article_slug, image_slug, format)
        return f"{self.public_url}/{key}"


# CLI for testing
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="R2 Storage CLI")
    parser.add_argument("--upload", help="Path to image file to upload")
    parser.add_argument("--site", default="test-site", help="Site slug")
    parser.add_argument("--article", default="test-article", help="Article slug")
    parser.add_argument("--image", default="hero", help="Image slug")
    parser.add_argument("--list", action="store_true", help="List images for article")
    parser.add_argument("--delete", action="store_true", help="Delete images for article")
    args = parser.parse_args()

    storage = R2Storage()

    if args.upload:
        with open(args.upload, "rb") as f:
            image_bytes = f.read()
        format = args.upload.rsplit(".", 1)[-1]
        url = storage.upload(
            image_bytes,
            site_slug=args.site,
            article_slug=args.article,
            image_slug=args.image,
            format=format,
        )
        print(f"Uploaded to: {url}")

    if args.list:
        urls = storage.list_article_images(args.site, args.article)
        print(f"Images for {args.site}/{args.article}:")
        for url in urls:
            print(f"  {url}")

    if args.delete:
        count = storage.delete_article_images(args.site, args.article)
        print(f"Deleted {count} images")
