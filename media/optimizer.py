"""
Image Optimizer — Resize and compress images for web delivery.

Uses Pillow to optimize images before R2 upload:
- Resize to max 1600px width
- Compress to JPEG-85 or WebP-80
- Target file size <= 200KB
"""
from __future__ import annotations

import hashlib
import io
import logging
from dataclasses import dataclass
from typing import Optional

from PIL import Image

logger = logging.getLogger("article_factory.media.optimizer")

# Optimization defaults
DEFAULT_MAX_WIDTH = 1600
DEFAULT_QUALITY = 85
DEFAULT_MAX_SIZE_KB = 200
MIN_QUALITY = 60  # Don't go below this


@dataclass
class OptimizedImage:
    """Result of image optimization."""
    data: bytes
    width: int
    height: int
    format: str  # 'jpeg' or 'webp'
    quality: int
    original_size: int
    optimized_size: int
    hash: str  # SHA-256

    @property
    def size_reduction_pct(self) -> float:
        """Percentage size reduction achieved."""
        if self.original_size == 0:
            return 0
        return (1 - self.optimized_size / self.original_size) * 100


class ImageOptimizer:
    """
    Optimize images for web delivery.

    Usage:
        optimizer = ImageOptimizer()
        result = optimizer.optimize(image_bytes)
        print(f"Optimized: {result.width}x{result.height}, {result.optimized_size} bytes")
    """

    def __init__(
        self,
        max_width: int = DEFAULT_MAX_WIDTH,
        default_quality: int = DEFAULT_QUALITY,
        max_size_kb: int = DEFAULT_MAX_SIZE_KB,
        default_format: str = "jpeg",
    ):
        self.max_width = max_width
        self.default_quality = default_quality
        self.max_size_kb = max_size_kb
        self.default_format = default_format

    def optimize(
        self,
        image_bytes: bytes,
        max_width: Optional[int] = None,
        quality: Optional[int] = None,
        format: Optional[str] = None,
        max_size_kb: Optional[int] = None,
    ) -> OptimizedImage:
        """
        Optimize an image for web delivery.

        Args:
            image_bytes: Raw image data
            max_width: Maximum width in pixels (maintains aspect ratio)
            quality: JPEG/WebP quality (1-100)
            format: Output format ('jpeg' or 'webp')
            max_size_kb: Target maximum file size in KB

        Returns:
            OptimizedImage with optimized data and metadata
        """
        max_width = max_width or self.max_width
        quality = quality or self.default_quality
        format = format or self.default_format
        max_size_kb = max_size_kb or self.max_size_kb

        original_size = len(image_bytes)

        # Load image
        try:
            img = Image.open(io.BytesIO(image_bytes))
        except Exception as e:
            raise ValueError(f"Failed to decode image: {e}")

        # Convert to RGB if needed (for JPEG compatibility)
        if img.mode in ("RGBA", "P"):
            # Create white background for transparency
            background = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "P":
                img = img.convert("RGBA")
            background.paste(img, mask=img.split()[-1] if img.mode == "RGBA" else None)
            img = background
        elif img.mode != "RGB":
            img = img.convert("RGB")

        # Resize if too wide
        if img.width > max_width:
            ratio = max_width / img.width
            new_height = int(img.height * ratio)
            img = img.resize((max_width, new_height), Image.Resampling.LANCZOS)
            logger.debug(f"Resized from {img.width}x{img.height} to {max_width}x{new_height}")

        # Compress with iterative quality reduction if needed
        optimized_bytes = self._compress(img, format, quality, max_size_kb)
        final_quality = quality

        # If still too large, reduce quality iteratively
        current_quality = quality
        while len(optimized_bytes) > max_size_kb * 1024 and current_quality > MIN_QUALITY:
            current_quality -= 5
            optimized_bytes = self._compress(img, format, current_quality, max_size_kb)
            final_quality = current_quality
            logger.debug(f"Reduced quality to {current_quality}, size: {len(optimized_bytes)} bytes")

        # Compute hash
        image_hash = self.compute_hash(optimized_bytes)

        result = OptimizedImage(
            data=optimized_bytes,
            width=img.width,
            height=img.height,
            format=format,
            quality=final_quality,
            original_size=original_size,
            optimized_size=len(optimized_bytes),
            hash=image_hash,
        )

        logger.info(
            f"Optimized: {result.width}x{result.height}, "
            f"{result.optimized_size // 1024}KB ({result.size_reduction_pct:.0f}% reduction), "
            f"quality={result.quality}"
        )

        return result

    def _compress(
        self,
        img: Image.Image,
        format: str,
        quality: int,
        max_size_kb: int,
    ) -> bytes:
        """Compress image to bytes."""
        buffer = io.BytesIO()

        if format == "webp":
            img.save(buffer, format="WEBP", quality=quality, method=6)
        else:
            # JPEG with optimization
            img.save(
                buffer,
                format="JPEG",
                quality=quality,
                optimize=True,
                progressive=True,
            )

        return buffer.getvalue()

    def compute_hash(self, image_bytes: bytes) -> str:
        """Compute SHA-256 hash of image data."""
        return hashlib.sha256(image_bytes).hexdigest()

    def get_dimensions(self, image_bytes: bytes) -> tuple[int, int]:
        """Get width and height of image without full decode."""
        try:
            img = Image.open(io.BytesIO(image_bytes))
            return img.width, img.height
        except Exception:
            return 0, 0


# CLI for testing
if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="Image Optimizer CLI")
    parser.add_argument("input", help="Input image file")
    parser.add_argument("--output", help="Output file (default: input_optimized.jpg)")
    parser.add_argument("--max-width", type=int, default=1600, help="Max width")
    parser.add_argument("--quality", type=int, default=85, help="Quality (1-100)")
    parser.add_argument("--format", choices=["jpeg", "webp"], default="jpeg")
    parser.add_argument("--max-size", type=int, default=200, help="Max size in KB")
    args = parser.parse_args()

    with open(args.input, "rb") as f:
        image_bytes = f.read()

    optimizer = ImageOptimizer()
    try:
        result = optimizer.optimize(
            image_bytes,
            max_width=args.max_width,
            quality=args.quality,
            format=args.format,
            max_size_kb=args.max_size,
        )
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    output_path = args.output or args.input.rsplit(".", 1)[0] + f"_optimized.{result.format}"
    with open(output_path, "wb") as f:
        f.write(result.data)

    print(f"\nOptimized image saved to: {output_path}")
    print(f"  Dimensions: {result.width}x{result.height}")
    print(f"  Format: {result.format.upper()}")
    print(f"  Quality: {result.quality}")
    print(f"  Original size: {result.original_size // 1024}KB")
    print(f"  Optimized size: {result.optimized_size // 1024}KB")
    print(f"  Reduction: {result.size_reduction_pct:.1f}%")
    print(f"  Hash: {result.hash[:16]}...")
