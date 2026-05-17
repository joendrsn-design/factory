"""
Media Pipeline module for Site Empire Article Factory.

Handles image sourcing, optimization, and storage for articles.
"""

from .unsplash import UnsplashClient, UnsplashPhoto
from .optimizer import ImageOptimizer, OptimizedImage
from .storage import R2Storage
from .pipeline import ImagePipeline, ArticleImage

__all__ = [
    "UnsplashClient",
    "UnsplashPhoto",
    "ImageOptimizer",
    "OptimizedImage",
    "R2Storage",
    "ImagePipeline",
    "ArticleImage",
]
