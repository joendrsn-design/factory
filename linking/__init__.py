"""
Internal Linking module for Site Empire Article Factory.

Provides semantic search for link candidates using OpenAI embeddings
and pgvector similarity search.
"""

from .embeddings import EmbeddingService
from .recommender import LinkRecommender, LinkCandidate

__all__ = [
    "EmbeddingService",
    "LinkRecommender",
    "LinkCandidate",
]
