"""
Embedding Service — Generate and store embeddings for articles.

Uses OpenAI text-embedding-3-small (1536 dimensions) for semantic search.
Embeddings are generated on article insert and stored in the content table.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("article_factory.linking.embeddings")

# OpenAI embedding model
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSIONS = 1536

# OpenAI API endpoint
OPENAI_API_URL = "https://api.openai.com/v1/embeddings"


class EmbeddingService:
    """
    Generate embeddings using OpenAI API.

    Usage:
        service = EmbeddingService()
        embedding = service.embed_text("Article title and content here")

        # Or embed and store directly
        service.embed_and_store(article_id, title, body)
    """

    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not self.api_key:
            logger.warning("OPENAI_API_KEY not set - embeddings will be unavailable")

    def embed_text(self, text: str) -> Optional[list[float]]:
        """
        Generate embedding for text using OpenAI API.

        Args:
            text: The text to embed (title + body, truncated if needed).

        Returns:
            List of 1536 floats, or None on error.
        """
        if not self.api_key:
            logger.warning("Cannot generate embedding - OPENAI_API_KEY not set")
            return None

        # Truncate to ~8000 tokens (~32000 chars) to stay within limits
        # text-embedding-3-small has 8191 token limit
        max_chars = 30000
        if len(text) > max_chars:
            text = text[:max_chars]
            logger.debug(f"Truncated text to {max_chars} chars for embedding")

        try:
            response = requests.post(
                OPENAI_API_URL,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": EMBEDDING_MODEL,
                    "input": text,
                    "dimensions": EMBEDDING_DIMENSIONS,
                },
                timeout=30,
            )

            if response.status_code != 200:
                logger.error(f"OpenAI API error: {response.status_code} - {response.text}")
                return None

            data = response.json()
            embedding = data["data"][0]["embedding"]

            logger.debug(f"Generated embedding with {len(embedding)} dimensions")
            return embedding

        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            return None

    def embed_article(self, title: str, body: str) -> Optional[list[float]]:
        """
        Generate embedding for an article.

        Combines title and body for embedding, with title weighted
        by appearing first.

        Args:
            title: Article title.
            body: Article body (markdown).

        Returns:
            Embedding vector or None on error.
        """
        # Combine title and body, title first for emphasis
        text = f"{title}\n\n{body}"
        return self.embed_text(text)

    def store_embedding(self, article_id: str, embedding: list[float]) -> bool:
        """
        Store embedding in the content table.

        Args:
            article_id: The article UUID.
            embedding: The embedding vector (1536 floats).

        Returns:
            True if stored successfully.
        """
        if not self.supabase_url or not self.supabase_key:
            logger.error("Supabase credentials not configured")
            return False

        try:
            # Format embedding as pgvector string
            embedding_str = f"[{','.join(str(x) for x in embedding)}]"

            response = requests.patch(
                f"{self.supabase_url}/rest/v1/content?id=eq.{article_id}",
                headers={
                    "apikey": self.supabase_key,
                    "Authorization": f"Bearer {self.supabase_key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                json={"embedding": embedding_str},
                timeout=30,
            )

            if response.status_code in (200, 204):
                logger.info(f"Stored embedding for article {article_id}")
                return True
            else:
                logger.error(f"Failed to store embedding: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Failed to store embedding: {e}")
            return False

    def embed_and_store(self, article_id: str, title: str, body: str) -> bool:
        """
        Generate embedding for an article and store it.

        This is the main entry point for embedding articles on insert.

        Args:
            article_id: The article UUID (from Site Empire API response).
            title: Article title.
            body: Article body.

        Returns:
            True if embedding was generated and stored successfully.
        """
        embedding = self.embed_article(title, body)
        if embedding is None:
            return False

        return self.store_embedding(article_id, embedding)


# CLI for testing
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="Embedding Service CLI")
    parser.add_argument("--test", action="store_true", help="Test embedding generation")
    parser.add_argument("--text", help="Text to embed")
    args = parser.parse_args()

    service = EmbeddingService()

    if args.test or args.text:
        text = args.text or "This is a test article about magnesium supplements and their health benefits."
        print(f"Embedding text: {text[:100]}...")

        embedding = service.embed_text(text)
        if embedding:
            print(f"Generated embedding with {len(embedding)} dimensions")
            print(f"First 5 values: {embedding[:5]}")
        else:
            print("Failed to generate embedding")
