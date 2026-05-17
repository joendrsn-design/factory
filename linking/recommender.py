"""
Link Recommender — Find semantically similar articles for internal linking.

Uses pgvector cosine similarity to find related published articles
and generates varied anchor text suggestions.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Optional

import requests
from dotenv import load_dotenv

from .embeddings import EmbeddingService

load_dotenv()

logger = logging.getLogger("article_factory.linking.recommender")

# Similarity threshold (cosine distance, lower = more similar)
# 0.6 means vectors are ~40% similar (relaxed for sparse datasets)
# Tighten to 0.4 once content volume increases
SIMILARITY_THRESHOLD = 0.6

# Default number of candidates to return
DEFAULT_LIMIT = 5


@dataclass
class LinkCandidate:
    """A candidate article for internal linking."""
    article_id: str
    slug: str
    title: str
    url: str
    similarity: float
    is_hub: bool = False
    anchors: dict = field(default_factory=dict)

    def __str__(self):
        return f"{self.title} ({self.slug}) - {self.similarity:.2f}"


class LinkRecommender:
    """
    Recommend internal link targets based on semantic similarity.

    Usage:
        recommender = LinkRecommender()
        candidates = recommender.get_candidates(
            site_id="lamphill",
            topic="magnesium supplements",
            title="Best Magnesium for Sleep",
            limit=5,
        )

        for c in candidates:
            print(f"Link to: {c.title}")
            print(f"  Anchors: {c.anchors}")
    """

    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        self.embedding_service = EmbeddingService()
        self._site_id_cache = {}  # Maps slug -> UUID

        if not self.supabase_url or not self.supabase_key:
            logger.warning("Supabase credentials not configured")

    def _resolve_site_id(self, site_id: str) -> str:
        """
        Resolve a site_id to UUID if needed.

        Factory configs use slug-based site_ids (e.g., 'lamphill'),
        but the content table uses UUIDs. This method handles both.
        """
        # Already a UUID
        if len(site_id) == 36 and site_id.count('-') == 4:
            return site_id

        # Check cache
        if site_id in self._site_id_cache:
            return self._site_id_cache[site_id]

        # Look up by domain pattern (site_id usually matches domain prefix)
        try:
            response = requests.get(
                f"{self.supabase_url}/rest/v1/sites",
                headers={
                    "apikey": self.supabase_key,
                    "Authorization": f"Bearer {self.supabase_key}",
                },
                params={
                    "select": "id,domain",
                    "or": f"(domain.ilike.{site_id}%,domain.eq.{site_id}.org,domain.eq.{site_id}.com)",
                    "limit": 1,
                },
                timeout=10,
            )

            if response.status_code == 200 and response.json():
                uuid = response.json()[0]["id"]
                self._site_id_cache[site_id] = uuid
                logger.debug(f"Resolved site_id '{site_id}' to UUID '{uuid}'")
                return uuid
        except Exception as e:
            logger.warning(f"Failed to resolve site_id '{site_id}': {e}")

        # Fallback: return as-is (will likely fail query but won't crash)
        return site_id

    def _rpc(self, function_name: str, params: dict) -> list[dict]:
        """Call a Supabase RPC function."""
        response = requests.post(
            f"{self.supabase_url}/rest/v1/rpc/{function_name}",
            headers={
                "apikey": self.supabase_key,
                "Authorization": f"Bearer {self.supabase_key}",
                "Content-Type": "application/json",
            },
            json=params,
            timeout=30,
        )

        if response.status_code != 200:
            logger.error(f"RPC {function_name} failed: {response.status_code} - {response.text}")
            return []

        return response.json()

    def _query(self, endpoint: str) -> list[dict]:
        """Execute a Supabase REST query."""
        response = requests.get(
            f"{self.supabase_url}/rest/v1/{endpoint}",
            headers={
                "apikey": self.supabase_key,
                "Authorization": f"Bearer {self.supabase_key}",
            },
            timeout=30,
        )

        if response.status_code != 200:
            logger.error(f"Query failed: {response.status_code} - {response.text}")
            return []

        return response.json()

    def get_candidates(
        self,
        site_id: str,
        topic: str,
        title: str,
        exclude_slugs: list[str] = None,
        limit: int = DEFAULT_LIMIT,
    ) -> list[LinkCandidate]:
        """
        Find semantically similar published articles for internal linking.

        Args:
            site_id: The site to search within.
            topic: The topic of the new article (used for embedding).
            title: The title of the new article.
            exclude_slugs: Slugs to exclude (e.g., the article itself).
            limit: Maximum number of candidates to return.

        Returns:
            List of LinkCandidate objects, sorted by similarity.
        """
        exclude_slugs = exclude_slugs or []

        # Resolve site_id to UUID if needed
        site_id = self._resolve_site_id(site_id)

        # Generate embedding for the new article's topic/title
        query_text = f"{title}\n\n{topic}"
        query_embedding = self.embedding_service.embed_text(query_text)

        if query_embedding is None:
            logger.warning("Could not generate query embedding, falling back to hub pages only")
            return self.get_hub_pages(site_id)

        # Query for similar articles using pgvector
        # We use a raw SQL approach via RPC since REST API doesn't support vector ops
        candidates = self._find_similar_articles(
            site_id=site_id,
            embedding=query_embedding,
            exclude_slugs=exclude_slugs,
            limit=limit + 5,  # Get extra to filter later
        )

        # Filter by similarity threshold and limit
        results = []
        for row in candidates:
            # pgvector returns distance (lower = more similar)
            # Convert to similarity score (higher = more similar)
            distance = row.get("distance", 1.0)
            if distance > SIMILARITY_THRESHOLD:
                continue

            similarity = 1.0 - distance

            candidate = LinkCandidate(
                article_id=row.get("id", ""),
                slug=row.get("slug", ""),
                title=row.get("title", ""),
                url=f"/{row.get('slug', '')}",
                similarity=similarity,
                is_hub=row.get("is_hub", False),
            )

            # Generate anchor text variants
            candidate.anchors = self._generate_anchors(
                title=candidate.title,
                slug=candidate.slug,
            )

            results.append(candidate)

            if len(results) >= limit:
                break

        # Ensure at least one hub page if available
        if results and not any(c.is_hub for c in results):
            hub_pages = self.get_hub_pages(site_id)
            if hub_pages:
                # Add the first hub page if not already in results
                hub = hub_pages[0]
                if hub.slug not in [c.slug for c in results]:
                    results.append(hub)

        logger.info(f"Found {len(results)} link candidates for '{title[:50]}'")
        return results

    def _find_similar_articles(
        self,
        site_id: str,
        embedding: list[float],
        exclude_slugs: list[str],
        limit: int,
    ) -> list[dict]:
        """
        Find similar articles using pgvector cosine distance.

        This uses a direct SQL query since the REST API doesn't support
        vector operations. We call it via Supabase's PostgREST.
        """
        # Format embedding for pgvector
        embedding_str = f"[{','.join(str(x) for x in embedding)}]"

        # Build exclude filter
        exclude_filter = ""
        if exclude_slugs:
            slugs_str = ",".join(f"'{s}'" for s in exclude_slugs)
            exclude_filter = f"AND slug NOT IN ({slugs_str})"

        # We need to use RPC for vector queries
        # First, let's try a simpler approach using the REST API with a function

        # For now, fall back to fetching all published articles with embeddings
        # and computing similarity in Python (less efficient but works without RPC)
        try:
            # Get published articles with embeddings for this site
            response = requests.get(
                f"{self.supabase_url}/rest/v1/content",
                headers={
                    "apikey": self.supabase_key,
                    "Authorization": f"Bearer {self.supabase_key}",
                },
                params={
                    "select": "id,slug,title,is_hub,embedding",
                    "site_id": f"eq.{site_id}",
                    "status": "eq.published",
                    "embedding": "not.is.null",
                    "limit": 100,  # Reasonable limit for similarity search
                },
                timeout=30,
            )

            if response.status_code != 200:
                logger.error(f"Failed to fetch articles: {response.text}")
                return []

            articles = response.json()

            # Compute cosine distance in Python
            results = []
            for article in articles:
                if article.get("slug") in exclude_slugs:
                    continue

                article_embedding = article.get("embedding")
                if not article_embedding:
                    continue

                # Parse embedding string if needed
                if isinstance(article_embedding, str):
                    article_embedding = [float(x) for x in article_embedding.strip("[]").split(",")]

                # Compute cosine distance
                distance = self._cosine_distance(embedding, article_embedding)

                results.append({
                    "id": article["id"],
                    "slug": article["slug"],
                    "title": article["title"],
                    "is_hub": article.get("is_hub", False),
                    "distance": distance,
                })

            # Sort by distance (ascending)
            results.sort(key=lambda x: x["distance"])
            return results[:limit]

        except Exception as e:
            logger.error(f"Failed to find similar articles: {e}")
            return []

    def _cosine_distance(self, a: list[float], b: list[float]) -> float:
        """Compute cosine distance between two vectors."""
        if len(a) != len(b):
            return 1.0

        dot_product = sum(x * y for x, y in zip(a, b))
        norm_a = sum(x * x for x in a) ** 0.5
        norm_b = sum(x * x for x in b) ** 0.5

        if norm_a == 0 or norm_b == 0:
            return 1.0

        cosine_similarity = dot_product / (norm_a * norm_b)
        return 1.0 - cosine_similarity

    def get_hub_pages(self, site_id: str) -> list[LinkCandidate]:
        """
        Get hub/pillar pages for a site.

        Hub pages should be linked from most articles.

        Args:
            site_id: The site to get hubs for.

        Returns:
            List of hub page LinkCandidates.
        """
        # Resolve site_id to UUID if needed
        site_id = self._resolve_site_id(site_id)

        try:
            articles = self._query(
                f"content?select=id,slug,title&site_id=eq.{site_id}&is_hub=eq.true&status=eq.published"
            )

            results = []
            for article in articles:
                candidate = LinkCandidate(
                    article_id=article.get("id", ""),
                    slug=article.get("slug", ""),
                    title=article.get("title", ""),
                    url=f"/{article.get('slug', '')}",
                    similarity=1.0,  # Hub pages always relevant
                    is_hub=True,
                )
                candidate.anchors = self._generate_anchors(
                    title=candidate.title,
                    slug=candidate.slug,
                )
                results.append(candidate)

            return results

        except Exception as e:
            logger.error(f"Failed to get hub pages: {e}")
            return []

    def _generate_anchors(self, title: str, slug: str) -> dict[str, str]:
        """
        Generate varied anchor text for a link target.

        Returns:
            Dict with anchor types: exact, partial, generic, contextual
        """
        # Exact: full title
        exact = title

        # Partial: first part of title or key phrase
        words = title.split()
        if len(words) > 4:
            partial = " ".join(words[:4])
        else:
            partial = title

        # Generic: common link phrases
        generic_options = [
            "learn more",
            "read more",
            "this guide",
            "our guide",
            "this article",
        ]
        # Pick based on slug hash for consistency
        generic = generic_options[hash(slug) % len(generic_options)]

        # Contextual: uses slug words
        slug_words = slug.replace("-", " ")
        contextual = f"more about {slug_words}"

        return {
            "exact": exact,
            "partial": partial,
            "generic": generic,
            "contextual": contextual,
        }

    def format_for_prompt(self, candidates: list[LinkCandidate]) -> str:
        """
        Format link candidates for inclusion in an LLM prompt.

        Args:
            candidates: List of LinkCandidate objects.

        Returns:
            Formatted string for prompt injection.
        """
        if not candidates:
            return "No internal link candidates available."

        lines = []
        for i, c in enumerate(candidates, 1):
            hub_marker = " [HUB]" if c.is_hub else ""
            lines.append(f"{i}. {c.title}{hub_marker}")
            lines.append(f"   URL: {c.url}")
            lines.append(f"   Anchors: exact=\"{c.anchors.get('exact', '')}\", "
                        f"partial=\"{c.anchors.get('partial', '')}\", "
                        f"generic=\"{c.anchors.get('generic', '')}\"")

        return "\n".join(lines)


# CLI for testing
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="Link Recommender CLI")
    parser.add_argument("--site", required=True, help="Site ID to search")
    parser.add_argument("--topic", required=True, help="Topic to find links for")
    parser.add_argument("--title", default="", help="Article title")
    parser.add_argument("--limit", type=int, default=5, help="Number of candidates")
    args = parser.parse_args()

    recommender = LinkRecommender()
    candidates = recommender.get_candidates(
        site_id=args.site,
        topic=args.topic,
        title=args.title or args.topic,
        limit=args.limit,
    )

    print(f"\nFound {len(candidates)} link candidates:\n")
    print(recommender.format_for_prompt(candidates))
