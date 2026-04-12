"""
============================================================
ARTICLE FACTORY — TOPIC GENERATOR (Module 0)
============================================================
Generates topic queues for each site based on:
  - Site config (niche, article types, keyword clusters)
  - Publishing history (what's already been written)
  - Seasonal/trending awareness
  - Research vault (what research already exists)

Model: Haiku (cheap, structural, high volume)
Input: Site configs + publishing history
Output folder: pipeline/topics/

This is the HEAD of the pipeline. Everything starts here.

Usage:
    python topic_generator.py submit --site lamphill --count 10
    python topic_generator.py collect --output pipeline/topics
    python topic_generator.py run --site lamphill --count 5 --output pipeline/topics

    # Generate for ALL sites based on their configured frequencies
    python topic_generator.py submit --all
    python topic_generator.py run --all --output pipeline/topics
============================================================
"""

import os
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from site_loader import SiteLoader, SiteContext
from artifacts import (
    topic_metadata, new_run_id, new_article_id,
    save_artifact, load_artifacts_from_dir,
    save_batch_manifest, find_latest_batch_manifest,
)

logger = logging.getLogger("article_factory.topic_generator")


# ── Publishing History ──────────────────────────────────────

class PublishingHistory:
    """
    Tracks what topics have already been published or are in the pipeline.
    Prevents duplicates and guides topic diversity.
    """

    def __init__(self, pipeline_dirs: list[str] = None, published_dir: str = ""):
        """
        Args:
            pipeline_dirs: folders to scan for in-flight topics
            published_dir: folder of already-published articles
        """
        self.pipeline_dirs = pipeline_dirs or [
            "pipeline/topics",
            "pipeline/research",
            "pipeline/plans",
            "pipeline/articles",
            "pipeline/qa",
        ]
        self.published_dir = published_dir
        self._cache = None

    def get_existing_topics(self, site_id: str = "") -> list[str]:
        """Get all topics already in pipeline or published for a site."""
        if self._cache is None:
            self._cache = self._scan()

        topics = self._cache.get(site_id, []) if site_id else []
        # Also include cross-site topics for dedup
        topics += self._cache.get("_all", [])
        return list(set(t.lower().strip() for t in topics))

    def _scan(self) -> dict:
        """Scan all pipeline directories for existing topics."""
        topics_by_site = {"_all": []}

        for directory in self.pipeline_dirs:
            artifacts = load_artifacts_from_dir(directory)
            for meta, body, fpath in artifacts:
                topic = meta.get("topic", "")
                site_id = meta.get("site_id", "")
                if topic:
                    topics_by_site.setdefault(site_id, []).append(topic)
                    topics_by_site["_all"].append(topic)

        # Scan published dir
        if self.published_dir:
            artifacts = load_artifacts_from_dir(self.published_dir)
            for meta, body, fpath in artifacts:
                topic = meta.get("topic", "")
                site_id = meta.get("site_id", "")
                if topic:
                    topics_by_site.setdefault(site_id, []).append(topic)
                    topics_by_site["_all"].append(topic)

        return topics_by_site


# ── Topic Generator ─────────────────────────────────────────

class TopicGenerator:

    model = "claude-haiku-4-5-20251001"
    default_max_tokens = 2048

    def __init__(self, config_dir: str = "config/sites", published_dir: str = ""):
        self.loader = SiteLoader(config_dir=config_dir)
        self.history = PublishingHistory(published_dir=published_dir)
        logger.info("[topic_gen] Initialized")

    def generate_for_site(
        self,
        site_id: str,
        count: int = 5,
        article_type_filter: str = "",
        run_id: str = "",
    ) -> list[tuple[dict, str]]:
        """
        Generate topic artifacts for a single site.
        Returns list of (metadata, body) tuples.
        """
        if not run_id:
            run_id = new_run_id()

        site_context = self.loader.load(site_id)
        existing = self.history.get_existing_topics(site_id)

        # Determine which article types to generate for
        article_types = site_context.get_enabled_article_types()
        if article_type_filter:
            article_types = [at for at in article_types if at["type_id"] == article_type_filter]

        if not article_types:
            logger.warning(f"[topic_gen] No article types for {site_id}")
            return []

        # Distribute count across article types by frequency
        type_counts = self._distribute_by_frequency(article_types, count)

        all_topics = []

        for article_type, type_count in type_counts:
            if type_count == 0:
                continue

            topics = self._generate_topics(
                site_context, article_type, type_count, existing, run_id
            )
            all_topics.extend(topics)

            # Add generated topics to existing list to prevent within-batch dupes
            for meta, body in topics:
                existing.append(meta.get("topic", "").lower())

        logger.info(f"[topic_gen] Generated {len(all_topics)} topics for {site_id}")
        return all_topics

    def generate_for_all(
        self,
        count_per_site: int = 0,
        run_id: str = "",
    ) -> dict[str, list[tuple[dict, str]]]:
        """
        Generate topics for ALL configured sites.
        If count_per_site=0, uses each site's configured frequency.
        Returns dict of site_id → list of (metadata, body) tuples.
        """
        if not run_id:
            run_id = new_run_id()

        results = {}
        for site_id in self.loader.list_sites():
            site = self.loader.load(site_id)
            count = count_per_site

            if count == 0:
                # Use configured frequency: sum of all article type frequencies
                count = sum(
                    at.get("frequency_per_week", 1)
                    for at in site.get_enabled_article_types()
                )
                # Daily run = weekly frequency / 7, minimum 1
                count = max(1, count // 7)

            topics = self.generate_for_site(site_id, count=count, run_id=run_id)
            results[site_id] = topics

        return results

    def _distribute_by_frequency(
        self, article_types: list[dict], total_count: int
    ) -> list[tuple[dict, int]]:
        """Distribute topic count across article types by their configured frequency."""
        total_freq = sum(at.get("frequency_per_week", 1) for at in article_types)
        if total_freq == 0:
            total_freq = len(article_types)

        distribution = []
        remaining = total_count

        for at in article_types:
            freq = at.get("frequency_per_week", 1)
            share = max(1, round(total_count * freq / total_freq))
            share = min(share, remaining)
            distribution.append((at, share))
            remaining -= share

        # Give leftovers to the highest-frequency type
        if remaining > 0 and distribution:
            at, count = distribution[0]
            distribution[0] = (at, count + remaining)

        return distribution

    def _generate_topics(
        self,
        site_context: SiteContext,
        article_type: dict,
        count: int,
        existing_topics: list[str],
        run_id: str,
    ) -> list[tuple[dict, str]]:
        """Generate topics for a specific article type using Haiku."""
        import anthropic

        voice = site_context.voice
        audience = site_context.audience
        seo = site_context.seo

        # Build existing topics block (for dedup)
        existing_block = ""
        if existing_topics:
            recent = existing_topics[:50]  # Don't overwhelm the prompt
            existing_block = f"""
ALREADY COVERED (do NOT repeat these or close variations):
{json.dumps(recent, indent=2)}
"""

        system = f"""You are the Topic Generator for an automated article factory.
Your job: generate {count} unique, high-quality topic ideas for articles.

SITE: {site_context.site_name}
NICHE: {site_context.niche} / {site_context.sub_niche}
AUDIENCE: {audience.get('profile', 'General')} | Expertise: {audience.get('expertise_level', 'mixed')}
VOICE: {voice.get('tone', 'professional')}

ARTICLE TYPE: {article_type.get('label', 'Article')}
{article_type.get('description', '')}
Word count range: {article_type.get('word_count_min', 800)}-{article_type.get('word_count_max', 2000)}

PRIMARY KEYWORDS: {json.dumps(seo.get('primary_keywords', [])[:10])}
KEYWORD CLUSTERS: {json.dumps(seo.get('keyword_clusters', [])[:5])}
{existing_block}

Respond with ONLY a JSON array of {count} topic objects:
[
    {{
        "topic": "Clear, specific topic statement",
        "keywords": ["primary keyword", "secondary keyword"],
        "angle": "What makes this take unique or valuable",
        "notes": "Any research direction hints or special considerations"
    }}
]

RULES:
1. Topics must be specific, not generic. "Magnesium Threonate for Sleep Onset Latency" not "Magnesium and Sleep"
2. Each topic must have a clear angle — why THIS article, why NOW
3. Keywords should be realistic search terms people actually use
4. Mix evergreen and timely topics
5. Respect the niche — stay in lane
6. No duplicates or near-duplicates of existing topics
7. Think SEO: would someone search for this?"""

        user = f"Generate {count} topic ideas for {article_type.get('label', 'articles')} on {site_context.site_name}. JSON array only."

        client = anthropic.Anthropic()
        response = client.messages.create(
            model=self.model,
            max_tokens=self.default_max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
        )

        response_text = ""
        for block in response.content:
            if block.type == "text":
                response_text += block.text

        return self._parse_topics(
            response_text, site_context, article_type, run_id
        )

    def _parse_topics(
        self,
        response_text: str,
        site_context: SiteContext,
        article_type: dict,
        run_id: str,
    ) -> list[tuple[dict, str]]:
        """Parse LLM response into topic artifacts."""
        import re

        text = response_text.strip()
        # Strip markdown fences if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

        try:
            topics = json.loads(text)
        except json.JSONDecodeError as e:
            # Try to find JSON array in response
            match = re.search(r"\[.*\]", text, re.DOTALL)
            if match:
                try:
                    topics = json.loads(match.group(0))
                except json.JSONDecodeError:
                    logger.error(f"[topic_gen] Failed to parse topics: {e}")
                    return []
            else:
                logger.error(f"[topic_gen] No JSON array found in response")
                return []

        results = []
        for item in topics:
            if not isinstance(item, dict) or not item.get("topic"):
                continue

            article_id = new_article_id()

            meta = topic_metadata(
                run_id=run_id,
                article_id=article_id,
                site_id=site_context.site_id,
                article_type=article_type["type_id"],
                topic=item["topic"],
                keywords=item.get("keywords", []),
                angle=item.get("angle", ""),
            )

            # Body: notes and context for downstream modules
            body_parts = [f"# Topic: {item['topic']}"]
            if item.get("angle"):
                body_parts.append(f"\n**Angle:** {item['angle']}")
            if item.get("notes"):
                body_parts.append(f"\n**Notes:** {item['notes']}")
            if item.get("keywords"):
                body_parts.append(f"\n**Keywords:** {', '.join(item['keywords'])}")

            body = "\n".join(body_parts)
            results.append((meta, body))

        return results

    # ── Batch Support ───────────────────────────────────────

    def submit_batch(
        self,
        site_ids: list[str] = None,
        count_per_site: int = 5,
        batch_dir: str = "pipeline/batches",
        run_id: str = "",
    ) -> Optional[str]:
        """Submit topic generation as a batch (one request per site×type combo)."""
        import anthropic

        if not run_id:
            run_id = new_run_id()

        if not site_ids:
            site_ids = self.loader.list_sites()

        requests = []
        custom_id_map = []  # Track what each request is for

        for site_id in site_ids:
            site_context = self.loader.load(site_id)
            existing = self.history.get_existing_topics(site_id)
            article_types = site_context.get_enabled_article_types()
            type_counts = self._distribute_by_frequency(article_types, count_per_site)

            for article_type, type_count in type_counts:
                if type_count == 0:
                    continue

                voice = site_context.voice
                audience = site_context.audience
                seo = site_context.seo

                existing_block = ""
                if existing:
                    existing_block = f"\nALREADY COVERED (do NOT repeat):\n{json.dumps(existing[:50])}\n"

                system = f"""You are the Topic Generator for an automated article factory.
Generate {type_count} unique topic ideas.

SITE: {site_context.site_name} | NICHE: {site_context.niche}
AUDIENCE: {audience.get('profile', 'General')} | Expertise: {audience.get('expertise_level', 'mixed')}
ARTICLE TYPE: {article_type.get('label', 'Article')}
{article_type.get('description', '')}
KEYWORDS: {json.dumps(seo.get('primary_keywords', [])[:10])}
{existing_block}

Respond with ONLY a JSON array:
[{{"topic": "...", "keywords": [...], "angle": "...", "notes": "..."}}]

Topics must be specific, SEO-worthy, and unique."""

                user = f"Generate {type_count} topics. JSON only."

                custom_id = f"{site_id}__{article_type['type_id']}__{run_id}"
                custom_id_map.append({
                    "custom_id": custom_id,
                    "site_id": site_id,
                    "article_type_id": article_type["type_id"],
                    "count": type_count,
                })

                requests.append({
                    "custom_id": custom_id,
                    "params": {
                        "model": self.model,
                        "max_tokens": self.default_max_tokens,
                        "system": system,
                        "messages": [{"role": "user", "content": user}],
                    },
                })

        if not requests:
            return None

        client = anthropic.Anthropic()
        batch = client.batches.create(requests=requests)
        batch_id = batch.id

        # Save manifest with mapping info
        manifest_path = Path(batch_dir)
        manifest_path.mkdir(parents=True, exist_ok=True)
        manifest = {
            "batch_id": batch_id,
            "module": "topic_generator",
            "submitted_at": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id,
            "request_count": len(requests),
            "request_map": custom_id_map,
            "status": "submitted",
        }
        with open(manifest_path / f"batch_{batch_id}.json", "w") as f:
            json.dump(manifest, f, indent=2)

        logger.info(f"[topic_gen] ✅ Batch submitted: {batch_id} ({len(requests)} requests)")
        return batch_id

    def save_topics(self, topics: list[tuple[dict, str]], output_dir: str = "pipeline/topics") -> int:
        """Save generated topics as artifacts. Returns count saved."""
        count = 0
        for meta, body in topics:
            save_artifact(meta, body, output_dir)
            count += 1
        return count


# ── CLI ─────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="Article Factory — Topic Generator")
    parser.add_argument("mode", choices=["submit", "collect", "run"],
                      help="submit=batch, collect=batch results, run=realtime")
    parser.add_argument("--site", default="", help="Generate for specific site")
    parser.add_argument("--all", action="store_true", help="Generate for all sites")
    parser.add_argument("--count", type=int, default=5, help="Topics per site")
    parser.add_argument("--type", default="", help="Filter by article type")
    parser.add_argument("--output", default="pipeline/topics", help="Output directory")
    parser.add_argument("--batch-dir", default="pipeline/batches", help="Batch manifest dir")
    parser.add_argument("--config", default="config/sites", help="Site config directory")

    args = parser.parse_args()
    gen = TopicGenerator(config_dir=args.config)

    if args.mode == "run":
        run_id = new_run_id()

        if args.all:
            all_results = gen.generate_for_all(count_per_site=args.count, run_id=run_id)
            total = 0
            for site_id, topics in all_results.items():
                saved = gen.save_topics(topics, args.output)
                total += saved
                print(f"  {site_id}: {saved} topics")
            print(f"\n✅ Generated {total} topics across {len(all_results)} sites")
        elif args.site:
            topics = gen.generate_for_site(
                args.site, count=args.count,
                article_type_filter=args.type, run_id=run_id
            )
            saved = gen.save_topics(topics, args.output)
            print(f"\n✅ Generated {saved} topics for {args.site}")
        else:
            print("Specify --site or --all")

    elif args.mode == "submit":
        sites = [args.site] if args.site else None
        batch_id = gen.submit_batch(
            site_ids=sites, count_per_site=args.count, batch_dir=args.batch_dir
        )
        if batch_id:
            print(f"\n✅ Batch submitted: {batch_id}")
        else:
            print("\n⚠️  Nothing to submit")

    elif args.mode == "collect":
        print("Collect not yet implemented for topic_generator (use run mode)")


if __name__ == "__main__":
    main()
