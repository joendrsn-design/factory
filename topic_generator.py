"""
============================================================
ARTICLE FACTORY — TOPIC GENERATOR (Module 0)
============================================================
Generates topic queues for each site based on:
  - Site config (niche, article types, keyword clusters)
  - Publishing history (what's already been written)
  - Voice exemplars (topics must be writeable in site voice)
  - Coverage gaps (prefer under-covered keyword clusters)

Anti-Slop Discipline:
  - Topics are SUBJECTS, not HEADLINES — headlines come from Planning
  - Banned patterns: listicles, insider-knowledge framing, clickbait
  - Voice-conditional topic shapes (clinical, contemplative, trading)
  - Deterministic slop validation rejects bad topics post-generation
  - Semantic dedup catches near-duplicates, not just exact matches

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
import re
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

    def find_similar_topics(
        self, candidate_topic: str, site_id: str, threshold: float = 0.82
    ) -> list[dict]:
        """
        Return existing topics semantically similar to the candidate.
        Uses the same embedding infrastructure as the link recommender.
        """
        try:
            from linking.recommender import LinkRecommender
            recommender = LinkRecommender()
            # Recommender already has site-scoped embeddings
            candidates = recommender.get_candidates(
                site_id=site_id,
                topic=candidate_topic,
                title=candidate_topic,
                limit=20,
            )
            return [
                {"slug": c.slug, "title": c.title, "similarity": c.similarity}
                for c in candidates
                if c.similarity >= threshold
            ]
        except Exception as e:
            logger.debug(f"[topic_gen] Semantic dedup unavailable: {e}")
            return []

    def get_coverage_by_cluster(
        self, site_id: str, keyword_clusters: list[dict]
    ) -> dict:
        """
        Returns {cluster_name: count_of_existing_articles} for the site.
        Helps Topic Generator prefer under-covered clusters.
        """
        existing = self.get_existing_topics(site_id)
        coverage = {}

        for cluster in keyword_clusters:
            cluster_name = cluster.get("name", cluster.get("label", ""))
            cluster_keywords = [k.lower() for k in cluster.get("keywords", [])]
            count = 0
            for topic in existing:
                if any(kw in topic.lower() for kw in cluster_keywords):
                    count += 1
            coverage[cluster_name] = count

        return coverage

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

    def _build_system_prompt(
        self,
        site_context: SiteContext,
        article_type: dict,
        count: int,
        existing_topics: list[str],
        coverage: dict,
    ) -> str:
        """
        Single source of truth for the topic generation system prompt.
        Used by both realtime (_generate_topics) and batch (submit_batch) paths.
        """
        voice = site_context.voice
        audience = site_context.audience
        seo = site_context.seo
        niche = site_context.niche.lower()

        # Voice exemplars block (Change 2)
        exemplar_block = ""
        if site_context.voice_exemplars:
            # Take first 1-2 exemplars only to save tokens
            exemplar_text = site_context.voice_exemplars[:1500]
            exemplar_block = f"""
=== VOICE EXEMPLARS (target prose for this site) ===
{exemplar_text.strip()}

The topics you generate should be writeable in this voice. A clinical site
needs evidence-shaped topics; a contemplative site needs question-shaped topics;
a trading site needs setup-shaped topics. Use the exemplars to calibrate.
"""

        # Voice-conditional topic shape hints (Change 3)
        topic_shape_hint = ""

        if any(k in niche for k in ["philosophy", "stoic", "contemplat", "spiritual"]):
            topic_shape_hint = """
TOPIC SHAPE — Contemplative/Philosophical:
- Topics are questions, ideas, or single concepts to sit with
- Anchor in a thinker, text, or specific notion when possible
- Good: "Marcus Aurelius on the asymmetry of judgment and event"
- Good: "What Augustine meant by restless heart"
- Good: "The Stoic distinction between preferred and dispreferred indifferents"
- Bad: "5 Stoic Principles for Modern Life" (listicle slop)
- Bad: "How Stoicism Can Change Your Life" (self-help slop)
"""
        elif any(k in niche for k in ["medical", "clinical", "pathology", "diagnostics", "laboratory"]):
            topic_shape_hint = """
TOPIC SHAPE — Clinical/Medical:
- Topics are clinical questions, diagnostic frameworks, or evidence reviews
- Specify the population, intervention, comparator, and outcome where applicable
- Good: "GLP-1 receptor agonists in patients over 65: dosing and renal considerations"
- Good: "Liquid biopsy for minimal residual disease in colorectal cancer: current evidence"
- Good: "Differential diagnosis of elevated ALP with normal GGT"
- Bad: "Everything You Need to Know About GLP-1 Drugs" (consumer slop)
- Bad: "The Cancer Test Doctors Don't Tell You About" (clickbait)
"""
        elif any(k in niche for k in ["health", "longevity", "wellness", "supplement", "nutrition"]):
            topic_shape_hint = """
TOPIC SHAPE — Health/Wellness DTC:
- Topics are evidence-based questions a thoughtful consumer would ask
- Specific intervention, specific outcome, specific population where possible
- Good: "Magnesium glycinate vs. magnesium oxide for sleep onset latency"
- Good: "Time-restricted eating: current evidence in adults over 50"
- Good: "Creatine monohydrate dosing for cognitive vs. muscular endpoints"
- Bad: "10 Best Supplements for Sleep" (listicle, no specificity)
- Bad: "The Anti-Aging Protocol That Changed My Life" (anecdote-bait)
"""
        elif any(k in niche for k in ["trading", "finance", "invest", "market", "stocks"]):
            topic_shape_hint = """
TOPIC SHAPE — Trading/Finance:
- Topics are setups, frameworks, instruments, or specific market conditions
- Name the instrument, the timeframe, the pattern, and the context
- Good: "Inside bar setups on SPY daily after FTC confirmation"
- Good: "Reading PSAR trail stops on /ZB futures"
- Good: "Put credit spread sizing for 7 DTE expirations on SPY"
- Bad: "Why Bitcoin Is Going to $1 Million" (price-target slop)
- Bad: "The Trading Strategy Wall Street Doesn't Want You to Know" (clickbait)
"""

        # Coverage block (Change 6)
        coverage_block = ""
        if coverage:
            sorted_coverage = sorted(coverage.items(), key=lambda x: x[1])
            under_covered = [name for name, count in sorted_coverage[:3] if name]
            over_covered = [name for name, cnt in sorted_coverage[-3:] if cnt > 5 and name]

            if under_covered or over_covered:
                coverage_block = f"""
COVERAGE STATE (article counts by keyword cluster):
{json.dumps(coverage, indent=2)}

PREFER topics in under-covered clusters: {under_covered if under_covered else 'N/A'}
AVOID adding more topics in over-saturated clusters: {over_covered if over_covered else 'N/A'}
"""

        # Freshness hint for time-sensitive niches (Change 10)
        freshness_hint = ""
        if any(k in niche for k in ["medical", "clinical", "trading", "finance"]):
            freshness_hint = f"""
FRESHNESS:
Today's date is {datetime.now().strftime('%B %Y')}. For evidence-based or
market-sensitive topics, consider whether recent developments warrant a topic
that addresses them. Examples:
- Recently approved drug or device → topic on its evidence base
- Recent landmark trial publication → topic synthesizing findings
- Recent policy change → topic on practical implications

Do NOT fabricate specific recent events. Only propose freshness-adjacent topics
if you genuinely know recent developments in this area. The Research module
will verify and source.
"""

        # Existing topics block (filtered by relevance - Change 9)
        existing_block = ""
        if existing_topics:
            # Filter to relevant subset if article type has focus keywords
            article_type_keywords = article_type.get("topic_focus_keywords", [])
            if article_type_keywords:
                relevant_existing = [
                    t for t in existing_topics
                    if any(kw.lower() in t.lower() for kw in article_type_keywords)
                ][:30]
            else:
                relevant_existing = existing_topics[:30]

            if relevant_existing:
                existing_block = f"""
ALREADY COVERED (do NOT repeat these or close variations):
{json.dumps(relevant_existing, indent=2)}
"""

        system = f"""You are the Topic Generator for an automated article factory.
Your job: generate {count} unique, high-quality topic ideas for articles.

SITE: {site_context.site_name}
NICHE: {site_context.niche} / {site_context.sub_niche}
AUDIENCE: {audience.get('profile', 'General')} | Expertise: {audience.get('expertise_level', 'mixed')}
VOICE: {voice.get('tone', 'professional')} | Persona: {voice.get('persona', '')}
{exemplar_block}

ARTICLE TYPE: {article_type.get('label', 'Article')}
{article_type.get('description', '')}
Word count range: {article_type.get('word_count_min', 800)}-{article_type.get('word_count_max', 2000)}

PRIMARY KEYWORDS: {json.dumps(seo.get('primary_keywords', [])[:10])}
KEYWORD CLUSTERS: {json.dumps(seo.get('keyword_clusters', [])[:5])}
{coverage_block}
{freshness_hint}
{existing_block}

=== TOPIC DISCIPLINE ===

A topic is a SUBJECT, not a HEADLINE. It describes what the article will cover,
declaratively. Headlines come later from Planning. Your job is to produce a
clean, descriptive subject the rest of the pipeline can frame appropriately.

BANNED TOPIC PATTERNS:
- "[Number] [Adjective] [Things/Ways/Reasons] About X"
  Bad: "5 Surprising Benefits of Magnesium"
  Good: "Magnesium supplementation: clinical effects on sleep and anxiety"

- "What [Most People/Nobody] [Knows/Realizes] About X"
  Bad: "What Most People Don't Know About Cortisol"
  Good: "Cortisol's role in metabolic regulation and sleep"

- "The [Hidden/Surprising/Shocking] Truth About X"
  Bad: "The Hidden Truth About Seed Oils"
  Good: "Seed oil consumption and inflammatory markers: current evidence"

- "Why X Will [Change/Transform/Revolutionize] Y"
  Bad: "Why GLP-1 Drugs Will Revolutionize Aging"
  Good: "GLP-1 receptor agonists and longevity: mechanisms and current trials"

- "How to [Verb] Like a [Persona]"
  Bad: "How to Trade Like a Pro"
  Good: "Identifying high-probability inside bar setups on the SPY daily chart"

- Question topics that imply gotchas
  Bad: "Are You Doing X All Wrong?"
  Good: "Common errors in X and how to correct them"

GOOD TOPIC SHAPE:
- Names a specific subject, mechanism, intervention, question, or framework
- Could be the title of a textbook chapter or journal article
- Includes enough specificity that the article writes itself differently
  from any other article on the same general subject
- Does not promise revelation, surprise, or insider knowledge
- Does not include adjectives like "shocking," "surprising," "hidden," "real"

SPECIFICITY TEST:
If the topic could apply equally well to ten different articles, it is too generic.
"Magnesium and Sleep" → too generic
"Magnesium glycinate vs. magnesium oxide for sleep onset latency in adults over 50" → specific

{topic_shape_hint}

RELEVANCE:
Topics should answer real questions readers in this niche actually ask.
The test is "does someone with this question deserve a substantive article?"
not "would this title rank well in search?"

Search relevance comes from being the best answer to a real question,
not from headline optimization. Headlines come later.

Respond with ONLY a JSON array of {count} topic objects:
[
    {{
        "topic": "Direct, descriptive topic statement (subject, not headline)",
        "primary_keyword": "Single primary keyword for SEO",
        "secondary_keywords": ["supporting keyword 1", "supporting keyword 2"],
        "intent": "What specific question does this article answer for the reader?",
        "audience_subset": "Which slice of the site's audience is this for?",
        "target_depth": "shallow | standard | deep",
        "angle": "What makes this article worth writing? What's the specific lens?",
        "evidence_demand": "low | medium | high",
        "notes": "Research direction, source hints, special considerations"
    }}
]

FIELD GUIDANCE:

primary_keyword: The single most important keyword. Should be searchable.
                 Used by Planning for SEO title and Write for first-100-words placement.

intent: The reader's question, in their words. "How do I dose creatine for cognition?"
        not "Discover the cognitive benefits of creatine."

audience_subset: Not "general audience." Be specific. "Adults 50+ with sleep onset issues"
                 or "Intermediate options traders sizing positions" or "Readers familiar with
                 basic Stoic vocabulary."

target_depth:
  - shallow: 400-800 words, single concept, daily/quick read
  - standard: 800-1500 words, full topic coverage
  - deep: 1500+ words, multi-faceted, evidence-heavy or framework-heavy

evidence_demand:
  - low: Contemplative, opinion, framework articles
  - medium: General health, trading education, philosophy explanations
  - high: Clinical articles, medical content, statistical claims

angle: Why THIS article? What does it offer that the topic alone doesn't promise?
       "Synthesizes three recent trials with conflicting findings"
       "Translates a technical concept for the general reader"
       "Applies a known framework to a new domain"

RULES:
1. Topics must be specific, not generic — pass the specificity test
2. Each topic must have a clear angle — why THIS article, why NOW
3. Keywords should be realistic search terms people actually use
4. Mix evergreen and timely topics
5. Respect the niche — stay in lane
6. No duplicates or near-duplicates of existing topics
7. Fill gaps in under-covered keyword clusters when possible"""

        return system

    def _is_slop_topic(self, topic: str) -> tuple[bool, str]:
        """
        Deterministic check for slop-shaped topics.
        Returns (is_slop, reason).
        """
        SLOP_PATTERNS = [
            (r"^\d+\s+(surprising|hidden|shocking|amazing|incredible|powerful)",
             "numbered listicle with hype adjective"),
            (r"^\d+\s+(things|ways|reasons|tips|tricks|hacks|secrets)",
             "numbered listicle"),
            (r"\bwhat\s+(most\s+people|nobody|no\s+one)\s+(know|realize|tell|understand)",
             "insider-knowledge framing"),
            (r"\bthe\s+(hidden|surprising|shocking|real|ugly)\s+(truth|reason|cost|story)",
             "hidden-truth framing"),
            (r"\bwhy\s+\w+\s+(will|is\s+about\s+to)\s+(explode|change|transform|revolutionize|destroy)",
             "inevitability framing"),
            (r"\bhow\s+to\s+\w+\s+like\s+a\s+\w+",
             "imitation framing"),
            (r"\b(you\s+won'?t\s+believe|mind[\s-]?blowing)",
             "clickbait phrase"),
            (r"\b(the\s+ultimate|the\s+complete|everything\s+you\s+need)",
             "exhaustive-claim phrase"),
        ]

        topic_lower = topic.lower().strip()
        for pattern, reason in SLOP_PATTERNS:
            if re.search(pattern, topic_lower, re.IGNORECASE):
                return True, reason
        return False, ""

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

        seo = site_context.seo

        # Get coverage by cluster for gap awareness (Change 6)
        coverage = self.history.get_coverage_by_cluster(
            site_context.site_id,
            seo.get("keyword_clusters", [])
        )

        # Build system prompt using consolidated method (Change 8)
        system = self._build_system_prompt(
            site_context, article_type, count, existing_topics, coverage
        )

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
        """Parse LLM response into topic artifacts with slop validation and semantic dedup."""
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
        rejected_count = 0

        for item in topics:
            if not isinstance(item, dict) or not item.get("topic"):
                continue

            topic_str = item["topic"]

            # Change 7: Deterministic slop validation
            is_slop, reason = self._is_slop_topic(topic_str)
            if is_slop:
                logger.warning(f"[topic_gen] Rejected slop topic '{topic_str}': {reason}")
                rejected_count += 1
                continue

            # Change 5: Semantic dedup
            similar = self.history.find_similar_topics(
                topic_str,
                site_context.site_id,
                threshold=0.82
            )
            if similar:
                logger.info(
                    f"[topic_gen] Rejected near-duplicate topic '{topic_str}': "
                    f"matches '{similar[0]['title']}' (similarity={similar[0]['similarity']:.2f})"
                )
                rejected_count += 1
                continue

            article_id = new_article_id()

            # Build enriched metadata (Change 4)
            meta = topic_metadata(
                run_id=run_id,
                article_id=article_id,
                site_id=site_context.site_id,
                article_type=article_type["type_id"],
                topic=topic_str,
                keywords=item.get("secondary_keywords", item.get("keywords", [])),
                angle=item.get("angle", ""),
            )

            # Add enriched fields
            meta["primary_keyword"] = item.get("primary_keyword", "")
            meta["secondary_keywords"] = item.get("secondary_keywords", [])
            meta["intent"] = item.get("intent", "")
            meta["audience_subset"] = item.get("audience_subset", "")
            meta["target_depth"] = item.get("target_depth", "standard")
            meta["evidence_demand"] = item.get("evidence_demand", "medium")
            meta["notes"] = item.get("notes", "")

            # Body: notes and context for downstream modules
            body_parts = [f"# Topic: {topic_str}"]
            if item.get("intent"):
                body_parts.append(f"\n**Reader Intent:** {item['intent']}")
            if item.get("audience_subset"):
                body_parts.append(f"\n**Audience:** {item['audience_subset']}")
            if item.get("angle"):
                body_parts.append(f"\n**Angle:** {item['angle']}")
            if item.get("target_depth"):
                body_parts.append(f"\n**Depth:** {item['target_depth']}")
            if item.get("evidence_demand"):
                body_parts.append(f"\n**Evidence Demand:** {item['evidence_demand']}")
            if item.get("notes"):
                body_parts.append(f"\n**Notes:** {item['notes']}")
            if item.get("primary_keyword"):
                body_parts.append(f"\n**Primary Keyword:** {item['primary_keyword']}")
            if item.get("secondary_keywords"):
                body_parts.append(f"\n**Secondary Keywords:** {', '.join(item['secondary_keywords'])}")

            body = "\n".join(body_parts)
            results.append((meta, body))

        # Log rejection rate
        if rejected_count > 0:
            total = len(topics)
            logger.info(f"[topic_gen] Rejected {rejected_count}/{total} topics ({rejected_count/total*100:.0f}%)")

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
            seo = site_context.seo

            # Get coverage by cluster for gap awareness
            coverage = self.history.get_coverage_by_cluster(
                site_id,
                seo.get("keyword_clusters", [])
            )

            for article_type, type_count in type_counts:
                if type_count == 0:
                    continue

                # Use consolidated prompt builder (Change 8)
                system = self._build_system_prompt(
                    site_context, article_type, type_count, existing, coverage
                )

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
