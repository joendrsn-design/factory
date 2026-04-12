"""
============================================================
ARTICLE FACTORY — RESEARCH MODULE
============================================================
Module 1: Takes topic artifacts → produces research briefs.

Model: Sonnet 4.5 (reasoning + synthesis)
Input folder: pipeline/topics/
Output folder: pipeline/research/

Two-phase research:
  Phase 1: GATHER — web search for real sources (Tavily/Brave/Perplexity)
  Phase 2: SYNTHESIZE — Sonnet reads sources and produces structured brief

Research Vault:
  Before running new research, checks vault for existing research
  on the same topic. If fresh enough (per site config), reuses it.

Usage:
    python research.py submit --input pipeline/topics
    python research.py collect --input pipeline/topics --output pipeline/research
    python research.py run --input pipeline/topics --output pipeline/research --limit 1

Web search config (env vars):
    RESEARCH_SEARCH_PROVIDER=tavily|brave|none
    TAVILY_API_KEY=tvly-xxxxx
    BRAVE_API_KEY=BSA-xxxxx
============================================================
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from base_module import BaseModule
from site_loader import SiteContext
from artifacts import (
    research_metadata, new_article_id,
    save_artifact, load_artifacts_from_dir,
)

logger = logging.getLogger("article_factory.research")


# ── Web Search Providers ────────────────────────────────────
# Swappable. Add new providers by implementing search(query, max_results) → list[dict]

class SearchProvider:
    """Base class for web search providers."""

    def search(self, query: str, max_results: int = 10) -> list[dict]:
        """
        Returns list of:
            {"title": str, "url": str, "snippet": str, "content": str}
        """
        raise NotImplementedError


class TavilyProvider(SearchProvider):
    """Tavily search API — good balance of quality and cost."""

    def __init__(self):
        self.api_key = os.environ.get("TAVILY_API_KEY", "")
        if not self.api_key:
            raise ValueError("TAVILY_API_KEY not set")

    def search(self, query: str, max_results: int = 10) -> list[dict]:
        try:
            from tavily import TavilyClient
        except ImportError:
            raise ImportError("pip install tavily-python")

        client = TavilyClient(api_key=self.api_key)
        response = client.search(
            query=query,
            max_results=max_results,
            search_depth="advanced",
            include_raw_content=True,
        )

        results = []
        for item in response.get("results", []):
            results.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "snippet": item.get("content", "")[:500],
                "content": item.get("raw_content", item.get("content", ""))[:3000],
            })

        return results


class BraveProvider(SearchProvider):
    """Brave Search API — privacy-focused, good coverage."""

    def __init__(self):
        self.api_key = os.environ.get("BRAVE_API_KEY", "")
        if not self.api_key:
            raise ValueError("BRAVE_API_KEY not set")

    def search(self, query: str, max_results: int = 10) -> list[dict]:
        import requests as req

        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": self.api_key,
        }
        params = {"q": query, "count": max_results}
        resp = req.get("https://api.search.brave.com/res/v1/web/search",
                       headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for item in data.get("web", {}).get("results", []):
            results.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "snippet": item.get("description", "")[:500],
                "content": item.get("description", "")[:3000],
            })

        return results


class NoSearchProvider(SearchProvider):
    """Fallback: no web search. LLM synthesizes from training data only."""

    def search(self, query: str, max_results: int = 10) -> list[dict]:
        logger.info("[research] No search provider configured — using LLM knowledge only")
        return []


def get_search_provider() -> SearchProvider:
    """Factory function: returns configured search provider."""
    provider = os.environ.get("RESEARCH_SEARCH_PROVIDER", "none").lower()

    if provider == "tavily":
        return TavilyProvider()
    elif provider == "brave":
        return BraveProvider()
    else:
        return NoSearchProvider()


# ── Research Vault ──────────────────────────────────────────

class ResearchVault:
    """
    Checks for existing research before running new queries.
    If relevant, recent research exists, skip the API call and reuse.
    """

    def __init__(self, vault_dir: str = "pipeline/research_vault"):
        self.vault_dir = Path(vault_dir)
        self.vault_dir.mkdir(parents=True, exist_ok=True)

    def find_existing(
        self,
        topic: str,
        site_id: str,
        max_age_days: int = 30,
        shared_sites: list[str] = None,
    ) -> Optional[tuple[dict, str]]:
        """
        Search vault for existing research on this topic.
        Checks both the requesting site and any shared sites.

        Returns (metadata, body) if found and fresh, None otherwise.
        """
        # Sites to check
        check_sites = [site_id]
        if shared_sites:
            check_sites.extend(shared_sites)

        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

        # Normalize topic for matching
        topic_lower = topic.lower().strip()

        for check_site in check_sites:
            artifacts = load_artifacts_from_dir(
                str(self.vault_dir),
                module_filter="research",
                status_filter="complete",
            )

            for meta, body, fpath in artifacts:
                # Topic match (fuzzy: check if core words overlap)
                existing_topic = meta.get("topic", "").lower().strip()
                if not self._topics_match(topic_lower, existing_topic):
                    continue

                # Freshness check
                timestamp_str = meta.get("timestamp", "")
                try:
                    ts = datetime.fromisoformat(timestamp_str)
                    if ts < cutoff:
                        logger.debug(f"[vault] Found but stale: {existing_topic}")
                        continue
                except (ValueError, TypeError):
                    continue

                logger.info(f"[vault] ✅ Cache hit: '{existing_topic}' from {meta.get('site_id')}")
                return meta, body

        return None

    def deposit(self, metadata: dict, body: str) -> Path:
        """Deposit research into the vault for future reuse."""
        return save_artifact(metadata, body, str(self.vault_dir))

    def _topics_match(self, topic_a: str, topic_b: str) -> bool:
        """
        Simple topic matching: checks if core words overlap significantly.
        Not perfect but catches "magnesium threonate sleep" ≈ "magnesium threonate and sleep quality"
        """
        words_a = set(topic_a.split()) - {"and", "the", "of", "for", "in", "on", "a", "an"}
        words_b = set(topic_b.split()) - {"and", "the", "of", "for", "in", "on", "a", "an"}

        if not words_a or not words_b:
            return False

        overlap = words_a & words_b
        smaller = min(len(words_a), len(words_b))

        return len(overlap) / smaller >= 0.6


# ── Research Module ─────────────────────────────────────────

class ResearchModule(BaseModule):

    module_name = "research"
    model = "claude-sonnet-4-5-20250929"
    input_module = "topic_generator"
    max_retries = 2
    default_max_tokens = 4096

    def __init__(self, config_dir: str = "config/sites", vault_dir: str = "pipeline/research_vault"):
        super().__init__(config_dir=config_dir)
        self.vault = ResearchVault(vault_dir=vault_dir)
        self.search_provider = get_search_provider()
        logger.info(f"[research] Search provider: {self.search_provider.__class__.__name__}")

    # ── Core Processing ─────────────────────────────────────

    def process_single(
        self,
        input_artifact: tuple,
        site_context: SiteContext,
    ) -> tuple[dict, str]:
        """
        Full research pipeline for a single topic:
        1. Check Research Vault for existing research
        2. Web search for real sources (if provider configured)
        3. LLM synthesis into structured research brief
        """
        # Handle both tuple format (from run_realtime's internal) and direct call
        if isinstance(input_artifact, tuple):
            metadata, body = input_artifact[0], input_artifact[1]
        else:
            metadata, body = input_artifact, ""

        topic = metadata.get("topic", "")
        site_id = metadata.get("site_id", "")
        article_type_id = metadata.get("article_type", "")

        # Get research config from site context
        research_cfg = site_context.research
        max_age = research_cfg.get("max_research_age_days", 30)
        shared_sites = research_cfg.get("shared_with", [])

        # 1. CHECK VAULT
        cached = self.vault.find_existing(topic, site_id, max_age, shared_sites)
        if cached:
            cached_meta, cached_body = cached
            # Update metadata to reflect this run
            cached_meta["from_cache"] = True
            cached_meta["run_id"] = metadata.get("run_id", "")
            cached_meta["article_id"] = metadata.get("article_id", new_article_id())
            return cached_meta, cached_body

        # 2. WEB SEARCH
        article_type = site_context.get_article_type(article_type_id)
        research_depth = "moderate"
        if article_type:
            research_depth = article_type.get("research_depth", "moderate")

        search_results = self._do_web_search(topic, research_depth, site_context)

        # 3. LLM SYNTHESIS
        import anthropic
        client = anthropic.Anthropic()

        system_prompt, user_message = self.build_prompt(
            metadata, body, site_context, search_results
        )

        response = client.messages.create(
            model=self.model,
            max_tokens=self.get_max_tokens(metadata, site_context),
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )

        response_text = ""
        for block in response.content:
            if block.type == "text":
                response_text += block.text

        out_meta, out_body = self.parse_response(
            response_text, metadata, body, site_context, search_results
        )

        # Deposit to vault for future reuse
        self.vault.deposit(out_meta, out_body)

        return out_meta, out_body

    def _do_web_search(
        self,
        topic: str,
        research_depth: str,
        site_context: SiteContext,
    ) -> list[dict]:
        """Run web searches calibrated to research depth."""

        # Calibrate search volume by depth
        search_configs = {
            "shallow": {"queries": 1, "results_per": 3},
            "moderate": {"queries": 2, "results_per": 5},
            "deep": {"queries": 3, "results_per": 8},
            "exhaustive": {"queries": 4, "results_per": 10},
        }
        config = search_configs.get(research_depth, search_configs["moderate"])

        # Build search queries (primary + variations)
        queries = [topic]
        niche = site_context.niche

        if config["queries"] >= 2:
            queries.append(f"{topic} research studies evidence")
        if config["queries"] >= 3:
            queries.append(f"{topic} benefits risks {niche}")
        if config["queries"] >= 4:
            queries.append(f"{topic} latest findings 2025 2026")

        all_results = []
        seen_urls = set()

        for query in queries[:config["queries"]]:
            try:
                results = self.search_provider.search(query, max_results=config["results_per"])
                for r in results:
                    if r["url"] not in seen_urls:
                        seen_urls.add(r["url"])
                        all_results.append(r)
            except Exception as e:
                logger.warning(f"[research] Search failed for '{query}': {e}")

        logger.info(f"[research] Web search: {len(all_results)} unique sources from {len(queries)} queries")
        return all_results

    # ── Prompt Construction ─────────────────────────────────

    def build_prompt(
        self,
        metadata: dict,
        body: str,
        site_context: SiteContext,
        search_results: list[dict] = None,
    ) -> tuple[str, str]:
        """Build system + user prompts for research synthesis."""

        article_type_id = metadata.get("article_type", "")
        article_type = site_context.get_article_type(article_type_id)
        research_depth = "moderate"
        citation_required = False
        if article_type:
            research_depth = article_type.get("research_depth", "moderate")
            citation_required = article_type.get("citation_required", False)

        audience = site_context.audience
        voice = site_context.voice

        depth_instructions = {
            "shallow": "Provide a concise overview. 3-5 key points. Brief and focused.",
            "moderate": "Provide solid coverage. 5-8 key findings with supporting evidence.",
            "deep": "Provide comprehensive analysis. 8-12 key findings. Cite specific studies, data points, and mechanisms of action. Include nuance and conflicting evidence.",
            "exhaustive": "Provide exhaustive coverage. 12+ key findings. Every major study, every mechanism, every counterargument. This is reference-grade research.",
        }

        system = f"""You are the Research module of an automated article factory.
Your job: produce a comprehensive research brief that will be used to plan and write an article.

SITE: {site_context.site_name}
NICHE: {site_context.niche} / {site_context.sub_niche}
AUDIENCE: {audience.get('profile', 'General')} | Expertise: {audience.get('expertise_level', 'mixed')}
RESEARCH DEPTH: {research_depth}
{depth_instructions.get(research_depth, depth_instructions['moderate'])}
CITATIONS REQUIRED: {citation_required}

Your output has TWO parts:

PART 1: A JSON block (wrapped in ```json fences) containing structured metadata:
```json
{{
    "key_findings": [
        "Finding 1 with specific data",
        "Finding 2 with specific data"
    ],
    "sources": [
        {{"title": "Source Title", "url": "https://...", "snippet": "Key quote or summary", "relevance": "high|medium|low"}}
    ],
    "source_count": 8,
    "confidence": "high|medium|low",
    "gaps": ["Any areas where evidence was insufficient"]
}}
```

PART 2: A detailed research brief in markdown. This is the meat — the actual research content that the Planning and Writing modules will use. Write it like a thorough research document:
- Organize by themes/subtopics
- Include specific data points, statistics, study results
- Note mechanisms of action where relevant
- Flag conflicting evidence or debates
- Include practical implications
- Write for {audience.get('expertise_level', 'mixed')} audience expertise

TONE GUIDANCE (for the brief):
- {voice.get('tone', 'professional')}
- Avoid: {json.dumps(voice.get('avoid', []))}

The brief should be detailed enough that a writer can produce a full article without additional research."""

        # User message
        topic = metadata.get("topic", "Unknown")
        angle = metadata.get("angle", "")
        keywords = metadata.get("keywords", [])

        user_parts = [f"Research the following topic thoroughly:\n\nTOPIC: {topic}"]

        if angle:
            user_parts.append(f"\nANGLE/FOCUS: {angle}")
        if keywords:
            user_parts.append(f"\nKEYWORDS TO ADDRESS: {', '.join(keywords)}")
        if body.strip():
            user_parts.append(f"\nADDITIONAL CONTEXT:\n{body}")

        # Inject web search results if available
        if search_results:
            user_parts.append("\n\n--- WEB SEARCH RESULTS ---")
            user_parts.append("Use these sources to ground your research. Cite them where relevant.\n")

            for i, result in enumerate(search_results, 1):
                user_parts.append(f"### Source {i}: {result.get('title', 'Untitled')}")
                user_parts.append(f"URL: {result.get('url', '')}")
                content = result.get("content", result.get("snippet", ""))
                if content:
                    user_parts.append(f"Content:\n{content[:2000]}")
                user_parts.append("")
        else:
            user_parts.append("\n\nNo web search results available. Use your training knowledge to produce the most accurate, evidence-based research brief possible. Cite sources from your knowledge where you can.")

        user_parts.append("\nProduce the JSON metadata block first, then the full research brief.")

        return system, "\n".join(user_parts)

    # ── Response Parsing ────────────────────────────────────

    def parse_response(
        self,
        response_text: str,
        input_metadata: dict,
        input_body: str,
        site_context: SiteContext,
        search_results: list[dict] = None,
    ) -> tuple[dict, str]:
        """Parse LLM response into research artifact (metadata + body)."""

        # Extract JSON block from response
        json_data = self._extract_json(response_text)

        # Extract the brief (everything after the JSON block)
        brief_body = self._extract_brief(response_text)

        # Build sources list — merge LLM-cited sources with web search sources
        llm_sources = json_data.get("sources", [])

        # If we had web search results, prefer those URLs
        if search_results:
            # Use LLM sources but validate/enrich with search results
            final_sources = llm_sources if llm_sources else [
                {"title": r["title"], "url": r["url"],
                 "snippet": r["snippet"][:200], "relevance": "medium"}
                for r in search_results
            ]
        else:
            final_sources = llm_sources

        # Build metadata
        meta = research_metadata(
            run_id=input_metadata.get("run_id", ""),
            article_id=input_metadata.get("article_id", new_article_id()),
            site_id=input_metadata.get("site_id", ""),
            article_type=input_metadata.get("article_type", ""),
            topic=input_metadata.get("topic", ""),
            research_depth=json_data.get("research_depth",
                input_metadata.get("research_depth", "moderate")),
            source_count=len(final_sources),
            key_findings=json_data.get("key_findings", []),
            sources=final_sources,
            from_cache=False,
        )

        return meta, brief_body

    def _extract_json(self, text: str) -> dict:
        """Extract the JSON metadata block from the response."""
        import re

        # Look for ```json ... ``` block
        pattern = r"```json\s*\n(.*?)\n```"
        match = re.search(pattern, text, re.DOTALL)

        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                logger.warning("[research] Failed to parse JSON block from response")

        # Fallback: try to find any JSON object
        brace_start = text.find("{")
        if brace_start >= 0:
            depth = 0
            for i in range(brace_start, len(text)):
                if text[i] == "{":
                    depth += 1
                elif text[i] == "}":
                    depth -= 1
                    if depth == 0:
                        try:
                            return json.loads(text[brace_start:i+1])
                        except json.JSONDecodeError:
                            break

        logger.warning("[research] No valid JSON found in response, using empty metadata")
        return {"key_findings": [], "sources": [], "source_count": 0}

    def _extract_brief(self, text: str) -> str:
        """Extract the research brief (everything after the JSON block)."""
        import re

        # Remove the JSON block
        pattern = r"```json\s*\n.*?\n```"
        cleaned = re.sub(pattern, "", text, flags=re.DOTALL).strip()

        # Remove any leading "PART 2:" or similar headers
        cleaned = re.sub(r"^(PART\s*2\s*:?\s*)", "", cleaned, flags=re.IGNORECASE).strip()

        if not cleaned:
            # JSON took up the whole response, use everything as brief
            return text.strip()

        return cleaned

    # ── Batch Prompt Building (override for search_results) ──

    def build_prompt_for_batch(
        self,
        metadata: dict,
        body: str,
        site_context: SiteContext,
    ) -> tuple[str, str]:
        """
        Build prompts for batch mode.
        Note: batch mode skips web search (search happens in submit phase).
        Web search results are embedded in the body before batch submission.
        """
        return self.build_prompt(metadata, body, site_context, search_results=None)

    # ── Submit Override (run web search before batch) ────────

    def submit(
        self,
        input_dir: str,
        batch_dir: str = "pipeline/batches",
        site_filter: str = "",
        run_filter: str = "",
    ) -> Optional[str]:
        """
        Override submit to run web search BEFORE batch submission.
        Web search is fast and synchronous. LLM synthesis is batched.

        Flow:
        1. Load topic artifacts
        2. Check vault for each (skip if cached)
        3. Run web search for each (fast, synchronous)
        4. Embed search results into the user message
        5. Submit all to Anthropic batch
        """
        import anthropic

        artifacts = load_artifacts_from_dir(
            input_dir,
            module_filter=self.input_module,
            status_filter="complete",
            site_filter=site_filter or None,
            run_filter=run_filter or None,
        )

        if not artifacts:
            logger.warning(f"[{self.module_name}] No artifacts found in {input_dir}")
            return None

        logger.info(f"[{self.module_name}] Processing {len(artifacts)} topic artifacts")

        requests = []
        article_ids = []
        cached_count = 0

        for metadata, body, filepath in artifacts:
            valid, error = self.validate_input(metadata, body)
            if not valid:
                logger.warning(f"[{self.module_name}] Skipping {metadata.get('article_id')}: {error}")
                continue

            site_id = metadata.get("site_id", "")
            try:
                site_context = self.loader.load(site_id)
            except Exception as e:
                logger.error(f"[{self.module_name}] Config error for '{site_id}': {e}")
                continue

            # Check vault first
            research_cfg = site_context.research
            cached = self.vault.find_existing(
                metadata.get("topic", ""),
                site_id,
                research_cfg.get("max_research_age_days", 30),
                research_cfg.get("shared_with", []),
            )

            if cached:
                # Deposit cached version directly to output (skip batch)
                cached_meta, cached_body = cached
                cached_meta["run_id"] = metadata.get("run_id", "")
                cached_meta["article_id"] = metadata.get("article_id", "")
                cached_meta["from_cache"] = True
                # We'll save these after batch too, for now just count
                cached_count += 1
                continue

            # Run web search (synchronous, fast)
            article_type = site_context.get_article_type(metadata.get("article_type", ""))
            depth = article_type.get("research_depth", "moderate") if article_type else "moderate"
            search_results = self._do_web_search(metadata.get("topic", ""), depth, site_context)

            # Build prompts with search results embedded
            system_prompt, user_message = self.build_prompt(
                metadata, body, site_context, search_results
            )

            article_id = metadata.get("article_id", "unknown")
            article_ids.append(article_id)

            requests.append({
                "custom_id": article_id,
                "params": {
                    "model": self.model,
                    "max_tokens": self.get_max_tokens(metadata, site_context),
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_message}],
                },
            })

        if cached_count:
            logger.info(f"[{self.module_name}] {cached_count} artifacts served from vault cache")

        if not requests:
            logger.info(f"[{self.module_name}] All artifacts served from cache or skipped")
            return None

        # Submit batch
        client = anthropic.Anthropic()
        logger.info(f"[{self.module_name}] Submitting batch of {len(requests)} requests...")

        batch = client.batches.create(requests=requests)
        batch_id = batch.id
        logger.info(f"[{self.module_name}] ✅ Batch submitted: {batch_id}")

        from artifacts import save_batch_manifest
        save_batch_manifest(batch_id, self.module_name, article_ids, batch_dir)

        return batch_id

    # ── Validation ──────────────────────────────────────────

    def validate_input(self, metadata: dict, body: str) -> tuple[bool, str]:
        """Topic artifact must have a topic."""
        if not metadata.get("topic"):
            return False, "Missing topic"
        if not metadata.get("site_id"):
            return False, "Missing site_id"
        if not metadata.get("article_type"):
            return False, "Missing article_type"
        return True, ""

    def validate_output(self, metadata: dict, body: str) -> tuple[bool, str]:
        """Research must have findings and substantive body."""
        if not body or len(body.strip()) < 200:
            return False, f"Research brief too short ({len(body.strip())} chars, min 200)"

        findings = metadata.get("key_findings", [])
        if len(findings) < 2:
            return False, f"Only {len(findings)} key findings (min 2)"

        return True, ""

    def get_max_tokens(self, metadata: dict, site_context: SiteContext) -> int:
        """Research output can be lengthy. Scale by depth."""
        article_type = site_context.get_article_type(metadata.get("article_type", ""))
        depth = article_type.get("research_depth", "moderate") if article_type else "moderate"

        token_map = {
            "shallow": 2048,
            "moderate": 4096,
            "deep": 6144,
            "exhaustive": 8192,
        }
        return token_map.get(depth, 4096)

    def run_single(
        self,
        metadata: dict,
        body: str,
        output_dir: str = "",
    ) -> tuple[dict, str]:
        """
        Override run_single to include web search + vault integration.
        1. Check vault for cached research
        2. If not cached, run web search (synchronous, fast)
        3. Call LLM with search results embedded
        4. Deposit to vault
        """
        import anthropic

        valid, error = self.validate_input(metadata, body)
        if not valid:
            raise ValueError(f"[{self.module_name}] Input validation failed: {error}")

        site_id = metadata.get("site_id", "")
        site_context = self.loader.load(site_id)
        research_cfg = site_context.research

        # Check vault
        cached = self.vault.find_existing(
            metadata.get("topic", ""),
            site_id,
            research_cfg.get("max_research_age_days", 30),
            research_cfg.get("shared_with", []),
        )
        if cached:
            cached_meta, cached_body = cached
            cached_meta["run_id"] = metadata.get("run_id", "")
            cached_meta["article_id"] = metadata.get("article_id", "")
            cached_meta["from_cache"] = True
            if output_dir:
                save_artifact(cached_meta, cached_body, output_dir)
            logger.info(f"[{self.module_name}] Cache hit for '{metadata.get('topic', '')}'")
            return cached_meta, cached_body

        # Web search
        article_type = site_context.get_article_type(metadata.get("article_type", ""))
        depth = article_type.get("research_depth", "moderate") if article_type else "moderate"
        search_results = self._do_web_search(metadata.get("topic", ""), depth, site_context)

        # Build prompts with search results
        system_prompt, user_message = self.build_prompt(
            metadata, body, site_context, search_results
        )

        # Call LLM
        client = anthropic.Anthropic()
        for attempt in range(1, self.max_retries + 1):
            try:
                response = client.messages.create(
                    model=self.model,
                    max_tokens=self.get_max_tokens(metadata, site_context),
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_message}],
                )
                response_text = ""
                for block in response.content:
                    if block.type == "text":
                        response_text += block.text

                out_meta, out_body = self.parse_response(
                    response_text, metadata, body, site_context
                )

                valid, error = self.validate_output(out_meta, out_body)
                if valid:
                    if output_dir:
                        save_artifact(out_meta, out_body, output_dir)
                    # Deposit to vault
                    self.vault.deposit(out_meta, out_body)
                    logger.info(f"[{self.module_name}] ✅ {metadata.get('article_id', 'unknown')}")
                    return out_meta, out_body
                else:
                    logger.warning(f"[{self.module_name}] Validation attempt {attempt}: {error}")
                    if attempt == self.max_retries:
                        raise ValueError(f"Output validation failed: {error}")

            except anthropic.APIError as e:
                logger.error(f"[{self.module_name}] API error attempt {attempt}: {e}")
                if attempt == self.max_retries:
                    raise
                import time
                time.sleep(2 ** attempt)

        raise RuntimeError(f"[{self.module_name}] Failed after {self.max_retries} attempts")


# ── CLI ─────────────────────────────────────────────────────

if __name__ == "__main__":
    ResearchModule.cli()
