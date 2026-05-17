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
  on the same topic. If fresh enough AND high-quality enough, reuses it.
  Vault scoring: 60% freshness + 40% source quality (Tier 1-2 ratio).
  Topic matching uses embeddings (threshold 0.85) with word-overlap fallback.

Source Quality Discipline:
  - Every source is assigned a tier (1-4) based on objective characteristics
  - Tier 1: peer-reviewed journals, Cochrane, government guidelines
  - Tier 2: reputable medical orgs, major news citing primary sources
  - Tier 3: trade publications, aggregators citing primary sources
  - Tier 4: blogs, wellness sites, marketing content — avoid
  - Clinical sites require majority Tier 1-2 sources

Source Verification:
  - Every cited URL is HEAD-checked after LLM synthesis
  - Clinical sites get deep verification (title word matching)
  - URLs not in search results are flagged as potential hallucinations
  - Unverified sources can be dropped or flagged per site config

Statistics Provenance:
  - Every statistic captures population, sample_size, methodology
  - data_year vs publication_year distinguished
  - is_derivable flag for Write module's "sourced or derived" rule

Voice-Conditional Research Shape:
  - Clinical: PICO framework, effect sizes, safety signals
  - Philosophy: primary texts, scholarly editions, passages
  - Trading: primary data, timeframes, instruments
  - DTC Health: evidence-based, consumer-translated

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
import re
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional
from urllib.parse import urlparse

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
    If relevant, recent, AND high-quality research exists, reuse it.

    Scoring: 60% freshness + 40% source quality (Tier 1-2 ratio).
    Topic matching uses embeddings (threshold 0.85) with word-overlap fallback.
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
        min_tier_1_2_ratio: float = 0.0,
    ) -> Optional[tuple[dict, str]]:
        """
        Search vault for existing research, scoring by freshness AND quality.
        Returns the best match meeting both freshness and quality thresholds.

        Args:
            topic: The topic to search for
            site_id: Primary site ID
            max_age_days: Maximum age for freshness scoring
            shared_sites: Additional sites to check for shared research
            min_tier_1_2_ratio: Minimum ratio of Tier 1-2 sources required

        Returns:
            (metadata, body) if found meeting thresholds, None otherwise
        """
        # Sites to check
        check_sites = [site_id]
        if shared_sites:
            check_sites.extend(shared_sites)

        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

        # Normalize topic for matching
        topic_lower = topic.lower().strip()

        candidates = []  # list of (meta, body, score)

        for check_site in check_sites:
            artifacts = load_artifacts_from_dir(
                str(self.vault_dir),
                module_filter="research",
                status_filter="complete",
            )

            for meta, body, fpath in artifacts:
                # Topic match (semantic or word-overlap)
                existing_topic = meta.get("topic", "").lower().strip()
                if not self._topics_match(topic_lower, existing_topic):
                    continue

                # Freshness scoring
                timestamp_str = meta.get("timestamp", "")
                try:
                    ts = datetime.fromisoformat(timestamp_str)
                    if ts < cutoff:
                        logger.debug(f"[vault] Found but stale: {existing_topic}")
                        continue
                    age_days = (datetime.now(timezone.utc) - ts).days
                    freshness_score = max(0, 1 - (age_days / max_age_days))
                except (ValueError, TypeError):
                    continue

                # Quality scoring based on source tiers
                sources = meta.get("sources", [])
                tier_1_2_count = sum(
                    1 for s in sources if int(s.get("tier", 4)) <= 2
                )
                quality_score = tier_1_2_count / len(sources) if sources else 0

                # Skip if doesn't meet quality threshold
                if quality_score < min_tier_1_2_ratio:
                    logger.debug(
                        f"[vault] '{existing_topic}' below quality threshold: "
                        f"{quality_score:.0%} < {min_tier_1_2_ratio:.0%}"
                    )
                    continue

                # Combined score: 60% freshness + 40% quality
                combined = 0.6 * freshness_score + 0.4 * quality_score
                candidates.append((meta, body, combined))

        if not candidates:
            return None

        # Return the highest-scoring candidate
        candidates.sort(key=lambda x: x[2], reverse=True)
        best_meta, best_body, best_score = candidates[0]
        logger.info(
            f"[vault] ✅ Cache hit (score {best_score:.2f}): "
            f"'{best_meta.get('topic')}' from {best_meta.get('site_id')}"
        )
        return best_meta, best_body

    def deposit(self, metadata: dict, body: str) -> Path:
        """Deposit research into the vault for future reuse."""
        return save_artifact(metadata, body, str(self.vault_dir))

    def _topics_match(self, topic_a: str, topic_b: str, threshold: float = 0.85) -> bool:
        """
        Semantic topic matching using embeddings.
        Falls back to word-overlap if embeddings unavailable.

        Higher threshold (0.85) than topic-generator dedup (0.82) because
        vault reuse should be more conservative — better to re-research
        than reuse stale-but-similar work.
        """
        try:
            from linking.embeddings import get_embedder
            import numpy as np

            embedder = get_embedder()
            emb_a = embedder.embed(topic_a)
            emb_b = embedder.embed(topic_b)

            # Cosine similarity
            sim = np.dot(emb_a, emb_b) / (np.linalg.norm(emb_a) * np.linalg.norm(emb_b))
            return sim >= threshold
        except Exception as e:
            logger.debug(f"[vault] Embedding match unavailable, using word overlap: {e}")
            return self._word_overlap_match(topic_a, topic_b)

    def _word_overlap_match(self, topic_a: str, topic_b: str) -> bool:
        """Fallback word-overlap matching (original logic, more conservative)."""
        words_a = set(topic_a.split()) - {"and", "the", "of", "for", "in", "on", "a", "an"}
        words_b = set(topic_b.split()) - {"and", "the", "of", "for", "in", "on", "a", "an"}

        if not words_a or not words_b:
            return False

        overlap = words_a & words_b
        smaller = min(len(words_a), len(words_b))

        # Raised from 0.6 to 0.7 — more conservative for vault reuse
        return len(overlap) / smaller >= 0.7


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
        min_tier_1_2_ratio = research_cfg.get("source_quality", {}).get("min_tier_1_2_ratio", 0.0)

        # 1. CHECK VAULT (scores by freshness AND quality)
        cached = self.vault.find_existing(
            topic, site_id, max_age, shared_sites, min_tier_1_2_ratio
        )
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

    def _build_search_queries(
        self,
        topic: str,
        site_context: SiteContext,
        config: dict,
    ) -> list[str]:
        """
        Build search queries calibrated to depth, niche, and current date.

        Returns niche-aware queries that:
        - Use current year (not hardcoded)
        - Include evidence-focused queries for clinical niches
        - Include counterargument query at deep+ levels
        """
        current_year = datetime.now().year
        last_year = current_year - 1
        niche = site_context.niche.lower()

        queries = [topic]  # Primary query — just the topic itself

        if config["queries"] >= 2:
            # Evidence-focused query, niche-conditional
            if any(k in niche for k in ["medical", "clinical", "pathology", "health"]):
                queries.append(f"{topic} systematic review meta-analysis")
            elif any(k in niche for k in ["trading", "finance"]):
                queries.append(f"{topic} primary data SEC filing")
            elif any(k in niche for k in ["philosophy", "stoic"]):
                queries.append(f"{topic} primary text translation")
            else:
                queries.append(f"{topic} research evidence")

        if config["queries"] >= 3:
            # Recency query, niche-conditional (uses current year, not hardcoded)
            if any(k in niche for k in ["medical", "clinical"]):
                queries.append(f"{topic} {last_year} {current_year} clinical guidelines")
            elif any(k in niche for k in ["trading", "finance"]):
                queries.append(f"{topic} {current_year} current data")
            else:
                queries.append(f"{topic} {current_year} recent findings")

        if config["queries"] >= 4:
            # Counterargument / nuance query — defends against echo-chamber sourcing
            queries.append(f"{topic} criticism limitations debate")

        return queries

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

        # Build niche-aware, date-aware search queries
        queries = self._build_search_queries(topic, site_context, config)

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

    # ── Source Verification ────────────────────────────────

    def _verify_sources(self, sources: list[dict], deep_verify: bool = False) -> list[dict]:
        """
        Verify that cited sources actually exist.

        Light verification (default): HEAD request, check for 200 response.
        Deep verification (clinical articles): GET, check that source title
        appears in the page, optionally check that cited stat values appear.

        Returns sources with added 'verification' field:
        {
            'verified': bool,
            'http_status': int or None,
            'verified_at': iso timestamp,
            'verification_notes': str,
        }
        """
        import requests as req

        verified_sources = []
        for src in sources:
            url = src.get("url", "").strip()
            verification = {
                "verified": False,
                "http_status": None,
                "verified_at": datetime.now(timezone.utc).isoformat(),
                "verification_notes": "",
            }

            # Skip if already verified from search provider
            if src.get("verification", {}).get("from") == "search_provider":
                src["verification"]["verified"] = True
                verified_sources.append(src)
                continue

            if not url or not url.startswith(("http://", "https://")):
                verification["verification_notes"] = "Invalid or missing URL"
                src["verification"] = verification
                verified_sources.append(src)
                continue

            try:
                # Light verification: HEAD request
                resp = req.head(url, timeout=10, allow_redirects=True,
                               headers={"User-Agent": "ArticleFactory/1.0 (research verification)"})
                verification["http_status"] = resp.status_code

                if resp.status_code < 400:
                    verification["verified"] = True
                else:
                    verification["verification_notes"] = f"HTTP {resp.status_code}"

                # Deep verification for clinical sources
                if deep_verify and resp.status_code < 400:
                    try:
                        full_resp = req.get(
                            url, timeout=15,
                            headers={"User-Agent": "ArticleFactory/1.0 (research verification)"}
                        )
                        body_lower = full_resp.text.lower()
                        title = src.get("title", "").lower()
                        # Check that some portion of the title appears in the page
                        title_words = [w for w in title.split() if len(w) > 4]
                        if title_words:
                            matches = sum(1 for w in title_words if w in body_lower)
                            if matches < len(title_words) * 0.5:
                                verification["verification_notes"] = (
                                    f"Title words match {matches}/{len(title_words)} — "
                                    f"possible mismatch"
                                )
                                verification["verified"] = False
                    except Exception as e:
                        verification["verification_notes"] = f"Deep verify failed: {e}"

            except req.exceptions.Timeout:
                verification["verification_notes"] = "Request timeout"
            except req.exceptions.RequestException as e:
                verification["verification_notes"] = f"Request failed: {type(e).__name__}"
            except Exception as e:
                verification["verification_notes"] = f"Verification error: {e}"

            src["verification"] = verification
            verified_sources.append(src)

        return verified_sources

    def _check_source_diversity(self, sources: list[dict]) -> dict:
        """
        Detect echo-chamber sourcing where many sources point to the same primary.
        Returns diversity metrics for QA visibility.
        """
        # Domain diversity
        domains = set()
        for src in sources:
            url = src.get("url", "")
            if url:
                try:
                    domain = urlparse(url).netloc.replace("www.", "")
                    domains.add(domain)
                except Exception:
                    continue

        # Tier diversity
        tier_counts = {}
        for src in sources:
            tier = src.get("tier", 4)
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

        # Type diversity
        type_counts = {}
        for src in sources:
            stype = src.get("type", "unknown")
            type_counts[stype] = type_counts.get(stype, 0) + 1

        # Primary source presence
        primary_count = sum(1 for s in sources if s.get("primary_or_secondary") == "primary")

        metrics = {
            "total_sources": len(sources),
            "unique_domains": len(domains),
            "domain_diversity_ratio": len(domains) / len(sources) if sources else 0,
            "tier_distribution": tier_counts,
            "type_distribution": type_counts,
            "primary_source_count": primary_count,
            "primary_source_ratio": primary_count / len(sources) if sources else 0,
        }

        warnings = []
        if metrics["domain_diversity_ratio"] < 0.6 and len(sources) >= 5:
            warnings.append("Low domain diversity — possible echo-chamber sourcing")
        if metrics["primary_source_ratio"] < 0.3 and len(sources) >= 5:
            warnings.append("Low primary-source ratio — research may be over-aggregated")
        if len(sources) >= 5 and tier_counts.get(1, 0) == 0:
            warnings.append("No Tier 1 sources — consider adding foundational evidence")

        metrics["warnings"] = warnings
        return metrics

    def _merge_sources(
        self,
        llm_sources: list[dict],
        search_results: list[dict],
    ) -> list[dict]:
        """
        Merge LLM-asserted sources with web-search sources.
        LLM sources are kept only if they correspond to a real search result URL.
        Search results not cited by LLM are kept as 'available_but_uncited' for QA.

        This is the upstream defense against hallucinated citations.
        """
        if not search_results:
            return llm_sources

        if not llm_sources:
            # Convert search results to source format
            return [
                {
                    "source_id": f"src_{i+1:03d}",
                    "title": r.get("title", ""),
                    "url": r.get("url", ""),
                    "publisher": urlparse(r.get("url", "")).netloc.replace("www.", ""),
                    "snippet": r.get("snippet", "")[:200],
                    "type": "unknown",
                    "tier": 3,  # Default to caution-tier when LLM didn't characterize
                    "relevance": "medium",
                    "primary_or_secondary": "secondary",
                    "verification": {"verified": True, "from": "search_provider"},
                }
                for i, r in enumerate(search_results)
            ]

        # Both present — match LLM citations to search URLs
        search_urls = {r.get("url", "").lower(): r for r in search_results}
        merged = []

        for src in llm_sources:
            url = src.get("url", "").lower()
            if url in search_urls:
                # LLM cited a real search result — keep with LLM characterization
                search_data = search_urls[url]
                src["snippet"] = src.get("snippet") or search_data.get("snippet", "")[:200]
                src["verification"] = {
                    "verified": True,
                    "from": "search_provider",
                    "matched_search_result": True,
                }
                merged.append(src)
            else:
                # LLM cited a URL not in search results — possible hallucination
                # Keep but flag for verification (HEAD check happens in _verify_sources)
                src["verification"] = {
                    "verified": False,
                    "from": "llm_only",
                    "matched_search_result": False,
                    "verification_notes": "URL not in search results — verify before use",
                }
                merged.append(src)

        return merged

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
            citation_required = article_type.get("citation_required", True)

        audience = site_context.audience
        voice = site_context.voice
        niche = site_context.niche.lower()

        depth_instructions = {
            "shallow": "Provide a concise overview. 3-5 key points. Brief and focused.",
            "moderate": "Provide solid coverage. 5-8 key findings with supporting evidence.",
            "deep": "Provide comprehensive analysis. 8-12 key findings. Cite specific studies, data points, and mechanisms of action. Include nuance and conflicting evidence.",
            "exhaustive": "Provide exhaustive coverage. 12+ key findings. Every major study, every mechanism, every counterargument. This is reference-grade research.",
        }

        # Voice-conditional research shape guidance (Change 6)
        niche_research_guidance = ""
        if any(k in niche for k in ["philosophy", "stoic", "contemplat", "spiritual"]):
            niche_research_guidance = """
=== RESEARCH SHAPE — Contemplative/Philosophical ===
This is not a clinical literature review. The research brief should:
- Identify the primary text(s) and authoritative translations/editions
- Surface relevant passages with citations to the standard reference (Meditations 4.7,
  Republic 514a, Summa I-II, Q.6, etc.)
- Note major scholarly interpretations only when they bear on the article
- Capture historical context only when it changes how the idea reads today
- The "statistics" array will usually be empty; this is fine.
- Sources are primary texts and major scholarly works, not search results.
- Total brief length: shorter than for clinical work. Aim for 600-1200 words.
"""
        elif any(k in niche for k in ["medical", "clinical", "pathology"]):
            niche_research_guidance = """
=== RESEARCH SHAPE — Clinical/Medical ===
This is reference-grade medical research. The brief should:
- Lead with foundational evidence (RCTs, meta-analyses, guidelines)
- Specify population, intervention, comparator, and outcome (PICO) for studies
- Include effect sizes with confidence intervals where reported
- Note conflicting evidence and address it explicitly
- Flag safety signals, contraindications, and special populations
- Distinguish between guideline recommendations and emerging evidence
- Tier 1 sources should be majority of citations
- Statistics should include sample sizes and methodology notes
"""
        elif any(k in niche for k in ["trading", "finance"]):
            niche_research_guidance = """
=== RESEARCH SHAPE — Trading/Finance ===
This is decision-grade financial research. The brief should:
- Cite primary data (SEC filings, exchange data, central bank releases) wherever possible
- For technical/setup articles: focus on price action history, not commentary
- Specify the timeframe and instrument for any technical claim
- Note that historical performance does not predict future results in the brief
- For market structure claims: prefer original studies over secondary commentary
- Avoid forecast-heavy sources (price predictions); prefer analytical sources
"""
        elif any(k in niche for k in ["health", "longevity", "wellness", "supplement"]):
            niche_research_guidance = """
=== RESEARCH SHAPE — Health/Wellness DTC ===
This is evidence-based consumer health research. The brief should:
- Lead with peer-reviewed evidence; demote wellness blogs to background
- Translate clinical terminology for general audiences in the brief
- Include effect sizes when available, not just "studies show"
- Address safety, dosing, and contraindications explicitly
- Note where evidence is limited or mixed — do not overstate
- For supplement topics: distinguish between forms (e.g., magnesium oxide vs. glycinate)
"""

        system = f"""You are the Research module of an automated article factory.
Your job: produce a comprehensive research brief that will be used to plan and write an article.

SITE: {site_context.site_name}
NICHE: {site_context.niche} / {site_context.sub_niche}
AUDIENCE: {audience.get('profile', 'General')} | Expertise: {audience.get('expertise_level', 'mixed')}
RESEARCH DEPTH: {research_depth}
{depth_instructions.get(research_depth, depth_instructions['moderate'])}
CITATIONS REQUIRED: {citation_required}
{niche_research_guidance}

Your output has TWO parts:

PART 1: A JSON block (wrapped in ```json fences) containing structured metadata:
```json
{{
    "key_findings": [
        "Finding 1 with specific data",
        "Finding 2 with specific data"
    ],
    "sources": [
        {{
            "source_id": "src_001",
            "title": "Source Title",
            "url": "https://...",
            "publisher": "Journal name, organization, or domain",
            "type": "peer_reviewed | government | guideline | meta_analysis | systematic_review | clinical_trial | preprint | reputable_organization | reputable_news | trade_publication | blog | aggregator | unknown",
            "tier": "1 | 2 | 3 | 4",
            "year": 2025,
            "publication_date": "YYYY-MM-DD or YYYY",
            "snippet": "Key quote or summary, max 200 chars",
            "relevance": "high | medium | low",
            "primary_or_secondary": "primary | secondary",
            "notes": "Methodology notes, sample size, population if relevant"
        }}
    ],
    "statistics": [
        {{
            "stat_id": "stat_001",
            "value": "48%",
            "value_numeric": 48.0,
            "unit": "percent",
            "context": "US adults whose magnesium intake fell below the EAR",
            "source_id": "src_001",
            "publication_year": 2024,
            "data_year": 2017,
            "data_year_range": "2013-2018",
            "population": "Non-pregnant US adults 19+, NHANES",
            "sample_size": 8341,
            "methodology_note": "24-hour dietary recall, two non-consecutive days",
            "confidence": "high | medium | low",
            "is_derivable": true,
            "derivation_note": "Reported figure or trivially calculable from source"
        }}
    ],
    "source_count": 8,
    "confidence": "high|medium|low",
    "gaps": ["Any areas where evidence was insufficient"]
}}
```

=== SOURCE TIERING (assign tier 1-4) ===

TIER 1 — Foundational evidence:
- Peer-reviewed journal articles (NEJM, Lancet, JAMA, Cell, Nature, etc.)
- Cochrane systematic reviews and meta-analyses
- Government clinical guidelines (NIH, CDC, WHO, FDA, NICE)
- Major specialty society guidelines (ACC/AHA, ASCO, ESMO, etc.)
- Federal statistical agencies (BLS, BEA, Federal Reserve, SEC filings)

TIER 2 — Reputable secondary:
- Reputable medical organizations (Mayo Clinic, Cleveland Clinic, academic medical centers)
- Major news outlets reporting on primary sources (Reuters, AP, Bloomberg, FT, WSJ)
- Industry research from established firms (McKinsey, Gartner, peer-reviewed think tanks)
- Preprints from established research groups (use cautiously, note as preprint)

TIER 3 — Use with caution:
- Trade publications and industry blogs
- Local news, opinion pieces in major outlets
- Health information aggregators (WebMD, Healthline) — only when they cite primary sources you can verify
- General-interest publications covering specialty topics

TIER 4 — Avoid unless no alternative:
- Personal blogs, wellness sites, marketing content disguised as content
- Social media posts, forum threads
- Unverified preprints, retracted papers
- Aggregators that don't cite primary sources
- Content marketing from product manufacturers

DEFAULT BEHAVIOR:
- For clinical/medical articles: REQUIRE majority of citations to be Tier 1-2.
  Reject the brief if more than 25% of sources are Tier 3-4.
- For DTC health: Aim for at least 60% Tier 1-2.
- For trading/finance: Tier 1 = primary data (SEC, Fed, exchanges). Tier 2 = major financial press. Avoid Tier 3-4 entirely for factual claims.
- For philosophy/contemplative: Tier system applies less directly — name primary texts and major scholarly editions/translations as Tier 1-2.

PRIMARY VS SECONDARY:
- primary: original research, original reporting, primary source documents
- secondary: aggregating, summarizing, or reporting on primary sources

NEVER cite a Tier 4 source as primary evidence for a clinical or factual claim.

=== STATISTICS PROVENANCE RULES ===

For EVERY statistic extracted, capture:
- value: the figure as it appears in the source (with unit and decimals)
- value_numeric: the numeric component for charting/comparison
- publication_year: year the source was published
- data_year: year the data refers to (often different from publication year)
- data_year_range: if the figure spans multiple years
- population: who the figure describes — be specific
- sample_size: n value if reported
- methodology_note: brief description of how the figure was generated
- is_derivable: true if Write can re-derive this from cited primary data,
  false if it must be quoted directly

DEFAULT CAUTION:
- If a source presents a number without methodology, mark confidence: medium
- If a source presents a number without a primary source citation, mark confidence: low
- Round-number forecasts ("1 billion users by 2030") with no derivation: do NOT extract as statistics.
  Note them in the brief as "claimed projection" but exclude from the statistics array.

=== PART 2: RESEARCH BRIEF ===

A detailed research brief in markdown. This is the meat — the actual research content that the Planning and Writing modules will use.

IMPORTANT: The research brief will be consumed by Planning and Write modules. It should be
written as research notes, not as draft article prose. Avoid the rhetorical patterns of
finished articles (no antithesis, no rhetorical questions, no manufactured stakes).
State findings declaratively. Let Planning shape the narrative.

Write it like a thorough research document:
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

        # Get research config from site context
        research_cfg = site_context.research
        quality_cfg = research_cfg.get("source_quality", {})

        # Build sources list — careful merge of LLM-cited sources with search results
        llm_sources = json_data.get("sources", [])
        final_sources = self._merge_sources(llm_sources, search_results or [])

        # Verify sources exist (HEAD check, optionally deep verify)
        deep_verify = research_cfg.get("deep_verify_sources", False)
        final_sources = self._verify_sources(final_sources, deep_verify=deep_verify)

        # Drop or flag unverified sources
        unverified = [s for s in final_sources if not s.get("verification", {}).get("verified")]
        unverified_count = len(unverified)

        if unverified:
            logger.warning(
                f"[research] {len(unverified)}/{len(final_sources)} sources failed verification"
            )

            # For clinical sites, drop unverified sources entirely
            if research_cfg.get("drop_unverified_sources", False):
                verified_only = [s for s in final_sources if s.get("verification", {}).get("verified")]
                if len(verified_only) >= 3:  # Keep some sources
                    final_sources = verified_only
                    logger.info(f"[research] Dropped {len(unverified)} unverified sources")
                # else: keep all but flag prominently

        # Check source quality tiering (Change 1)
        tier_counts = {1: 0, 2: 0, 3: 0, 4: 0}
        for src in final_sources:
            tier = int(src.get("tier", 4))
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

        source_quality_warning = None
        min_tier_1_2_ratio = quality_cfg.get("min_tier_1_2_ratio", 0.0)

        if final_sources:
            high_tier_ratio = (tier_counts[1] + tier_counts[2]) / len(final_sources)
            if high_tier_ratio < min_tier_1_2_ratio:
                logger.warning(
                    f"[research] Source quality below threshold: "
                    f"{high_tier_ratio:.0%} Tier 1-2 (required {min_tier_1_2_ratio:.0%})"
                )
                source_quality_warning = {
                    "tier_distribution": tier_counts,
                    "high_tier_ratio": high_tier_ratio,
                    "required": min_tier_1_2_ratio,
                }

        # Check source diversity (Change 5)
        diversity_metrics = self._check_source_diversity(final_sources)

        if diversity_metrics["warnings"]:
            logger.warning(
                f"[research] Source diversity issues: {diversity_metrics['warnings']}"
            )

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
            statistics=json_data.get("statistics", []),
        )

        # Add new quality/diversity metadata for QA visibility
        meta["source_diversity"] = diversity_metrics
        meta["gaps"] = json_data.get("gaps", [])

        if source_quality_warning:
            meta["source_quality_warning"] = source_quality_warning

        if unverified_count > 0:
            meta["unverified_source_count"] = unverified_count

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
        return {"key_findings": [], "sources": [], "statistics": [], "source_count": 0}

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

            # Check vault first (scores by freshness AND quality)
            research_cfg = site_context.research
            min_tier_1_2_ratio = research_cfg.get("source_quality", {}).get("min_tier_1_2_ratio", 0.0)
            cached = self.vault.find_existing(
                metadata.get("topic", ""),
                site_id,
                research_cfg.get("max_research_age_days", 30),
                research_cfg.get("shared_with", []),
                min_tier_1_2_ratio,
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
        """Research must have findings and substantive body, scaled by depth."""
        if not body or len(body.strip()) < 200:
            return False, f"Research brief too short ({len(body.strip())} chars, min 200)"

        findings = metadata.get("key_findings", [])
        depth = metadata.get("research_depth", "moderate")

        # Scale minimum findings by depth
        min_findings = {
            "shallow": 3,
            "moderate": 5,
            "deep": 8,
            "exhaustive": 12,
        }.get(depth, 5)

        if len(findings) < min_findings:
            return False, (
                f"Only {len(findings)} key findings for {depth} research "
                f"(min {min_findings})"
            )

        # Validate sources structure, also scaled by depth
        sources = metadata.get("sources", [])
        min_sources = {
            "shallow": 3,
            "moderate": 5,
            "deep": 8,
            "exhaustive": 12,
        }.get(depth, 5)

        if len(sources) < min_sources:
            return False, (
                f"Only {len(sources)} sources for {depth} research "
                f"(min {min_sources})"
            )

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
        min_tier_1_2_ratio = research_cfg.get("source_quality", {}).get("min_tier_1_2_ratio", 0.0)

        # Check vault (scores by freshness AND quality)
        cached = self.vault.find_existing(
            metadata.get("topic", ""),
            site_id,
            research_cfg.get("max_research_age_days", 30),
            research_cfg.get("shared_with", []),
            min_tier_1_2_ratio,
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
