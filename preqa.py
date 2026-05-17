"""
============================================================
ARTICLE FACTORY — PRE-QA SNIFF TEST MODULE
============================================================
Module 3.5: Fast, cheap pre-filter before full QA.

Model: Haiku (cheap, fast — ~0.5¢ per article)
Input folder: pipeline/articles/
Output: PASS/FAIL verdict (articles that PASS continue to QA)

Purpose:
  Save expensive Sonnet QA calls by filtering out obviously bad articles.
  Catches: too short, missing structure, off-topic, empty sections, broken formatting.

This is NOT a full quality assessment. It's a sniff test.
Articles that PASS here still need full QA.
Articles that FAIL get sent back to Write with basic feedback.

Usage:
    python preqa.py run --input pipeline/articles --output pipeline/preqa --limit 1
============================================================
"""

import json
import re
import logging
from typing import Optional

from base_module import BaseModule
from site_loader import SiteContext
from artifacts import base_metadata

logger = logging.getLogger("article_factory.preqa")


# ── PreQA Module ─────────────────────────────────────────────

class PreQAModule(BaseModule):

    module_name = "preqa"
    model = "claude-haiku-4-5-20251001"  # Cheap! ~0.5¢ per article
    input_module = "write"
    max_retries = 1
    default_max_tokens = 512  # Very short responses needed

    # ── Prompt Construction ─────────────────────────────────

    def build_prompt(
        self,
        metadata: dict,
        body: str,
        site_context: SiteContext,
    ) -> tuple[str, str]:
        """Build a fast, simple sniff test prompt."""

        article_type_id = metadata.get("article_type", "")
        article_type = site_context.get_article_type(article_type_id)
        if not article_type:
            article_type = site_context.get_enabled_article_types()[0]

        word_min = article_type.get("word_count_min", 600)
        word_max = article_type.get("word_count_max", 3000)
        actual_words = metadata.get("word_count", 0)

        system = f"""You are a FAST pre-filter for an article factory.
Your job: quick sniff test to catch obviously bad articles BEFORE expensive full QA.

SITE: {site_context.site_name}
TOPIC EXPECTED: {metadata.get('topic', 'Unknown')}
WORD COUNT: {actual_words} (target range: {word_min}-{word_max})

CHECK THESE THINGS ONLY (be fast, not thorough):
1. STRUCTURE: Does it have an H1 title and at least 2 H2 sections?
2. LENGTH: Is word count within 50% of target range? (Very lenient)
3. ON-TOPIC: Does the article appear to be about the expected topic?
4. COMPLETENESS: No obviously empty sections, no "TODO" placeholders, no "[insert here]"
5. FORMATTING: No broken markdown, no raw HTML, no obvious errors

DO NOT CHECK:
- Voice quality (full QA does this)
- Citation quality (full QA does this)
- AI detection (full QA does this)
- Factual accuracy (full QA does this)
- SEO optimization (full QA does this)

Respond with ONLY this JSON (no markdown, no explanation):
{{
    "verdict": "PASS" or "FAIL",
    "reason": "One sentence if FAIL, empty if PASS"
}}

Be LENIENT. When in doubt, PASS. Only FAIL obvious problems."""

        user = f"""Quick check this article:

TITLE: {metadata.get('title', 'Unknown')}
EXPECTED TOPIC: {metadata.get('topic', 'Unknown')}

---
{body[:3000]}
---

(Article truncated for speed. Check structure and topic from what you see.)

Quick verdict - JSON only:"""

        return system, user

    # ── Response Parsing ────────────────────────────────────

    def parse_response(
        self,
        response_text: str,
        input_metadata: dict,
        input_body: str,
        site_context: SiteContext,
    ) -> tuple[dict, str]:
        """Parse PreQA sniff test result."""

        # Clean JSON
        text = response_text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

        try:
            result = json.loads(text)
        except json.JSONDecodeError as e:
            # If parsing fails, PASS (don't block on PreQA errors)
            logger.warning(f"[preqa] Parse error, defaulting to PASS: {e}")
            result = {"verdict": "PASS", "reason": ""}

        verdict = result.get("verdict", "PASS").upper()
        if verdict not in ("PASS", "FAIL"):
            verdict = "PASS"  # Be lenient

        reason = result.get("reason", "")

        # Build metadata
        meta = base_metadata(
            run_id=input_metadata.get("run_id", ""),
            article_id=input_metadata.get("article_id", ""),
            site_id=input_metadata.get("site_id", ""),
            module="preqa",
            article_type=input_metadata.get("article_type", ""),
        )
        meta["preqa_verdict"] = verdict
        meta["preqa_reason"] = reason

        # Carry forward all input metadata for next stage
        for key in ["title", "slug", "topic", "target_word_count", "word_count",
                    "seo_title", "meta_description", "tags", "sources",
                    "internal_links", "product_mentions", "category"]:
            if key in input_metadata:
                meta[key] = input_metadata[key]

        # Body passes through unchanged
        return meta, input_body

    # ── Validation ──────────────────────────────────────────

    def validate_input(self, metadata: dict, body: str) -> tuple[bool, str]:
        """Basic input check."""
        if not metadata.get("site_id"):
            return False, "Missing site_id"
        if not body or len(body.strip()) < 50:
            return False, f"Body too short ({len(body.strip())} chars)"
        return True, ""

    def validate_output(self, metadata: dict, body: str) -> tuple[bool, str]:
        """PreQA output must have verdict."""
        verdict = metadata.get("preqa_verdict", "")
        if verdict not in ("PASS", "FAIL"):
            return False, f"Invalid verdict: {verdict}"
        return True, ""

    def get_max_tokens(self, metadata: dict, site_context: SiteContext) -> int:
        """Very short responses needed."""
        return 256


# ── Standalone check (for orchestrator) ──────────────────────

def run_preqa_check(metadata: dict, body: str) -> tuple[bool, str]:
    """
    Quick standalone function for orchestrator to call.
    Returns (passed: bool, reason: str)
    """
    preqa = PreQAModule()
    try:
        out_meta, _ = preqa.run_single(metadata, body)
        verdict = out_meta.get("preqa_verdict", "PASS")
        reason = out_meta.get("preqa_reason", "")
        return verdict == "PASS", reason
    except Exception as e:
        # On error, pass through (don't block)
        logger.warning(f"[preqa] Error in sniff test, passing through: {e}")
        return True, ""


# ── CLI ─────────────────────────────────────────────────────

if __name__ == "__main__":
    PreQAModule.cli()
