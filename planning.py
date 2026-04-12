"""
============================================================
ARTICLE FACTORY — PLANNING MODULE
============================================================
Takes research artifacts → produces article plans.

Model: Haiku (structural work, cost-effective)
Input folder: pipeline/research/
Output folder: pipeline/plans/

Usage:
    python planning.py submit --input pipeline/research
    python planning.py collect --input pipeline/research --output pipeline/plans
    python planning.py run --input pipeline/research --output pipeline/plans --limit 1
============================================================
"""

import json
import logging
from typing import Optional

from base_module import BaseModule
from site_loader import SiteContext
from artifacts import plan_metadata, new_article_id

logger = logging.getLogger("article_factory.planning")


class PlanningModule(BaseModule):

    module_name = "planning"
    model = "claude-haiku-4-5-20251001"
    input_module = "research"
    max_retries = 2
    default_max_tokens = 4096

    # ── Prompt Construction ─────────────────────────────────

    def build_prompt(
        self,
        metadata: dict,
        body: str,
        site_context: SiteContext,
    ) -> tuple[str, str]:
        """Build system and user prompts from site context + research."""

        article_type_id = metadata.get("article_type", "")
        article_type = site_context.get_article_type(article_type_id)
        if not article_type:
            article_type = site_context.get_enabled_article_types()[0]

        voice = site_context.voice
        audience = site_context.audience
        categories = site_context.categories

        # Build category options for the prompt
        category_options = []
        for cat in categories:
            category_options.append(f"- {cat.get('slug')}: {cat.get('description', cat.get('label', ''))}")
        category_list = "\n".join(category_options) if category_options else "- general"

        system = f"""You are the Planning module of an automated article factory.
Your job: create a detailed article outline from the research provided.

SITE: {site_context.site_name} ({site_context.niche})
AUDIENCE: {audience.get('profile', 'General')} | Expertise: {audience.get('expertise_level', 'mixed')}
VOICE: {voice.get('tone', 'professional')} | Persona: {voice.get('persona', 'knowledgeable writer')}
POV: {voice.get('pov', 'third_person')} | Reading level: {voice.get('reading_level', 'grade_10')}

ARTICLE TYPE: {article_type.get('label', 'Article')}
{article_type.get('description', '')}
Word count: {article_type.get('word_count_min', 800)}-{article_type.get('word_count_max', 2000)}
Structure: {json.dumps(article_type.get('structure', []))}
Citations required: {article_type.get('citation_required', False)}

SEO KEYWORDS: {json.dumps(site_context.seo.get('primary_keywords', [])[:5])}
INTERNAL LINK TARGETS: {json.dumps(site_context.seo.get('internal_link_targets', [])[:5])}
AVOID: {json.dumps(voice.get('avoid', []))}

CATEGORIES (pick the BEST fit):
{category_list}

Respond with ONLY valid JSON (no markdown fences, no explanation):
{{
    "title": "Compelling, SEO-friendly article title",
    "slug": "url-friendly-slug",
    "category": "best-fit-category-slug",
    "seo_title": "SEO title (50-60 chars)",
    "meta_description": "Meta description (150-160 chars)",
    "target_keywords": ["primary", "secondary"],
    "target_word_count": {article_type.get('word_count_min', 800)},
    "internal_links": ["relevant link targets"],
    "outline": [
        {{
            "section_id": "s1",
            "heading": "Section Heading",
            "purpose": "What this section accomplishes",
            "key_points": ["point 1", "point 2"],
            "target_words": 200,
            "sources_to_cite": ["source title"]
        }}
    ]
}}

RULES:
1. Follow the structure template for this article type
2. Distribute word count to hit target
3. Map research sources to sections where they belong
4. Title must include primary keyword naturally
5. Every section needs a clear purpose — no filler
6. Pick the single BEST category from the list above — must be exact slug"""

        # User message: the research content
        key_findings = metadata.get("key_findings", [])
        sources = metadata.get("sources", [])

        user = f"""Create a detailed article plan from this research:

TOPIC: {metadata.get('topic', 'Unknown')}

RESEARCH BRIEF:
{body}

KEY FINDINGS:
{json.dumps(key_findings, indent=2)}

SOURCES:
{json.dumps(sources, indent=2)}

Respond with ONLY the JSON object."""

        return system, user

    # ── Response Parsing ────────────────────────────────────

    def parse_response(
        self,
        response_text: str,
        input_metadata: dict,
        input_body: str,
        site_context: SiteContext,
    ) -> tuple[dict, str]:
        """Parse LLM JSON response into plan artifact (metadata + body)."""

        # Clean JSON from response
        text = response_text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

        try:
            plan = json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse plan JSON: {e}\nRaw: {text[:500]}")

        # Build metadata
        meta = plan_metadata(
            run_id=input_metadata.get("run_id", ""),
            article_id=input_metadata.get("article_id", new_article_id()),
            site_id=input_metadata.get("site_id", ""),
            article_type=input_metadata.get("article_type", ""),
            topic=input_metadata.get("topic", ""),
            title=plan.get("title", "Untitled"),
            slug=plan.get("slug", "untitled"),
            target_word_count=plan.get("target_word_count", 1000),
            seo_title=plan.get("seo_title", ""),
            meta_description=plan.get("meta_description", ""),
            target_keywords=plan.get("target_keywords", []),
            internal_links=plan.get("internal_links", []),
            section_count=len(plan.get("outline", [])),
        )

        # Add category from plan (AI picks best fit from site categories)
        meta["category"] = plan.get("category", "")

        # Build body: human-readable outline in markdown
        outline = plan.get("outline", [])

        # Carry sources forward from research
        meta["sources"] = input_metadata.get("sources", [])

        body_lines = [f"# Article Plan: {plan.get('title', 'Untitled')}\n"]
        body_lines.append(f"**Target:** {plan.get('target_word_count', 0)} words\n")
        body_lines.append(f"**Keywords:** {', '.join(plan.get('target_keywords', []))}\n")

        for section in outline:
            body_lines.append(f"\n## {section.get('heading', 'Section')}")
            body_lines.append(f"*Purpose:* {section.get('purpose', '')}")
            body_lines.append(f"*Target words:* {section.get('target_words', 0)}")
            if section.get("key_points"):
                for point in section["key_points"]:
                    body_lines.append(f"- {point}")
            if section.get("sources_to_cite"):
                body_lines.append(f"*Sources:* {', '.join(section['sources_to_cite'])}")

        # Also embed the raw JSON outline in a fenced block for Write to parse
        body_lines.append(f"\n\n---\n\n```json\n{json.dumps(outline, indent=2)}\n```")

        body = "\n".join(body_lines)

        return meta, body

    # ── Validation ──────────────────────────────────────────

    def validate_input(self, metadata: dict, body: str) -> tuple[bool, str]:
        """Research must have a topic, body, and minimum findings."""
        base_valid, base_error = super().validate_input(metadata, body)
        if not base_valid:
            return False, base_error

        if not metadata.get("topic"):
            return False, "Missing topic"

        findings = metadata.get("key_findings", [])
        if len(findings) < 2:
            return False, f"Only {len(findings)} key findings (min 2)"

        return True, ""

    def validate_output(self, metadata: dict, body: str) -> tuple[bool, str]:
        """Plan must have title, slug, and enough sections."""
        base_valid, base_error = super().validate_output(metadata, body)
        if not base_valid:
            return False, base_error

        if not metadata.get("title"):
            return False, "Missing title"
        if not metadata.get("slug"):
            return False, "Missing slug"
        if metadata.get("section_count", 0) < 2:
            return False, f"Only {metadata.get('section_count', 0)} sections (min 2)"
        if metadata.get("target_word_count", 0) < 100:
            return False, f"Target word count too low: {metadata.get('target_word_count', 0)}"

        return True, ""


# ── CLI ─────────────────────────────────────────────────────

if __name__ == "__main__":
    PlanningModule.cli()
