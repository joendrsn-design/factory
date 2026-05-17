"""
============================================================
ARTICLE FACTORY — EXPANSION MODULE
============================================================
Takes a single research artifact → produces multiple article angles.
Each angle becomes a separate artifact that flows through Planning → Write → QA.

This maximizes ROI on expensive research by generating 3-5 articles
from one research brief instead of 1.

Model: Haiku (cheap, structured output)
Input folder: pipeline/research/
Output folder: pipeline/angles/

Usage:
    python expansion.py run --input pipeline/research --output pipeline/angles --limit 1
============================================================
"""

import json
import logging
from typing import Optional

from base_module import BaseModule
from site_loader import SiteContext
from artifacts import base_metadata, new_article_id, save_artifact

logger = logging.getLogger("article_factory.expansion")


class ExpansionModule(BaseModule):
    """
    Expansion module generates multiple article angles from one research brief.

    Unlike other modules that produce 1 output per input, this module produces
    N outputs per input (where N is configurable per site).

    The parse_response method returns a LIST of (metadata, body) tuples.
    Each tuple becomes a separate angle artifact that flows to Planning.
    """

    module_name = "expansion"
    model = "claude-haiku-4-5-20251001"  # Cheap!
    input_module = "research"
    max_retries = 2
    default_max_tokens = 4096

    # ── Prompt Construction ─────────────────────────────────

    def build_prompt(
        self,
        metadata: dict,
        body: str,
        site_context: SiteContext,
        category_priorities: dict = None,
    ) -> tuple[str, str]:
        """
        Build prompts to generate multiple article angles from research.

        Args:
            metadata: Research artifact metadata
            body: Research artifact body
            site_context: Site configuration
            category_priorities: Optional dict with:
                - hungry_categories: list of categories needing content (prioritize these)
                - saturated_categories: list of categories to avoid
                - category_scores: dict of category -> priority score
        """

        expansion_config = site_context.raw_config.get("expansion", {})
        expansion_count = expansion_config.get("expansion_count", 3)
        allow_cross_category = expansion_config.get("allow_cross_category", True)
        angle_diversity = expansion_config.get("angle_diversity", "moderate")

        # Build category options with priority hints
        categories = site_context.categories
        hungry = category_priorities.get("hungry_categories", []) if category_priorities else []
        saturated = category_priorities.get("saturated_categories", []) if category_priorities else []

        category_options = []
        for cat in categories:
            slug = cat.get('slug', '')
            desc = cat.get('description', cat.get('label', ''))

            # Add priority hints
            if slug in saturated:
                hint = " ⛔ AT QUOTA - AVOID"
            elif slug in hungry[:3]:
                hint = " ⭐ HIGH PRIORITY - NEEDS CONTENT"
            elif slug in hungry[:6]:
                hint = " 📌 PRIORITY"
            else:
                hint = ""

            category_options.append(f"- {slug}: {desc}{hint}")

        category_list = "\n".join(category_options) if category_options else "- general"

        # Build article type options
        article_types = site_context.get_enabled_article_types()
        type_options = []
        for at in article_types:
            type_options.append(f"- {at.get('type_id')}: {at.get('label')} ({at.get('word_count_min', 500)}-{at.get('word_count_max', 2000)} words)")
        type_list = "\n".join(type_options) if type_options else "- deep_dive"

        diversity_guidance = {
            "low": "Angles should be variations on the same core topic with minor focus shifts.",
            "moderate": "Angles should explore distinct facets of the topic while sharing the research base.",
            "high": "Angles should be maximally diverse — different audiences, formats, and perspectives on the topic.",
        }

        # Build category distribution guidance
        if saturated:
            saturated_warning = f"""
⛔ SATURATED CATEGORIES (AT QUOTA - DO NOT USE):
{chr(10).join(f'  - {c}' for c in saturated)}

IMPORTANT: Do NOT assign any angles to saturated categories. They will be rejected."""
        else:
            saturated_warning = ""

        if hungry[:5]:
            hungry_guidance = f"""
⭐ PRIORITY CATEGORIES (NEED CONTENT - PREFER THESE):
{chr(10).join(f'  - {c}' for c in hungry[:5])}

Prefer assigning angles to priority categories when the topic fits."""
        else:
            hungry_guidance = ""

        system = f"""You are the Expansion module of an automated article factory.
Your job: generate {expansion_count} distinct article angles from one research brief.

SITE: {site_context.site_name} ({site_context.niche})
AUDIENCE: {site_context.audience.get('profile', 'General')}

DIVERSITY LEVEL: {angle_diversity.upper()}
{diversity_guidance.get(angle_diversity, diversity_guidance['moderate'])}

CROSS-CATEGORY: {"Angles can span different categories" if allow_cross_category else "All angles should fit the same category"}
{saturated_warning}
{hungry_guidance}

AVAILABLE CATEGORIES:
{category_list}

AVAILABLE ARTICLE TYPES:
{type_list}

For EACH angle, you must specify:
1. A unique focus or perspective on the topic
2. Target audience segment (if different from main)
3. Which findings from the research to emphasize
4. Suggested category and article type
5. A compelling hook that differentiates this angle

Respond with ONLY valid JSON (no markdown fences):
{{
    "angles": [
        {{
            "angle_index": 1,
            "angle_description": "Brief description of this angle's unique focus",
            "target_audience": "Specific audience segment for this angle",
            "suggested_title": "Working title for this angle",
            "suggested_category": "category-slug",
            "suggested_article_type": "type_id",
            "focus_findings": [0, 2, 5],
            "unique_hook": "What makes this angle stand out",
            "tone_adjustment": "Any voice/tone adjustments for this angle"
        }}
    ]
}}

RULES:
1. Generate exactly {expansion_count} angles
2. Each angle must have a genuinely distinct focus — no padding
3. Angles should NOT significantly overlap in content
4. Focus_findings indexes refer to the key_findings array in the research
5. At least one angle should target the site's primary audience
6. Consider different article types for different angles where appropriate"""

        # User message: the research content
        key_findings = metadata.get("key_findings", [])
        sources = metadata.get("sources", [])

        # Number the findings for reference
        numbered_findings = []
        for i, finding in enumerate(key_findings):
            numbered_findings.append(f"[{i}] {finding}")

        user = f"""Generate {expansion_count} distinct article angles from this research:

TOPIC: {metadata.get('topic', 'Unknown')}

RESEARCH BRIEF:
{body}

KEY FINDINGS (indexed for reference):
{chr(10).join(numbered_findings)}

SOURCE COUNT: {len(sources)} sources available

Respond with ONLY the JSON object containing exactly {expansion_count} angles."""

        return system, user

    # ── Response Parsing ────────────────────────────────────

    def parse_response(
        self,
        response_text: str,
        input_metadata: dict,
        input_body: str,
        site_context: SiteContext,
    ) -> tuple[dict, str]:
        """
        Parse LLM JSON response into angle artifacts.

        NOTE: Unlike other modules, this returns the FIRST angle only.
        The run_expansion method handles creating multiple outputs.

        For standard BaseModule compatibility, we return just one output here.
        Use run_expansion() for the full multi-output behavior.
        """

        # Clean JSON from response
        text = response_text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

        try:
            result = json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse expansion JSON: {e}\nRaw: {text[:500]}")

        angles = result.get("angles", [])
        if not angles:
            raise ValueError("No angles generated")

        # Return just the first angle for BaseModule compatibility
        angle = angles[0]
        return self._build_angle_artifact(angle, input_metadata, input_body, site_context)

    def parse_response_multi(
        self,
        response_text: str,
        input_metadata: dict,
        input_body: str,
        site_context: SiteContext,
    ) -> list[tuple[dict, str]]:
        """
        Parse LLM JSON response into MULTIPLE angle artifacts.
        Returns a list of (metadata, body) tuples, one per angle.
        """

        # Clean JSON from response
        text = response_text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

        try:
            result = json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse expansion JSON: {e}\nRaw: {text[:500]}")

        angles = result.get("angles", [])
        if not angles:
            raise ValueError("No angles generated")

        outputs = []
        for angle in angles:
            meta, body = self._build_angle_artifact(angle, input_metadata, input_body, site_context)
            outputs.append((meta, body))

        return outputs

    def _build_angle_artifact(
        self,
        angle: dict,
        input_metadata: dict,
        input_body: str,
        site_context: SiteContext,
    ) -> tuple[dict, str]:
        """Build a single angle artifact from angle data."""

        parent_article_id = input_metadata.get("article_id", "")
        angle_index = angle.get("angle_index", 1)

        # Generate unique article_id for this angle
        angle_article_id = f"{parent_article_id}_angle_{angle_index}"

        # Build metadata
        meta = base_metadata(
            run_id=input_metadata.get("run_id", ""),
            article_id=angle_article_id,
            site_id=input_metadata.get("site_id", ""),
            module="expansion",
        )

        # Add angle-specific fields
        meta["parent_article_id"] = parent_article_id
        meta["expansion_index"] = angle_index
        meta["angle_description"] = angle.get("angle_description", "")
        meta["target_audience"] = angle.get("target_audience", "")
        meta["suggested_title"] = angle.get("suggested_title", "")
        meta["suggested_category"] = angle.get("suggested_category", "")
        meta["article_type"] = angle.get("suggested_article_type", input_metadata.get("article_type", ""))
        meta["topic"] = input_metadata.get("topic", "")
        meta["unique_hook"] = angle.get("unique_hook", "")
        meta["tone_adjustment"] = angle.get("tone_adjustment", "")

        # Carry forward relevant research data
        focus_findings = angle.get("focus_findings", [])
        all_findings = input_metadata.get("key_findings", [])
        meta["key_findings"] = [all_findings[i] for i in focus_findings if i < len(all_findings)]
        meta["sources"] = input_metadata.get("sources", [])

        # Build body: filtered research content for this angle
        body_lines = [f"# Research Brief (Angle {angle_index}): {angle.get('suggested_title', 'Untitled')}\n"]
        body_lines.append(f"**Angle Focus:** {angle.get('angle_description', '')}\n")
        body_lines.append(f"**Target Audience:** {angle.get('target_audience', 'General')}\n")
        body_lines.append(f"**Unique Hook:** {angle.get('unique_hook', '')}\n")

        if angle.get("tone_adjustment"):
            body_lines.append(f"**Tone Adjustment:** {angle.get('tone_adjustment')}\n")

        body_lines.append(f"\n## Original Topic\n{input_metadata.get('topic', 'Unknown')}\n")

        body_lines.append(f"\n## Focused Findings\n")
        for i, finding in enumerate(meta["key_findings"]):
            body_lines.append(f"{i+1}. {finding}")

        body_lines.append(f"\n## Full Research Brief\n{input_body}")

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
        """Angle must have description, title, and category."""
        base_valid, base_error = super().validate_output(metadata, body)
        if not base_valid:
            return False, base_error

        if not metadata.get("angle_description"):
            return False, "Missing angle_description"
        if not metadata.get("suggested_title"):
            return False, "Missing suggested_title"
        if not metadata.get("expansion_index"):
            return False, "Missing expansion_index"

        return True, ""

    # ── Multi-Output Methods ────────────────────────────────

    def run_expansion(
        self,
        metadata: dict,
        body: str,
        output_dir: str = "",
        category_priorities: dict = None,
    ) -> list[tuple[dict, str]]:
        """
        Process a single research artifact and generate multiple angle artifacts.
        This is the method the orchestrator should call.

        Args:
            metadata: Research artifact metadata
            body: Research artifact body
            output_dir: Where to save angle artifacts
            category_priorities: Optional dict from CategoryTracker.get_priorities()
                - hungry_categories: categories needing content
                - saturated_categories: categories to avoid (at quota)

        Returns list of (metadata, body) tuples for each angle.
        """
        import anthropic

        valid, error = self.validate_input(metadata, body)
        if not valid:
            raise ValueError(f"[{self.module_name}] Input validation failed: {error}")

        site_context = self.loader.load(metadata.get("site_id", ""))

        # Check if expansion is enabled for this site
        expansion_config = site_context.raw_config.get("expansion", {})
        if not expansion_config.get("enabled", False):
            logger.info(f"[{self.module_name}] Expansion disabled for {metadata.get('site_id')}, passing through")
            # Return the input as-is (no expansion)
            return [(metadata, body)]

        system_prompt, user_message = self.build_prompt(
            metadata, body, site_context, category_priorities
        )

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

                angles = self.parse_response_multi(
                    response_text, metadata, body, site_context
                )

                # Validate all angles
                valid_angles = []
                for angle_meta, angle_body in angles:
                    valid, error = self.validate_output(angle_meta, angle_body)
                    if valid:
                        if output_dir:
                            save_artifact(angle_meta, angle_body, output_dir)
                        valid_angles.append((angle_meta, angle_body))
                        logger.info(f"[{self.module_name}] ✅ {angle_meta.get('article_id')}")
                    else:
                        logger.warning(f"[{self.module_name}] ❌ Invalid angle: {error}")

                if valid_angles:
                    return valid_angles
                else:
                    raise ValueError("No valid angles generated")

            except anthropic.APIError as e:
                logger.error(f"[{self.module_name}] API error attempt {attempt}: {e}")
                if attempt == self.max_retries:
                    raise
                import time
                time.sleep(2 ** attempt)

        raise RuntimeError(f"[{self.module_name}] Failed after {self.max_retries} attempts")


# ── CLI ─────────────────────────────────────────────────────

if __name__ == "__main__":
    ExpansionModule.cli()
