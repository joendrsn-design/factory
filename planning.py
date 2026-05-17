"""
============================================================
ARTICLE FACTORY — PLANNING MODULE
============================================================
Takes research artifacts → produces article plans.

Model: Haiku (structural work, cost-effective)
Input folder: pipeline/research/
Output folder: pipeline/plans/

Anti-Slop Structural Discipline:
  - Banned title/heading patterns (clickbait, manufactured stakes)
  - Voice-conditional structure hints (philosophy vs clinical vs trading)
  - Section purpose validation (no "hook the reader" purposes)
  - Word count defaults to middle of range, not maximum

Template Dispatch:
  Article types can specify a structure_template in their config.
  If present, planning uses a specialized template instead of the
  default prompt. Templates live in the templates registry.

  DONE: Anti-slop changes applied to StatsDrivenV1.py template.
  Added: banned title/heading patterns, structural_notes guidance,
  word count discipline (default to middle, not max).

Usage:
    python planning.py submit --input pipeline/research
    python planning.py collect --input pipeline/research --output pipeline/plans
    python planning.py run --input pipeline/research --output pipeline/plans --limit 1
============================================================
"""

import json
import logging
import re
from typing import Optional

from base_module import BaseModule
from site_loader import SiteContext
from artifacts import plan_metadata, new_article_id, save_artifact

logger = logging.getLogger("article_factory.planning")


# ── Template Registry ────────────────────────────────────────
# Maps structure_template names to their config modules.
# Each template provides: model, system_prompt, build_messages,
# validate_plan, render_artifact

def _load_template_registry() -> dict:
    """Lazily load templates to avoid circular imports."""
    registry = {}
    try:
        from StatsDrivenV1 import get_template_config as stats_config
        registry["stats_driven_v1"] = stats_config()
        logger.info("[planning] Loaded template: stats_driven_v1")
    except ImportError as e:
        logger.debug(f"[planning] stats_driven_v1 template not available: {e}")
    return registry


TEMPLATE_REGISTRY: dict = None  # Lazy-loaded


class PlanningModule(BaseModule):

    module_name = "planning"
    model = "claude-haiku-4-5-20251001"
    input_module = "research"
    max_retries = 2
    default_max_tokens = 4096

    def __init__(self, config_dir: str = "config/sites"):
        super().__init__(config_dir=config_dir)
        global TEMPLATE_REGISTRY
        if TEMPLATE_REGISTRY is None:
            TEMPLATE_REGISTRY = _load_template_registry()

    def _get_template(self, article_type: dict) -> Optional[dict]:
        """Get template config if article_type specifies a structure_template."""
        template_name = article_type.get("structure_template", "")
        if template_name and template_name in TEMPLATE_REGISTRY:
            return TEMPLATE_REGISTRY[template_name]
        return None

    def _get_link_candidates(
        self,
        site_id: str,
        topic: str,
        title: str,
        limit: int = 10,
    ) -> str:
        """
        Get semantically similar articles for internal linking (P6).
        Returns formatted text for prompt injection.
        Falls back to 'No internal link candidates available.' on error.
        """
        try:
            from linking.recommender import LinkRecommender
            recommender = LinkRecommender()
            candidates = recommender.get_candidates(
                site_id=site_id,
                topic=topic,
                title=title,
                limit=limit,
            )
            if candidates:
                return recommender.format_for_prompt(candidates)
            return "No internal link candidates available."
        except Exception as e:
            logger.warning(f"[planning] Link recommender failed: {e}")
            return "No internal link candidates available."

    def _get_link_candidates_raw(
        self,
        site_id: str,
        topic: str,
        title: str,
        limit: int = 10,
    ) -> list[dict]:
        """
        Get semantically similar articles for internal linking (P6).
        Returns raw list of candidate dicts for template use.
        """
        try:
            from linking.recommender import LinkRecommender
            recommender = LinkRecommender()
            candidates = recommender.get_candidates(
                site_id=site_id,
                topic=topic,
                title=title,
                limit=limit,
            )
            return [
                {
                    "slug": c.slug,
                    "title": c.title,
                    "url": c.url,
                    "similarity": c.similarity,
                    "is_hub": c.is_hub,
                    "anchors": c.anchors,
                }
                for c in candidates
            ]
        except Exception as e:
            logger.warning(f"[planning] Link recommender failed: {e}")
            return []

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

        # P6: Get semantic link candidates from recommender
        link_candidates_text = self._get_link_candidates(
            site_id=site_context.site_id,
            topic=metadata.get("topic", ""),
            title=metadata.get("title", metadata.get("topic", "")),
        )

        # Voice exemplars block (Change 2)
        exemplar_block = ""
        if site_context.voice_exemplars:
            exemplar_block = f"""
=== VOICE EXEMPLARS (target prose for this site) ===
{site_context.voice_exemplars.strip()}

The structure you produce should support prose like this. Notice the cadence,
section length, and density. Plan accordingly — short contemplative pieces
need different scaffolding than evidence-dense clinical pieces.
"""

        # Voice-conditional structure hints (Change 3)
        niche = site_context.niche.lower()
        structure_hint = ""

        if any(k in niche for k in ["philosophy", "stoic", "contemplat", "spiritual"]):
            structure_hint = """
STRUCTURE NOTE — Contemplative/Philosophical:
- Short articles (300-600 words). Resist padding.
- 2-3 sections maximum. Often a single flowing piece works better than headed sections.
- For very short pieces, you may return a single section with no heading.
- Section purpose is "deliver an idea" not "cover a topic area."
- Do not include action items, takeaways, or summary sections.
"""
        elif any(k in niche for k in ["medical", "clinical", "pathology"]):
            structure_hint = """
STRUCTURE NOTE — Clinical/Medical:
- Standard structure: Question/Problem → Mechanism → Evidence → Application → Safety/Caveats.
- Every section must map to specific cited sources from the research.
- Safety/contraindication content goes in its own section, not as a footnote.
- Do not use "What is X?" or "Understanding X" as opening sections — go straight to the clinical question.
"""
        elif any(k in niche for k in ["trading", "finance", "invest", "market"]):
            structure_hint = """
STRUCTURE NOTE — Trading/Finance:
- Standard structure: Setup/Thesis → Context → Technical Detail → Risk Management → Application.
- Risk management is its own section, never a parenthetical.
- Specific levels, prices, and dates belong in the plan, not as vague "discuss risk" purposes.
- Do not use "Why This Matters" or "What This Means For You" as section headings.
"""
        elif any(k in niche for k in ["health", "longevity", "wellness", "supplement"]):
            structure_hint = """
STRUCTURE NOTE — Health/Wellness DTC:
- Standard structure: Question → Evidence → Mechanism (simplified) → Practical Application → What to Watch For.
- Evidence section must reference specific studies from research, not "research suggests."
- Practical application section gives concrete guidance, not generic "consult your doctor" filler.
"""

        # Word count range
        word_min = article_type.get('word_count_min', 800)
        word_max = article_type.get('word_count_max', 2000)
        word_mid = (word_min + word_max) // 2

        system = f"""You are the Planning module of an automated article factory.
Your job: create a detailed article outline from the research provided.

SITE: {site_context.site_name} ({site_context.niche})
AUDIENCE: {audience.get('profile', 'General')} | Expertise: {audience.get('expertise_level', 'mixed')}
VOICE: {voice.get('tone', 'professional')} | Persona: {voice.get('persona', 'knowledgeable writer')}
{exemplar_block}
POV: {voice.get('pov', 'third_person')} | Reading level: {voice.get('reading_level', 'grade_10')}

ARTICLE TYPE: {article_type.get('label', 'Article')}
{article_type.get('description', '')}
Word count range: {word_min}-{word_max} (default to {word_mid}, the middle)
Structure: {json.dumps(article_type.get('structure', []))}
Citations required: {article_type.get('citation_required', False)}

SEO KEYWORDS: {json.dumps(site_context.seo.get('primary_keywords', [])[:5])}

INTERNAL LINK CANDIDATES (pick 3-5, vary anchor text):
{link_candidates_text}

AVOID: {json.dumps(voice.get('avoid', []))}

CATEGORIES (pick the BEST fit):
{category_list}

=== STRUCTURAL DISCIPLINE ===

The plan you produce shapes the article. Slop-shaped plans produce slop articles
even when the writer tries to avoid it. Apply these rules:

TITLES:
- State what the article is about, declaratively.
- Include the primary keyword naturally — do not force it.
- Banned title patterns:
  - "5 [Adjective] [Things] About X"
  - "What Nobody Tells You About X"
  - "The [Hidden/Surprising/Shocking] Truth About X"
  - "Why X Will [Change/Transform/Revolutionize] Y"
  - Title-case clickbait phrases like "You Won't Believe..."
  - Question titles that imply a gotcha answer
- Good: "GLP-1 receptor agonists in older adults: dosing and safety"
- Good: "What Marcus Aurelius wrote about anger"
- Good: "Reading the McClellan Oscillator for SPY entries"
- Bad: "5 Shocking Things About GLP-1 Drugs"
- Bad: "The Stoic Secret to Anger Management"
- Bad: "The McClellan Trick Wall Street Doesn't Want You to Know"

SECTION HEADINGS:
- Headings name what the section covers. They do not tease, hook, or reveal.
- Good: "How GLP-1 agonists affect resting metabolic rate"
- Bad: "The Metabolic Truth Big Pharma Won't Tell You"
- Good: "Dosing considerations for older adults"
- Bad: "What Nobody Tells You About Dosing"

STRUCTURE PATTERNS TO AVOID:
- Problem-Agitation-Solution (the copywriter skeleton)
- "Here's the surprising thing" reveal structures
- Listicles padded with throat-clearing sections
- Three-act emotional arcs (setup → tension → resolution)
- Any structure where Section 1's purpose is "Hook the reader"

WHAT GOOD STRUCTURE LOOKS LIKE:
- Each section answers a specific question or covers a specific aspect
- Sections build cumulatively, not narratively
- The strongest concrete content is at the end, not buried
- Word count is allocated to depth, not to ramping up
- "Purpose" fields describe what the reader learns, not what the reader feels

OPENING SECTIONS:
- The first section establishes the subject directly. It does not "hook."
- For evidence-based content: state the question or problem in clinical terms.
- For contemplative content: anchor in the idea, not in a manufactured scene.
- For analytical content: lead with the thesis, not with a teaser.

CLOSING SECTIONS:
- The final section delivers the most actionable or substantive content.
- It does NOT summarize what was already said.
- It does NOT issue a call to action unless the article type explicitly requires one.
- "Conclusion" or "Final Thoughts" headings are banned. Name the section by content.

WORD COUNT:
- Default to the MIDDLE of the range ({word_mid}), not the maximum.
- Only target the maximum when the topic genuinely requires it (complex evidence,
  multiple competing positions, technical depth).
- For contemplative or simple-question articles, target the minimum.
- Allocate words to substance, not to throat-clearing or summary.
- If you can't justify a word count with specific content, lower it.

{structure_hint}

Respond with ONLY valid JSON (no markdown fences, no explanation):
{{
    "title": "Direct, descriptive article title (include primary keyword naturally)",
    "slug": "url-friendly-slug",
    "category": "best-fit-category-slug",
    "seo_title": "SEO-optimized title for search results (50-60 chars, primary keyword near front)",
    "meta_description": "Meta description summarizing article content (150-160 chars, primary keyword once)",
    "target_keywords": ["primary", "secondary"],
    "target_word_count": {word_mid},
    "internal_links": ["relevant link targets"],
    "outline": [
        {{
            "section_id": "s1",
            "heading": "Section Heading",
            "purpose": "What the reader learns from this section",
            "key_points": ["point 1", "point 2"],
            "target_words": 200,
            "sources_to_cite": ["source title"]
        }}
    ]
}}

RULES:
1. Follow the structure template for this article type
2. Distribute word count to hit target (prefer middle of range)
3. Map research sources to sections where they belong
4. Every section needs a clear purpose — no filler
5. Pick the single BEST category from the list above — must be exact slug
6. Section purposes describe what the reader LEARNS, not what they FEEL"""

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

        # Carry sources and statistics forward from research
        meta["sources"] = input_metadata.get("sources", [])
        meta["statistics"] = input_metadata.get("statistics", [])

        # Store structured outline as first-class metadata (Change 4)
        meta["outline"] = outline

        # Word count sanity check (Change 7)
        article_type_id = input_metadata.get("article_type", "")
        article_type = site_context.get_article_type(article_type_id)
        if article_type:
            type_max = article_type.get("word_count_max", 2000)
            if plan.get("target_word_count", 0) == type_max:
                logger.warning(f"[planning] Plan targeted maximum word count ({type_max}) — verify this is justified")

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

        # Body is pure markdown for human review — no embedded JSON (Change 4)
        # Structured data lives in meta["outline"]

        body = "\n".join(body_lines)

        return meta, body

    # ── Template-Aware Processing ────────────────────────────

    def run_single(
        self,
        metadata: dict,
        body: str,
        output_dir: str = "",
    ) -> tuple[dict, str]:
        """
        Process a single artifact through planning.
        If article_type has a structure_template, dispatch to that template.
        Otherwise, use the default planning logic.
        """
        import anthropic

        valid, error = self.validate_input(metadata, body)
        if not valid:
            raise ValueError(f"[{self.module_name}] Input validation failed: {error}")

        site_id = metadata.get("site_id", "")
        site_context = self.loader.load(site_id)

        article_type_id = metadata.get("article_type", "")
        article_type = site_context.get_article_type(article_type_id)
        if not article_type:
            article_type = site_context.get_enabled_article_types()[0]

        template = self._get_template(article_type)

        if template:
            # Use template's specialized logic
            return self._run_with_template(
                template, metadata, body, site_context, article_type, output_dir
            )
        else:
            # Use default planning logic (parent class behavior)
            return super().run_single(metadata, body, output_dir)

    def _run_with_template(
        self,
        template: dict,
        metadata: dict,
        body: str,
        site_context: SiteContext,
        article_type: dict,
        output_dir: str,
    ) -> tuple[dict, str]:
        """Run planning using a specialized template."""
        import anthropic

        logger.info(f"[planning] Using template: {template['template_id']}")

        # Build site context dict for template (Change 10: enriched voice context)
        site_ctx = {
            "site_id": site_context.site_id,
            "site_name": site_context.site_name,
            "domain": site_context.domain,
            "audience": site_context.audience.get("profile", "general"),
            "voice_summary": site_context.voice.get("tone", "professional"),
            "voice_persona": site_context.voice.get("persona", ""),
            "voice_avoid": site_context.voice.get("avoid", []),
            "voice_exemplars": site_context.voice_exemplars,  # For template anti-slop guidance
            "niche": site_context.niche,  # Lets template apply voice-conditional logic
            "content_pillars": [c.get("label", "") for c in site_context.categories],
        }

        # Build research artifact dict for template
        research_artifact = {
            "statistics": metadata.get("statistics", []),
            "sources": metadata.get("sources", []),
            "summary": body[:3000],
        }

        # Build topic artifact dict for template
        topic_artifact = {
            "topic": metadata.get("topic", ""),
            "data_year": metadata.get("data_year", 2026),
            "anchor_urls": metadata.get("anchor_urls", []),
            "primary_keyword": metadata.get("primary_keyword", metadata.get("topic", "")),
        }

        # P6: Get semantic link candidates for template
        link_candidates = self._get_link_candidates_raw(
            site_id=site_context.site_id,
            topic=metadata.get("topic", ""),
            title=metadata.get("title", metadata.get("topic", "")),
        )

        # Get messages from template
        build_messages = template["build_messages"]
        messages = build_messages(
            site_ctx,
            topic_artifact,
            research_artifact,
            article_type,
            internal_link_candidates=link_candidates,
        )

        # Call LLM with template's model and system prompt
        client = anthropic.Anthropic()
        response = client.messages.create(
            model=template.get("model", self.model),
            max_tokens=template.get("max_tokens", self.default_max_tokens),
            temperature=template.get("temperature", 0.4),
            system=template["system_prompt"],
            messages=messages,
        )

        response_text = ""
        for block in response.content:
            if block.type == "text":
                response_text += block.text

        # Parse JSON plan
        text = response_text.strip()
        try:
            plan = json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse template plan JSON: {e}\nRaw: {text[:500]}")

        # Validate plan references real stat_ids and source_ids
        validate_plan = template.get("validate_plan")
        if validate_plan:
            try:
                validate_plan(
                    plan,
                    metadata.get("statistics", []),
                    metadata.get("sources", []),
                )
            except Exception as e:
                raise ValueError(f"Plan validation failed: {e}")

        # Render artifact using template's renderer
        render_artifact = template.get("render_artifact")
        if render_artifact:
            artifact_body = render_artifact(
                plan,
                site_context.site_id,
                metadata.get("topic", ""),
            )
        else:
            artifact_body = json.dumps(plan, indent=2)

        # Build metadata
        fm = plan.get("frontmatter", {})
        meta = plan_metadata(
            run_id=metadata.get("run_id", ""),
            article_id=metadata.get("article_id", new_article_id()),
            site_id=metadata.get("site_id", ""),
            article_type=metadata.get("article_type", ""),
            topic=metadata.get("topic", ""),
            title=fm.get("title", "Untitled"),
            slug=fm.get("slug", "untitled"),
            target_word_count=fm.get("target_word_count", 2400),
            seo_title=fm.get("seo_title", ""),
            meta_description=fm.get("meta_description", ""),
            target_keywords=[fm.get("primary_keyword", "")] + fm.get("secondary_keywords", []),
            internal_links=[],
            section_count=len(plan.get("sections", [])),
        )

        # Carry forward data for downstream modules
        meta["sources"] = metadata.get("sources", [])
        meta["statistics"] = metadata.get("statistics", [])
        meta["structure_template"] = template["template_id"]
        meta["plan"] = plan  # Full structured plan for Write module

        if output_dir:
            save_artifact(meta, artifact_body, output_dir)

        logger.info(f"[planning] ✅ Template plan: {meta.get('title', 'Untitled')}")
        return meta, artifact_body

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
        """Plan must have title, slug, enough sections, and no slop patterns."""
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

        # Change 5: Check for slop-shaped section purposes and headings
        outline = metadata.get("outline", [])

        BANNED_PURPOSE_PATTERNS = [
            r"\bhook\s+the\s+reader\b",
            r"\bgrab\s+attention\b",
            r"\btease\b",
            r"\bbuild\s+suspense\b",
            r"\bsurprising\b",
            r"\bshocking\b",
            r"\bwhat\s+nobody\s+(knows|realizes|tells)\b",
            r"\bwhat\s+most\s+people\s+(don'?t|miss|overlook)\b",
            r"\bset\s+up\s+the\s+reveal\b",
        ]

        BANNED_HEADING_PATTERNS = [
            r"^conclusion$",
            r"^final\s+thoughts$",
            r"^wrapping\s+up$",
            r"^the\s+(hidden|shocking|surprising|real)\s+truth\b",
            r"^what\s+(nobody|most\s+people)\b",
            r"^why\s+.+\s+matters$",
            r"^the\s+bottom\s+line$",
        ]

        issues = []
        for i, section in enumerate(outline):
            purpose = section.get("purpose", "").lower()
            heading = section.get("heading", "").lower()

            for pattern in BANNED_PURPOSE_PATTERNS:
                if re.search(pattern, purpose):
                    issues.append(f"Section {i+1} purpose contains slop pattern: '{pattern}'")

            for pattern in BANNED_HEADING_PATTERNS:
                if re.search(pattern, heading):
                    issues.append(f"Section {i+1} heading is slop-shaped: '{section.get('heading')}'")

        if issues:
            return False, f"Plan has slop-shaped structure: {'; '.join(issues[:3])}"

        return True, ""


# ── CLI ─────────────────────────────────────────────────────

if __name__ == "__main__":
    PlanningModule.cli()
