"""
============================================================
ARTICLE FACTORY — WRITE MODULE
============================================================
Module 3: Takes plan artifacts → produces finished articles.

Model: Opus 4.7 (voice fidelity, prose quality — don't cheap out)
Input folder: pipeline/plans/
Output folder: pipeline/articles/

This is the most important module. Voice is everything.
A LampHill article reads like a physician-researcher explaining
evidence to an informed patient. A Daily Marcus Aurelius piece
reads like morning stillness. The Inside Bar reads like a trader
who's been in the trenches. Same factory, completely different feel.

Voice Strategy:
  - Medical sites: clinical authority, softened jargon, citations
    required, evidence-first structure, risk/safety language
  - DTC sites: authoritative yet approachable, benefit-driven,
    citations required, builds trust through evidence
  - Philosophy sites: contemplative, minimal, no citations needed,
    questions that linger
  - Trading sites: direct, actionable, risk disclaimers, chart-aware

Citation Handling:
  - Sites with citation_required=true get inline references
  - References section appended at end of article
  - Sources from research artifact mapped to claims
  - Format: [1], [2] inline → numbered list at bottom

Usage:
    python write.py submit --input pipeline/plans
    python write.py collect --input pipeline/plans --output pipeline/articles
    python write.py run --input pipeline/plans --output pipeline/articles --limit 1
============================================================
"""

import json
import os
import re
import logging
from typing import Optional

from base_module import BaseModule
from site_loader import SiteContext
from artifacts import (
    article_metadata, new_article_id,
    load_artifacts_from_dir, load_artifact,
)

# P7: Image Pipeline (optional - graceful fallback if Unsplash not configured)
try:
    from media.pipeline import ImagePipeline
    IMAGE_PIPELINE_AVAILABLE = True
except ImportError:
    IMAGE_PIPELINE_AVAILABLE = False

logger = logging.getLogger("article_factory.write")


# ── Visual Block Markdown Fallbacks ─────────────────────────
# For stats_driven articles, charts/keystats/tables are rendered as
# fenced blocks. Site Empire components replace these with interactive
# visuals. Until then, emit markdown-readable versions as fallbacks.

def _add_visual_fallbacks(body: str) -> str:
    """
    Post-process article body to add markdown fallbacks after visual blocks.

    For each ```chart, ```keystat, or ```table fenced block, emit a
    markdown-readable version immediately after. Site Empire components
    will hide the fallback when they render the interactive version.
    """
    import json

    # Pattern to match visual fenced blocks
    pattern = r'```(chart|keystat|table)\n(.*?)```'

    def replace_with_fallback(match):
        block_type = match.group(1)
        content = match.group(2).strip()
        original = match.group(0)

        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            # If not valid JSON, return original unchanged
            return original

        fallback = ""

        if block_type == "chart":
            # Chart fallback: description + data points as list
            title = data.get("title", "Chart")
            description = data.get("description", "")
            stat_ids = data.get("stat_ids", [])
            fallback = f"\n\n<!-- chart-fallback -->\n**{title}**"
            if description:
                fallback += f"\n_{description}_"
            if stat_ids:
                fallback += f"\n_Data: {', '.join(stat_ids)}_"
            fallback += "\n<!-- /chart-fallback -->\n"

        elif block_type == "keystat":
            # Key stat callout fallback: bold stat with framing
            stat_id = data.get("stat_id", "")
            framing = data.get("framing", "")
            value = data.get("value", "")
            fallback = f"\n\n<!-- keystat-fallback -->\n> **{value}** — {framing}\n<!-- /keystat-fallback -->\n"

        elif block_type == "table":
            # Table fallback: markdown table
            title = data.get("title", "")
            columns = data.get("columns", [])
            rows = data.get("rows", [])

            if columns:
                fallback = f"\n\n<!-- table-fallback -->\n"
                if title:
                    fallback += f"**{title}**\n\n"
                # Header row
                fallback += "| " + " | ".join(str(c) for c in columns) + " |\n"
                fallback += "| " + " | ".join("---" for _ in columns) + " |\n"
                # Data rows
                for row in rows:
                    if isinstance(row, list):
                        fallback += "| " + " | ".join(str(cell) for cell in row) + " |\n"
                    elif isinstance(row, dict):
                        fallback += "| " + " | ".join(str(row.get(c, "")) for c in columns) + " |\n"
                fallback += "<!-- /table-fallback -->\n"

        return original + fallback

    return re.sub(pattern, replace_with_fallback, body, flags=re.DOTALL)


# ── Voice Profiles ──────────────────────────────────────────
# These are deep prompt fragments that go beyond the site config's
# tone/persona fields. They encode HOW to write, not just WHAT tone.
# Keyed by site tier + niche pattern. Falls back to site config voice.

VOICE_PROFILES = {
    # ── Medical / Clinical Sites ────────────────────────────
    "medical": """
VOICE RULES — MEDICAL/CLINICAL:
You are writing as a physician-researcher. This is NOT consumer health content 
written by a copywriter. This is a clinician translating evidence for an informed audience.

AUTHORITY MARKERS:
- Lead with evidence, not opinion. "A 2024 RCT demonstrated..." not "Many experts believe..."
- Use precise clinical language, then immediately clarify: "bioavailability (how much your body actually absorbs)"
- Reference specific study designs: RCT, meta-analysis, cohort study — your audience respects rigor
- Include effect sizes and confidence intervals when available, not just "studies show"
- When evidence is mixed or limited, say so explicitly. Never overstate.

CLINICAL TONE:
- Warm but never casual. You're the doctor patients trust, not their gym buddy.
- "The evidence suggests" > "Research shows" > "Studies prove" (never use "prove")
- Use "may," "appears to," "the data indicate" — appropriate epistemic humility
- Address safety and contraindications proactively. A physician always does.
- Never use superlatives about supplements: no "miracle," "breakthrough," "game-changer"

STRUCTURE:
- Clinical articles follow: What → Why (mechanism) → Evidence → Application → Safety
- Every major claim needs a source. Unsourced claims undermine physician authority.
- Include a practical takeaway section — what should the reader actually DO?
""",

    # ── Direct-to-Consumer Health/Wellness ──────────────────
    "dtc_health": """
VOICE RULES — DTC HEALTH/WELLNESS:
You are writing for health-conscious consumers who want evidence-based guidance 
they can act on. Authoritative yet approachable. Trust through transparency.

AUTHORITY MARKERS:
- Cite research to build trust: "According to a 2024 study in the Journal of Sleep Research..."
- Reference specific numbers: "participants saw a 20% improvement" not "significant improvement"
- Acknowledge what we don't know. Consumers respect honesty over hype.
- Position the reader as an informed decision-maker, not a patient being prescribed to.

DTC TONE:
- Conversational but substantive. Like explaining to a smart friend over coffee.
- Use "you" freely — this is personal, actionable content
- Warm, encouraging, never preachy or alarmist
- OK to use occasional colloquialisms if they serve clarity
- Never talk down to the reader. They're smart; they just need the information organized.

STRUCTURE:
- Hook with relevance: why should the reader care right now?
- Build understanding: what is this, how does it work (simply)
- Present evidence: what does the research actually say?
- Make it actionable: dosing, timing, what to look for, what to avoid
- Close with empowerment: the reader is equipped to make their own call

PRODUCT MENTIONS:
- If product integration is enabled, mention naturally in context
- Never force a product mention. If it doesn't fit, skip it.
- Frame as "one option worth considering" not "the answer"
- Max one CTA per article. Soft sell, never hard sell.
""",

    # ── Philosophy / Contemplative ──────────────────────────
    "philosophy": """
VOICE RULES — PHILOSOPHICAL/CONTEMPLATIVE:
You are a quiet guide, not a lecturer. Each piece should feel like 
a moment of stillness in the reader's day.

TONE:
- Meditative, unhurried. Let ideas breathe.
- Short sentences have power. Use them.
- Pose questions that linger past the reading. Don't always answer them.
- Ground ancient wisdom in modern moments — commutes, arguments, quiet mornings
- Never academic. Never preachy. Never self-help cliché.
- "Consider" > "You should" > "Remember to"

STRUCTURE:
- Anchor in a quote, idea, or moment
- Reflect — what does this mean for a life lived today?
- Close with a thought, question, or image — not a summary
- Short. Every word earns its place. 300-600 words for daily reflections.

WHAT TO AVOID:
- Productivity framing ("Stoicism for peak performance!")
- Self-help language ("5 ways Marcus Aurelius can change your life!")
- Historical lectures (the reader doesn't need a biography)
- Forced relevance — if the connection is thin, let the philosophy stand alone
""",

    # ── Trading / Finance ───────────────────────────────────
    "trading": """
VOICE RULES — TRADING/FINANCE:
You are a seasoned trader who teaches. Direct, no-BS, respects the reader's 
time and money. Every word should have a purpose.

TONE:
- Direct and actionable. Traders don't have patience for fluff.
- Confident but honest about uncertainty. Markets humble everyone.
- "The setup suggests" not "this WILL move"
- Include risk framing naturally: "if this level breaks, the thesis is invalidated"
- Respect that the reader is risking real money on this information

STRUCTURE:
- Lead with the setup or thesis
- Context: what's happening in the market and why it matters
- Technical detail: levels, patterns, indicators — be specific
- Risk management: always. Every article. Non-negotiable.
- Actionable takeaway: what the reader can DO with this

LEGAL:
- Include standard risk disclaimer language
- Never give specific financial advice ("you should buy X")
- Frame as education and analysis, not recommendations
- "This is not financial advice" must be present
""",
}


def get_voice_profile(site_context: SiteContext) -> str:
    """
    Select the appropriate deep voice profile based on site characteristics.
    Falls back to building from site config if no profile matches.
    """
    niche = site_context.niche.lower()
    tier = site_context.tier
    sub_niche = (site_context.sub_niche or "").lower()

    # Medical sites
    if any(k in niche for k in ["medical", "clinical", "pathology", "diagnostics", "laboratory"]):
        return VOICE_PROFILES["medical"]

    # DTC health/wellness/longevity
    if any(k in niche for k in ["health", "longevity", "wellness", "supplement", "nutrition"]):
        return VOICE_PROFILES["dtc_health"]

    # Philosophy/contemplative
    if any(k in niche for k in ["philosophy", "stoic", "contemplat", "meditation", "spiritual"]):
        return VOICE_PROFILES["philosophy"]

    # Trading/finance
    if any(k in niche for k in ["trading", "finance", "invest", "market", "stocks"]):
        return VOICE_PROFILES["trading"]

    # Fallback: build from site config
    voice = site_context.voice
    return f"""
VOICE RULES:
Tone: {voice.get('tone', 'professional')}
Persona: {voice.get('persona', 'knowledgeable writer')}
POV: {voice.get('pov', 'third_person')}
Reading level: {voice.get('reading_level', 'grade_10')}
Style notes: {voice.get('style_notes', '')}
Avoid: {', '.join(voice.get('avoid', []))}
"""


# ── Citation Formatter ──────────────────────────────────────

def _get_citation_persona(site_context: SiteContext) -> str:
    """
    Get the citation persona based on site niche.
    This determines how the writer frames their authority when citing.
    """
    niche = site_context.niche.lower()

    if any(k in niche for k in ["medical", "clinical", "pathology", "diagnostics", "laboratory"]):
        return "physician-researcher"
    if any(k in niche for k in ["health", "longevity", "wellness", "supplement", "nutrition"]):
        return "health researcher"
    if any(k in niche for k in ["trading", "finance", "invest", "market"]):
        return "market analyst"
    if any(k in niche for k in ["tech", "software", "programming", "developer"]):
        return "technical writer"
    if any(k in niche for k in ["legal", "law", "attorney"]):
        return "legal analyst"

    # Fallback to site voice persona
    return site_context.voice.get("persona", "subject-matter expert")


def build_citation_instructions(site_context: SiteContext, article_type: dict, sources: list) -> str:
    """
    Build citation instructions for the writing prompt.
    Only included when the article type requires citations.
    """
    if not article_type.get("citation_required", False):
        return "\nCITATIONS: Not required for this article type. Write naturally without inline references."

    if not sources:
        return "\nCITATIONS: Required but no sources provided. Use your knowledge and note claims that should be sourced."

    persona = _get_citation_persona(site_context)

    source_list = []
    for i, src in enumerate(sources, 1):
        title = src.get("title", f"Source {i}")
        url = src.get("url", "")
        source_list.append(f"  [{i}] {title} — {url}")

    sources_block = "\n".join(source_list)

    return f"""
CITATION REQUIREMENTS:
This article REQUIRES inline citations. You are writing as a {persona}.
Uncited claims undermine credibility.

AVAILABLE SOURCES:
{sources_block}

CITATION FORMAT:
- Inline: Use [1], [2], etc. after claims that reference specific evidence
- Every major factual claim, statistic, or study result MUST have a citation
- You may cite multiple sources for one claim: [1, 2]
- General knowledge statements don't need citations
- When paraphrasing, still cite the source

REFERENCES SECTION:
At the end of the article, include a "## References" section listing all cited sources:
1. Source Title. URL
2. Source Title. URL

RULES:
- Cite at least 3 sources in the article body
- Don't fabricate sources — only cite from the available sources list above
- If making a claim that none of the available sources support, note it needs verification
- Quality over quantity — cite where it matters, not every sentence
"""


# ── Write Module ────────────────────────────────────────────

class WriteModule(BaseModule):

    module_name = "write"
    model = "claude-opus-4-7"  # Default, can be overridden per article type
    input_module = "planning"
    max_retries = 2
    default_max_tokens = 8192

    def get_model(self, metadata: dict, site_context: SiteContext) -> str:
        """
        Get the model to use for this article.
        Article types can specify model_override to use a cheaper model for simpler content.
        E.g., quick_guide can use Haiku instead of Opus for ~49¢ savings per article.
        """
        article_type_id = metadata.get("article_type", "")
        article_type = site_context.get_article_type(article_type_id)

        if article_type and article_type.get("model_override"):
            model = article_type["model_override"]
            logger.info(f"[write] Using model override: {model} (article type: {article_type_id})")
            return model

        return self.model

    # ── Prompt Construction ─────────────────────────────────

    def build_prompt(
        self,
        metadata: dict,
        body: str,
        site_context: SiteContext,
    ) -> tuple[str, str]:
        """Build the writing prompt. This is where voice lives or dies."""

        article_type_id = metadata.get("article_type", "")
        article_type = site_context.get_article_type(article_type_id)
        if not article_type:
            article_type = site_context.get_enabled_article_types()[0]

        voice = site_context.voice
        audience = site_context.audience

        # Get the deep voice profile
        voice_profile = get_voice_profile(site_context)

        # Get citation instructions (sources come from metadata only — no body parsing)
        sources = metadata.get("sources", [])
        citation_instructions = build_citation_instructions(site_context, article_type, sources)

        # Product integration rules
        product_block = ""
        products = site_context.products
        if products.get("enabled"):
            owned = products.get("owned_products", [])
            if owned:
                product_list = "\n".join(
                    f"  - {p['name']}: mention in {p.get('context', 'relevant')} articles → {p.get('url', '')}"
                    for p in owned
                )
                product_block = f"""
PRODUCT INTEGRATION:
{products.get('mention_rules', 'Mention naturally when relevant. Never force.')}

Available products:
{product_list}

Only mention a product if it is genuinely relevant to this specific article's topic.
"""

        # Word count
        word_min = article_type.get("word_count_min", 800)
        word_max = article_type.get("word_count_max", 2000)
        target = metadata.get("target_word_count", word_min)

        system = f"""You are the Write module of an automated article factory.
Your job: produce a complete, publish-ready article in markdown format.

SITE: {site_context.site_name}
DOMAIN: {site_context.domain}
NICHE: {site_context.niche}

AUDIENCE:
- Profile: {audience.get('profile', 'General audience')}
- Expertise: {audience.get('expertise_level', 'mixed')}
- Intent: {audience.get('intent', 'learn')}

ARTICLE TYPE: {article_type.get('label', 'Article')}
{article_type.get('description', '')}

WORD COUNT TARGET: {target} words (range: {word_min}-{word_max})
This is important. Hit the target. Don't pad. Don't truncate.

{voice_profile}

{self._build_exemplar_block(site_context)}

ADDITIONAL STYLE NOTES FROM SITE CONFIG:
{voice.get('style_notes', 'None')}

THINGS TO ABSOLUTELY AVOID:
{json.dumps(voice.get('avoid', []))}

=== HARD BANS (NEVER USE THESE EXACT PATTERNS) ===
1. "It's not about X. It's about Y" / "This isn't X; it's Y" / "Not X, but Y"
2. "turning X into Y" / "transforms X into Y" / "X becomes Y"
3. "What nobody tells you..." / "The hard truth is..." / "Let that sink in."
4. "Here's the thing:" / "Make no mistake:" / "Full stop."
5. "most people don't realize" / "almost nobody is paying attention"
6. "It's worth noting..." / "Worth keeping in mind..." / "Interestingly,"
7. "Furthermore," / "Moreover," / "Additionally," / "That being said,"
8. "I want to be clear..." / "Let me be clear..." / "Let me say that again."
9. "Discover how..." / "uncover the secrets" / "reveal" (breathless discovery)
10. "Pro tip:" / "Hot take:" / "Plot twist:" / "Here's the kicker:"
11. Round-number forecasts without derivation ("1 billion new investors")
12. "structural shift" / "reprices" / "asymmetric" (without adjacent mechanism)
13. "We need to talk about..." / "unhinged" / "lands" (as engagement bait)
14. "Whether you're X, Y, or Z" openings / tricolons in consecutive paragraphs
15. LLM closing pattern: restatement + round-number prediction + rhetorical question + CTA

=== WRITING PRINCIPLES ===
1. DEFAULT TO DECLARATIVE — Make claims directly. Never frame with "It's not X, it's Y."
2. EARN ABSTRACTIONS — Words like "structural" require an adjacent number, citation, or mechanism.
3. SHOW, DON'T FRAME — Never tell readers they're seeing something others miss. Present the evidence.
4. SOURCE ALL NUMBERS — No round forecasts. Expert quotes need date/venue.
5. VARY STRUCTURE — No back-to-back antitheses, tricolons, or rhetorical questions.

=== VOICE DISCIPLINE ===
Your job is to channel the site's voice, not to write "AI-style" prose.

MECHANICS:
- Contractions always (don't, won't, it's) — stiff prose is a tell
- Vary sentence length: 4 words. Then twenty-three that flow with subordinate clauses.
- Vary paragraph length: some one sentence, some five
- Specific numbers (47.3%) over vague ("about half")
- Strong positions ("This works") not hedged mush ("This may potentially help")
- Active voice default; passive only for emphasis or when agent is unknown
- First person where voice permits ("I've seen...", "In my experience...")

HEADLINE WORDS (ALLOWED IN H1/H2 ONLY, NEVER IN BODY):
These power words are fine in headlines but become slop in body text:
- "ultimate," "essential," "comprehensive," "complete guide"
- "everything you need to know," "definitive"
Use them for SEO in titles; never let them leak into prose.

SELF-CHECK BEFORE OUTPUT:
□ Does every abstraction have a concrete anchor within 2 sentences?
□ Are there back-to-back paragraphs with the same rhetorical structure?
□ Does the closing rely on synthesis + question + CTA? (If so, cut the CTA, end on substance.)
□ Would a subject-matter expert find any phrase eye-rollingly generic?

{citation_instructions}

{product_block}

SEO REQUIREMENTS:
- Primary keywords to include naturally: {json.dumps(metadata.get('target_keywords', []))}
- SEO Title: {metadata.get('seo_title', 'Generate one')}
- Meta Description: {metadata.get('meta_description', 'Generate one')}
- Internal linking targets: {json.dumps(metadata.get('internal_links', []))}
- Use the primary keyword in the first 100 words and in at least one H2

OUTPUT FORMAT:
Write the complete article in markdown. Include:
- Title as H1
- Proper heading hierarchy (H2, H3)
- Natural paragraph flow (no bullet point lists unless the article type demands it)
- If citations required: inline [1], [2] references and a References section at the end
- If product mention is appropriate: one natural, contextual mention

AFTER the article, include a self-audit JSON block for QA:

```json
{{
  "word_count": <actual word count>,
  "slop_violations": ["<any banned patterns you caught yourself using>"],
  "voice_confidence": <1-5 how well you matched the voice>,
  "citation_count": <number of inline citations used>,
  "structure_notes": "<any concerns about structure or flow>"
}}
```

Write the article now. Output the article markdown followed by the self-audit JSON block."""

        # User message: the plan
        title = metadata.get("title", "Untitled")
        topic = metadata.get("topic", "")

        user = f"""Write this article:

TITLE: {title}
TOPIC: {topic}

ARTICLE PLAN:
{body}

Write the complete article now. Markdown only."""

        return system, user

    # ── Response Parsing ────────────────────────────────────

    def parse_response(
        self,
        response_text: str,
        input_metadata: dict,
        input_body: str,
        site_context: SiteContext,
    ) -> tuple[dict, str]:
        """Parse the written article into an artifact."""

        article_body = response_text.strip()

        # Extract self-audit JSON block (if present) and remove from article body
        self_audit = {}
        audit_match = re.search(r'```json\s*\n(\{[^}]+\})\s*\n```\s*$', article_body, re.DOTALL)
        if audit_match:
            try:
                self_audit = json.loads(audit_match.group(1))
                # Remove the audit block from the article
                article_body = article_body[:audit_match.start()].strip()
                logger.info(f"[write] Self-audit extracted: voice_confidence={self_audit.get('voice_confidence')}")
            except json.JSONDecodeError:
                logger.warning("[write] Failed to parse self-audit JSON")

        # Add markdown fallbacks for visual blocks (stats_driven articles)
        if input_metadata.get("structure_template") == "stats_driven_v1":
            article_body = _add_visual_fallbacks(article_body)

        # Extract title from the article (first H1)
        title = input_metadata.get("title", "Untitled")
        h1_match = re.match(r"^#\s+(.+)", article_body, re.MULTILINE)
        if h1_match:
            title = h1_match.group(1).strip()

        # Count words
        word_count = len(article_body.split())

        # Extract any tags from the article content
        tags = input_metadata.get("target_keywords", [])

        # Check for product mentions
        product_mentions = []
        products = site_context.products
        if products.get("enabled"):
            for product in products.get("owned_products", []):
                if product["name"].lower() in article_body.lower():
                    product_mentions.append(product["name"])

        # Determine category
        category = input_metadata.get("category", "") or site_context.niche

        # Get slug for image sourcing
        slug = input_metadata.get("slug", self._slugify(title))

        # P7: Source hero image from Unsplash
        featured_image = ""
        featured_image_alt = ""
        featured_image_meta = {}
        if IMAGE_PIPELINE_AVAILABLE and os.getenv("UNSPLASH_ACCESS_KEY"):
            hero = self._source_hero_image(
                title=title,
                topic=input_metadata.get("topic", ""),
                site_slug=input_metadata.get("site_id", ""),
                article_slug=slug,
            )
            if hero:
                featured_image = hero.url
                featured_image_alt = hero.alt_text
                featured_image_meta = {
                    "width": hero.width,
                    "height": hero.height,
                    "attribution": hero.attribution,
                    "photographer": hero.photographer,
                    "source_id": hero.source_id,
                }

        # Build metadata
        meta = article_metadata(
            run_id=input_metadata.get("run_id", ""),
            article_id=input_metadata.get("article_id", new_article_id()),
            site_id=input_metadata.get("site_id", ""),
            article_type=input_metadata.get("article_type", ""),
            title=title,
            slug=slug,
            word_count=word_count,
            seo_title=input_metadata.get("seo_title", title),
            meta_description=input_metadata.get("meta_description", ""),
            tags=tags,
            featured_image=featured_image,
            category=category,
        )

        # Carry forward fields QA will need
        meta["topic"] = input_metadata.get("topic", "")
        meta["target_word_count"] = input_metadata.get("target_word_count", 0)
        meta["sources"] = input_metadata.get("sources", [])
        meta["internal_links"] = input_metadata.get("internal_links", [])
        meta["product_mentions"] = product_mentions

        # P7: Add featured image metadata
        if featured_image:
            meta["featured_image_alt"] = featured_image_alt
            meta["featured_image_meta"] = featured_image_meta

        # Add self-audit for QA scoring
        if self_audit:
            meta["self_audit"] = self_audit

        return meta, article_body

    def _source_hero_image(
        self,
        title: str,
        topic: str,
        site_slug: str,
        article_slug: str,
    ):
        """
        Source a hero image for the article using the P7 image pipeline.

        Returns ArticleImage or None if sourcing fails.
        """
        if not IMAGE_PIPELINE_AVAILABLE:
            return None

        try:
            pipeline = ImagePipeline()
            image = pipeline.source_hero_image(
                title=title,
                topic=topic,
                site_slug=site_slug,
                article_slug=article_slug,
            )
            if image:
                logger.info(f"[write] Hero image sourced: {image.url}")
            return image
        except Exception as e:
            logger.warning(f"[write] Failed to source hero image: {e}")
            return None

    # ── Validation ──────────────────────────────────────────

    def validate_input(self, metadata: dict, body: str) -> tuple[bool, str]:
        """Plan must have title, structure, and enough content to write from."""
        if not metadata.get("title"):
            return False, "Plan missing title"
        if not metadata.get("site_id"):
            return False, "Plan missing site_id"
        if not body or len(body.strip()) < 100:
            return False, f"Plan body too short ({len(body.strip())} chars, min 100)"
        if metadata.get("section_count", 0) < 1:
            return False, "Plan has no sections"
        return True, ""

    def validate_output(self, metadata: dict, body: str) -> tuple[bool, str]:
        """Article must meet word count and basic structure checks."""
        word_count = metadata.get("word_count", 0)
        target = metadata.get("target_word_count", 0)

        if word_count < 100:
            return False, f"Article too short: {word_count} words"

        # Stricter word count validation: 85% minimum, 130% maximum
        if target:
            if word_count < target * 0.85:
                return False, f"Article under target: {word_count}/{target} words ({word_count/target*100:.0f}%, need 85%+)"
            if word_count > target * 1.30:
                return False, f"Article over target: {word_count}/{target} words ({word_count/target*100:.0f}%, max 130%)"

        # Check for H1 title
        if not re.search(r"^#\s+", body, re.MULTILINE):
            return False, "Article missing H1 title"

        # Check for citations if site requires them
        site_id = metadata.get("site_id", "")
        try:
            site_context = self.loader.load(site_id)
            article_type = site_context.get_article_type(metadata.get("article_type", ""))
            if article_type and article_type.get("citation_required", False):
                if not re.search(r"\[\d+\]", body):
                    return False, "Article requires citations but none found"
                if "## References" not in body and "## Sources" not in body:
                    return False, "Article requires References section but none found"
        except Exception:
            pass  # If we can't load site config, skip citation check

        return True, ""

    def get_max_tokens(self, metadata: dict, site_context: SiteContext) -> int:
        """Scale token budget to target word count. ~1.3 tokens per word + overhead."""
        article_type = site_context.get_article_type(metadata.get("article_type", ""))
        if article_type:
            max_words = article_type.get("word_count_max", 2000)
        else:
            max_words = metadata.get("target_word_count", 2000)

        # 1.3 tokens/word * max_words + 500 for references/metadata overhead
        tokens = int(max_words * 1.5) + 1000
        return min(tokens, 16384)  # cap at 16k

    # ── Helpers ─────────────────────────────────────────────

    def _slugify(self, text: str) -> str:
        """Convert text to URL-friendly slug."""
        slug = text.lower().strip()
        slug = re.sub(r"[^\w\s-]", "", slug)
        slug = re.sub(r"[-\s]+", "-", slug)
        return slug.strip("-")[:80]

    def _build_exemplar_block(self, site_context: SiteContext) -> str:
        """
        Build voice exemplar block for prompt injection.
        Exemplars are 2-3 paragraphs of ideal prose that demonstrate the site's voice.
        """
        exemplars = site_context.voice_exemplars
        if not exemplars:
            return ""

        return f"""
VOICE EXEMPLARS — MATCH THIS STYLE:
The following paragraphs demonstrate exactly how this site should sound.
Study the rhythm, word choice, and structure. Then write like this.

---
{exemplars.strip()}
---

Your article should be indistinguishable from the exemplars above.
"""


# ── CLI ─────────────────────────────────────────────────────

if __name__ == "__main__":
    WriteModule.cli()
