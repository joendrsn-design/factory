"""
============================================================
ARTICLE FACTORY — WRITE MODULE
============================================================
Module 3: Takes plan artifacts → produces finished articles.

Model: Opus 4.5 (voice fidelity, prose quality — don't cheap out)
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
import re
import logging
from typing import Optional

from base_module import BaseModule
from site_loader import SiteContext
from artifacts import (
    article_metadata, new_article_id,
    load_artifacts_from_dir, load_artifact,
)

logger = logging.getLogger("article_factory.write")


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

def build_citation_instructions(site_context: SiteContext, article_type: dict, sources: list) -> str:
    """
    Build citation instructions for the writing prompt.
    Only included when the article type requires citations.
    """
    if not article_type.get("citation_required", False):
        return "\nCITATIONS: Not required for this article type. Write naturally without inline references."

    if not sources:
        return "\nCITATIONS: Required but no sources provided. Use your knowledge and note claims that should be sourced."

    source_list = []
    for i, src in enumerate(sources, 1):
        title = src.get("title", f"Source {i}")
        url = src.get("url", "")
        source_list.append(f"  [{i}] {title} — {url}")

    sources_block = "\n".join(source_list)

    return f"""
CITATION REQUIREMENTS:
This article REQUIRES inline citations. You are writing as a physician-researcher.
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
    model = "claude-opus-4-6"
    input_module = "planning"
    max_retries = 2
    default_max_tokens = 8192

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

        # Get citation instructions
        sources = metadata.get("sources", [])
        # Sources might be on the research artifact that traveled through planning
        # Check if they're in the body as JSON
        if not sources:
            sources = self._extract_sources_from_body(body)
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

ADDITIONAL STYLE NOTES FROM SITE CONFIG:
{voice.get('style_notes', 'None')}

THINGS TO ABSOLUTELY AVOID:
{json.dumps(voice.get('avoid', []))}

CRITICAL — WRITE LIKE A HUMAN, NOT AN AI:
The article MUST be indistinguishable from expert human writing. AI detectors will flag:

NEVER USE these AI-tell phrases:
- "In today's world/age/fast-paced..." (opening)
- "When it comes to..." / "In the realm of..."
- "Furthermore," / "Moreover," / "Additionally," (overused transitions)
- "It's worth noting..." / "It's important to note..."
- "In conclusion," / "To sum up," / "In summary,"
- "may potentially" / "could possibly" (hedging)
- "This is a game-changer" / "Take X to the next level"
- "Dive deep into..." / "Unlock the power of..."

NEVER USE formulaic contrast structures:
- "turning X into Y" / "transforms X into Y"
- "It's not about X, it's about Y"
- "He didn't X, he Y" / "She didn't X, she Y"
- "less X, more Y" / "not X but Y"
- "stop doing X and start doing Y"
- "the key isn't X, it's Y"
- "from X to Y" (as cliché transformation)

NEVER USE LinkedIn-brain slop:
- "Here's the thing:" / "Let that sink in." / "Read that again."
- "And that's okay." / "Full stop." / "Period."
- "Let me be clear:" / "Make no mistake:"
- "The truth is," / "The reality is,"
- "Here's why that matters:" / "And here's the kicker:"
- "Pro tip:" / "Hot take:" / "Plot twist:"
- "worth sitting with" / "sit with that"
- "This isn't just X, it's Y" / "This isn't just about..."

NEVER USE breathless discovery language:
- "Discover how..." / "Discover the..." / "What you'll discover"
- "uncover" / "reveal" / "secrets" / "little-known"
- "what most people don't realize" / "the surprising truth"

AVOID excessive parallel structure:
- Don't make every list item grammatically identical
- Don't use "improving X, enhancing Y, and boosting Z" patterns
- Vary your sentence structures — not every sentence should be subject-verb-object

WRITE LIKE A HUMAN:
- Use contractions naturally (don't, won't, can't, it's, you'll)
- Vary sentence length dramatically. Short punchy sentences. Then longer, flowing ones with multiple clauses that breathe.
- Vary paragraph length too. Some one sentence. Some longer.
- Use specific numbers (47.3%, not "about half")
- Take strong positions ("This works" not "This may potentially help")
- Include occasional sentence fragments. For emphasis.
- Use natural transitions, not formal ones ("But here's the thing" vs "However,")
- Write in active voice predominantly ("Studies show" vs "It has been shown")
- Include first-person where appropriate ("I've seen..." or "In my experience...")
- Add personality, wit, or strong opinions where the voice permits
- Reference specific studies by author/year, not "research suggests"

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

Write the article now. Output ONLY the article markdown. No preamble, no "here's the article," no meta-commentary."""

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

        # Determine category (Site Empire generates featured images)
        category = input_metadata.get("category", "") or site_context.niche

        # Build metadata
        meta = article_metadata(
            run_id=input_metadata.get("run_id", ""),
            article_id=input_metadata.get("article_id", new_article_id()),
            site_id=input_metadata.get("site_id", ""),
            article_type=input_metadata.get("article_type", ""),
            title=title,
            slug=input_metadata.get("slug", self._slugify(title)),
            word_count=word_count,
            seo_title=input_metadata.get("seo_title", title),
            meta_description=input_metadata.get("meta_description", ""),
            tags=tags,
            featured_image="",  # Site Empire generates based on category
            category=category,
        )

        # Carry forward fields QA will need
        meta["topic"] = input_metadata.get("topic", "")
        meta["target_word_count"] = input_metadata.get("target_word_count", 0)
        meta["sources"] = input_metadata.get("sources", [])
        meta["internal_links"] = input_metadata.get("internal_links", [])
        meta["product_mentions"] = product_mentions

        return meta, article_body

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

        # Allow 20% under target (we'd rather publish slightly short than reject good work)
        if target and word_count < target * 0.6:
            return False, f"Article significantly under target: {word_count}/{target} words ({word_count/target*100:.0f}%)"

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

    def _extract_sources_from_body(self, body: str) -> list[dict]:
        """
        Try to extract sources from the plan body.
        Research sources travel through Planning as JSON in the body.
        """
        # Look for JSON block with sources
        json_match = re.search(r"```json\s*\n(.*?)\n```", body, re.DOTALL)
        if json_match:
            try:
                data = json.loads(json_match.group(1))
                # Could be outline array or sources — check
                if isinstance(data, list) and data and "sources_to_cite" in data[0]:
                    # This is an outline, extract source references
                    all_sources = []
                    for section in data:
                        for src in section.get("sources_to_cite", []):
                            if src not in all_sources:
                                all_sources.append(src)
                    return [{"title": s} for s in all_sources]
            except (json.JSONDecodeError, KeyError, IndexError):
                pass
        return []


# ── CLI ─────────────────────────────────────────────────────

if __name__ == "__main__":
    WriteModule.cli()
