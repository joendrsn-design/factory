"""
============================================================
ARTICLE FACTORY — QA MODULE
============================================================
Module 4: Takes article artifacts → scores + verdict.

Model: Sonnet 4.5 (judgment without creative overhead)
Input folder: pipeline/articles/
Output folder: pipeline/qa/

Verdicts:
  PUBLISH  → article passes, ready for deposit to Obsidian
  REWRITE  → article has fixable issues, send back to Write with feedback
  KILL     → article is unsalvageable, start over from Planning

Scoring:
  Each article scored on multiple dimensions (0-10).
  Composite score determines verdict based on site-specific thresholds.
  QA checks are driven by site config — different sites, different standards.

Rewrite Loop:
  If verdict=REWRITE, QA produces specific rewrite_instructions.
  The orchestrator feeds these back to Write module.
  Max rewrites configurable per site (default: 2).

Usage:
    python qa.py submit --input pipeline/articles
    python qa.py collect --input pipeline/articles --output pipeline/qa
    python qa.py run --input pipeline/articles --output pipeline/qa --limit 1
============================================================
"""

import json
import re
import logging
from typing import Optional

from base_module import BaseModule
from site_loader import SiteContext
from artifacts import qa_metadata, new_article_id

logger = logging.getLogger("article_factory.qa")


# ── Scoring Dimensions ──────────────────────────────────────

SCORING_DIMENSIONS = {
    "voice_fidelity": {
        "description": "Does the article sound like it belongs on this site? Tone, persona, reading level.",
        "weight": 2.0,
    },
    "factual_accuracy": {
        "description": "Are claims supported by evidence? Are citations used correctly? Any red flags?",
        "weight": 2.0,
    },
    "human_authenticity": {
        "description": "Does this read like a human expert wrote it? See AI_DETECTION_CHECKLIST for specific patterns to flag.",
        "weight": 3.0,  # Highest weight - undetectability is critical
    },
    "structure": {
        "description": "Logical flow, proper heading hierarchy, smooth transitions, no abrupt jumps.",
        "weight": 1.5,
    },
    "depth": {
        "description": "Does the article deliver substance for this article type? Not padding, not thin.",
        "weight": 1.5,
    },
    "readability": {
        "description": "Clear prose, appropriate sentence length, no jargon without explanation.",
        "weight": 1.0,
    },
    "seo": {
        "description": "Primary keyword in title, first 100 words, H2s. Meta description quality.",
        "weight": 1.0,
    },
    "engagement": {
        "description": "Would this hold a reader? Hook quality, practical value, closing impact.",
        "weight": 1.0,
    },
    "citation_quality": {
        "description": "If citations required: inline refs present, references section complete, sources real.",
        "weight": 1.5,  # Only applied when citations required
    },
}

# ── AI Detection Checklist ──────────────────────────────────
# These patterns are tells that content was AI-generated.
# Articles with multiple violations should be flagged for REWRITE.

AI_DETECTION_CHECKLIST = """
CRITICAL: Score human_authenticity STRICTLY. Any article that could be flagged by AI detectors is a failure.

## PHRASE-LEVEL TELLS (automatic -2 points each)

### Opening Patterns (AI loves these)
- "In today's [world/age/fast-paced...]"
- "When it comes to..."
- "In the realm of..."
- "Are you struggling with...?"
- "[Topic] is a [adjective] topic that..."
- "Have you ever wondered..."
- "It's no secret that..."

### Transition Crutches (overuse = AI tell)
- "Furthermore," / "Moreover," / "Additionally,"
- "It's worth noting that..."
- "It's important to note..."
- "Interestingly,"
- "That being said,"
- "With that in mind,"
- "At the end of the day,"
- "In essence,"
- "Ultimately,"

### Closing Patterns
- "In conclusion,"
- "To sum up,"
- "In summary,"
- "All in all,"
- "At the end of the day,"
- "The bottom line is..."

### Hedging Language (AI hedges too much)
- "may potentially"
- "could possibly"
- "it's believed that"
- "some experts suggest"
- "research indicates that" (without specific citation)
- "studies show" (without specific citation)
- "it is generally accepted"
- "tends to be"

### AI Filler Phrases
- "This is a game-changer"
- "Take your X to the next level"
- "Unlock the power of..."
- "Dive deep into..."
- "Embark on a journey..."
- "Navigate the complexities of..."
- "Revolutionize your..."
- "Leverage the benefits of..."

### Formulaic Contrast Patterns (AI loves these)
- "turning X into Y" / "transforms X into Y"
- "It's not about X, it's about Y"
- "He/She didn't X, he/she Y"
- "less X, more Y"
- "from X to Y" (when used as a cliché transformation)
- "not X but Y"
- "X? No. Y."
- "forget X — try Y instead"
- "stop doing X and start doing Y"
- "the key isn't X, it's Y"
- "move from X to Y"
- "shift from X to Y"
- "trade X for Y"

### Parallel Structure Overuse
- Three-part lists with identical grammar: "improving X, enhancing Y, and boosting Z"
- "Whether you're X, Y, or Z" openings
- "From X to Y to Z" progressions
- Sentences that all follow subject-verb-object perfectly
- Every bullet starting with same part of speech
- Repetitive "[verb]ing your [noun]" patterns
- "The more X, the more Y" constructions (overused)

### LinkedIn-Brain / Corporate AI Slop
- "Here's the thing:"
- "Let that sink in."
- "Read that again."
- "This. So much this."
- "And that's okay."
- "Full stop."
- "Period."
- "Let me be clear:"
- "Make no mistake:"
- "The truth is,"
- "The reality is,"
- "Here's why that matters:"
- "And here's the kicker:"
- "Plot twist:"
- "Spoiler alert:"
- "Pro tip:"
- "Hot take:"
- "Unpopular opinion:"
- "I said what I said."
- "worth sitting with"
- "This isn't just X, it's Y"
- "This isn't just about..."
- "Discover how..."
- "Discover the..."
- "What you'll discover"

### Breathless Discovery Language
- "discover" (overused — prefer "learn," "find," "see," or just state it)
- "uncover"
- "reveal" / "revealed"
- "secret" / "secrets"
- "little-known"
- "what most people don't realize"
- "the surprising truth"
- "you won't believe"
- "mind-blowing"

## STRUCTURAL TELLS

### Sentence Patterns
- Uniform sentence length (human writing has SHORT. Then longer flowing sentences. Variety.)
- Every paragraph same length (3-4 sentences each = AI tell)
- Perfect parallelism in all lists (humans aren't this consistent)
- Starting multiple consecutive sentences with "This" or "It"

### Paragraph Patterns
- Opening with a claim, followed by explanation, followed by example (every single time)
- Robotic topic-sentence-first structure in every paragraph
- No sentence fragments or one-word paragraphs (humans use these for emphasis)

### List Patterns
- Every bullet point same grammatical structure
- Every bullet point same length
- No personality or opinion in lists (just facts)

## VOICE TELLS

### Missing Human Elements
- No contractions (humans use don't, won't, can't, it's)
- No first-person experience or opinion
- No humor, wit, or personality
- No strong stance or commitment ("I believe" vs "some may think")
- No colloquialisms or natural speech patterns
- Overly formal register throughout
- No rhetorical questions that feel natural
- No specific anecdotes or examples from experience

### Passive Voice Overuse
- More than 20% passive constructions = AI tell
- "It has been shown that..." vs "Smith's 2023 study showed..."
- "Benefits can be experienced..." vs "You'll notice..."

### Specificity Problems
- Vague quantifiers: "many studies," "numerous benefits," "various factors"
- Round numbers only: "50%" instead of "47.3%"
- No specific dates, names, or places
- Generic examples instead of real ones

## CONTENT TELLS

### AI Safety Padding
- Excessive disclaimers
- "Consult a professional before..." in every section
- Avoiding any definitive statements
- Both-sidesing everything unnecessarily

### Knowledge Cutoff Tells
- Only referencing old/classic studies
- Missing recent developments a real expert would know
- Generic information available on Wikipedia

SCORING GUIDE:
- 10: Indistinguishable from expert human writing. Would pass any AI detector.
- 8-9: Minor tells, easily fixable. Mostly human-feeling.
- 6-7: Multiple AI patterns detected. Needs rewrite.
- 4-5: Obviously AI-generated to trained eye. Major rewrite needed.
- 0-3: Would be flagged by any AI detector. Likely KILL.

If human_authenticity < 8, verdict should be REWRITE with specific instructions on which patterns to fix.
"""


def build_scoring_prompt(site_context: SiteContext, article_type: dict) -> str:
    """Build the scoring criteria section of the QA prompt."""

    lines = ["Score each dimension from 0-10:\n"]

    for dim_id, dim in SCORING_DIMENSIONS.items():
        # Skip citation scoring if citations not required
        if dim_id == "citation_quality" and not article_type.get("citation_required", False):
            continue

        lines.append(f"  {dim_id} (weight {dim['weight']}x): {dim['description']}")

    lines.append("\nComposite score = weighted average of all dimensions.")

    # Add site-specific thresholds
    quality = site_context.quality
    publish = quality.get("publish_score", 7.0)
    rewrite = quality.get("rewrite_score", 5.0)

    lines.append(f"\nVERDICT THRESHOLDS for {site_context.site_name}:")
    lines.append(f"  PUBLISH: composite >= {publish}")
    lines.append(f"  REWRITE: composite >= {rewrite} and < {publish}")
    lines.append(f"  KILL:    composite < {rewrite}")

    return "\n".join(lines)


def build_site_checks(site_context: SiteContext, article_type: dict) -> str:
    """Build site-specific quality checks from config."""

    quality = site_context.quality
    checks = quality.get("quality_checks", []) or []

    lines = []
    if checks:
        lines.append("\nSITE-SPECIFIC CHECKS:")
        for check in checks:
            lines.append(f"  - {check}")

    # Always include universal checks
    lines.append("\nUNIVERSAL CHECKS:")
    lines.append("  - No hallucinated sources or fabricated statistics")
    lines.append("  - No broken markdown formatting")
    lines.append("  - Word count within 20% of target")

    if article_type.get("citation_required"):
        lines.append("  - Every major claim has a citation")
        lines.append("  - References section is complete and matches inline citations")
        lines.append("  - No citations to sources not in the provided source list")

    # AI Detection is now handled by dedicated checklist
    lines.append("\nAI DETECTION (CRITICAL - see full checklist below):")
    lines.append("  - Article MUST be indistinguishable from expert human writing")
    lines.append("  - ANY detectable AI patterns = automatic REWRITE verdict")
    lines.append("  - human_authenticity score < 8 = REWRITE")

    return "\n".join(lines)


# ── QA Module ───────────────────────────────────────────────

class QAModule(BaseModule):

    module_name = "qa"
    model = "claude-sonnet-4-5-20250929"
    input_module = "write"
    max_retries = 1  # QA itself doesn't retry
    default_max_tokens = 2048

    # ── Prompt Construction ─────────────────────────────────

    def build_prompt(
        self,
        metadata: dict,
        body: str,
        site_context: SiteContext,
    ) -> tuple[str, str]:
        """Build the QA evaluation prompt."""

        article_type_id = metadata.get("article_type", "")
        article_type = site_context.get_article_type(article_type_id)
        if not article_type:
            article_type = site_context.get_enabled_article_types()[0]

        voice = site_context.voice
        audience = site_context.audience

        scoring_block = build_scoring_prompt(site_context, article_type)
        checks_block = build_site_checks(site_context, article_type)

        # Rewrite count tracking
        rewrite_count = metadata.get("rewrite_count", 0)
        max_rewrites = site_context.quality.get("max_rewrites", 2)
        rewrite_note = ""
        if rewrite_count > 0:
            rewrite_note = f"""
NOTE: This is rewrite attempt {rewrite_count} of {max_rewrites}.
Previous feedback was: {metadata.get('previous_feedback', 'Not available')}
Be stricter on issues that should have been fixed. If the same problems persist, consider KILL."""

        system = f"""You are the QA module of an automated article factory.
Your job: evaluate an article against site standards and issue a verdict.

CRITICAL PRIORITY: Articles must be COMPLETELY UNDETECTABLE as AI-generated.
Any article that could be flagged by AI detectors (GPTZero, Originality.ai, etc.) is a FAILURE.

SITE: {site_context.site_name} ({site_context.niche})
AUDIENCE: {audience.get('profile', 'General')} | Expertise: {audience.get('expertise_level', 'mixed')}
EXPECTED VOICE: {voice.get('tone', 'professional')} | Persona: {voice.get('persona', 'knowledgeable writer')}
ARTICLE TYPE: {article_type.get('label', 'Article')}
TARGET WORD COUNT: {metadata.get('target_word_count', 'Not specified')}
ACTUAL WORD COUNT: {metadata.get('word_count', 'Unknown')}
{rewrite_note}

{scoring_block}

{checks_block}

{AI_DETECTION_CHECKLIST}

AVOID LIST (voice violations):
{json.dumps(voice.get('avoid', []))}

Respond with ONLY valid JSON (no markdown fences, no preamble):
{{
    "verdict": "PUBLISH|REWRITE|KILL",
    "composite_score": 7.5,
    "scores": {{
        "voice_fidelity": 8,
        "factual_accuracy": 7,
        "human_authenticity": 8,
        "structure": 8,
        "depth": 7,
        "readability": 8,
        "seo": 7,
        "engagement": 7,
        "citation_quality": 8
    }},
    "feedback": "2-3 sentence overall assessment",
    "strengths": ["what works well"],
    "issues": ["specific problems found"],
    "ai_tells_found": ["list specific AI patterns detected from the checklist"],
    "rewrite_instructions": "If REWRITE: specific, actionable instructions for the Write module. If PUBLISH/KILL: empty string."
}}

RULES:
1. human_authenticity is the MOST IMPORTANT score. Weight 3x. If < 8, verdict MUST be REWRITE.
2. Voice violations are serious — wrong tone poisons the whole site brand.
3. Factual errors or hallucinated sources are automatic KILL.
4. If word count is >30% off target, note it but don't auto-kill.
5. REWRITE instructions must be specific enough for an LLM to fix the issues.
6. List EVERY AI tell you find in ai_tells_found. Be exhaustive.
7. A single "In conclusion," or "It's worth noting" = automatic -2 on human_authenticity.
8. Check for: uniform sentence length, hedging language, missing contractions, passive voice overuse."""

        user = f"""Evaluate this article:

TITLE: {metadata.get('title', 'Untitled')}
TOPIC: {metadata.get('topic', 'Unknown')}

SOURCES THAT SHOULD BE CITED:
{json.dumps(metadata.get('sources', []), indent=2)}

---

{body}

---

Score and issue your verdict. JSON only."""

        return system, user

    # ── Response Parsing ────────────────────────────────────

    def parse_response(
        self,
        response_text: str,
        input_metadata: dict,
        input_body: str,
        site_context: SiteContext,
    ) -> tuple[dict, str]:
        """Parse QA evaluation into verdict artifact."""

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
            # If parsing fails, default to REWRITE with feedback
            logger.error(f"[qa] Failed to parse QA response: {e}")
            result = {
                "verdict": "REWRITE",
                "composite_score": 5.0,
                "scores": {},
                "feedback": f"QA parse error: {str(e)[:200]}",
                "rewrite_instructions": "QA evaluation failed. Please review and resubmit.",
            }

        verdict = result.get("verdict", "REWRITE").upper()
        if verdict not in ("PUBLISH", "REWRITE", "KILL"):
            verdict = "REWRITE"

        composite = float(result.get("composite_score", 5.0))
        scores = result.get("scores", {})

        # Verify verdict against thresholds
        quality = site_context.quality
        publish_threshold = quality.get("publish_score", 7.0)
        rewrite_threshold = quality.get("rewrite_score", 5.0)

        # Override if score contradicts verdict
        if composite >= publish_threshold and verdict != "PUBLISH":
            logger.info(f"[qa] Score {composite} >= {publish_threshold} but verdict was {verdict}, overriding to PUBLISH")
            verdict = "PUBLISH"
        elif composite < rewrite_threshold and verdict != "KILL":
            logger.info(f"[qa] Score {composite} < {rewrite_threshold} but verdict was {verdict}, overriding to KILL")
            verdict = "KILL"

        # Build feedback string
        feedback_parts = []
        if result.get("feedback"):
            feedback_parts.append(result["feedback"])
        if result.get("strengths"):
            feedback_parts.append("Strengths: " + "; ".join(result["strengths"]))
        if result.get("issues"):
            feedback_parts.append("Issues: " + "; ".join(result["issues"]))
        feedback = "\n".join(feedback_parts)

        rewrite_instructions = result.get("rewrite_instructions", "")

        # Build metadata
        meta = qa_metadata(
            run_id=input_metadata.get("run_id", ""),
            article_id=input_metadata.get("article_id", new_article_id()),
            site_id=input_metadata.get("site_id", ""),
            article_type=input_metadata.get("article_type", ""),
            verdict=verdict,
            score=composite,
            scores_breakdown=scores,
            feedback=feedback,
            rewrite_instructions=rewrite_instructions,
            rewrite_count=input_metadata.get("rewrite_count", 0),
        )

        # Carry forward fields needed for rewrite or deposit
        meta["title"] = input_metadata.get("title", "")
        meta["slug"] = input_metadata.get("slug", "")
        meta["topic"] = input_metadata.get("topic", "")
        meta["target_word_count"] = input_metadata.get("target_word_count", 0)
        meta["word_count"] = input_metadata.get("word_count", 0)
        meta["seo_title"] = input_metadata.get("seo_title", "")
        meta["meta_description"] = input_metadata.get("meta_description", "")
        meta["tags"] = input_metadata.get("tags", [])
        meta["sources"] = input_metadata.get("sources", [])

        # Body: the original article passes through (for deposit or rewrite)
        return meta, input_body

    # ── Validation ──────────────────────────────────────────

    def validate_input(self, metadata: dict, body: str) -> tuple[bool, str]:
        """Article must exist and have basic structure."""
        if not metadata.get("title"):
            return False, "Article missing title"
        if not metadata.get("site_id"):
            return False, "Article missing site_id"
        if not body or len(body.strip()) < 50:
            return False, f"Article body too short ({len(body.strip())} chars)"
        return True, ""

    def validate_output(self, metadata: dict, body: str) -> tuple[bool, str]:
        """QA output must have a valid verdict and score."""
        verdict = metadata.get("verdict", "")
        if verdict not in ("PUBLISH", "REWRITE", "KILL"):
            return False, f"Invalid verdict: {verdict}"

        score = metadata.get("score", -1)
        if not (0 <= score <= 10):
            return False, f"Score out of range: {score}"

        if verdict == "REWRITE" and not metadata.get("rewrite_instructions"):
            return False, "REWRITE verdict but no rewrite_instructions"

        return True, ""

    def get_max_tokens(self, metadata: dict, site_context: SiteContext) -> int:
        """QA responses are structured JSON, don't need many tokens."""
        return 2048


# ── CLI ─────────────────────────────────────────────────────

if __name__ == "__main__":
    QAModule.cli()
