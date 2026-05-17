"""
============================================================
ARTICLE FACTORY — QA MODULE
============================================================
Module 4: Takes article artifacts → scores + verdict.

Model: Sonnet 4.7 (better instruction-following for structured JSON)
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

Deterministic Pre-Scoring:
  compute_slop_metrics() runs regex checks BEFORE the LLM call.
  Hard-fail gates (5+ antitheses, 3+ banned phrases) short-circuit to REWRITE.
  Slop metrics are passed as facts to the LLM, not re-derived.

Rewrite Loop:
  If verdict=REWRITE, QA produces specific rewrite_instructions.
  previous_issues are tracked — recurring issues escalate to KILL.
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


# ── Deterministic Slop Metrics ─────────────────────────────────
# Pre-compute pattern counts via regex before LLM evaluation.
# These are passed as facts, not re-derived by the model.

def compute_slop_metrics(body: str, site_context: Optional[SiteContext] = None) -> dict:
    """
    Deterministic pre-scoring: count specific slop patterns via regex.
    These counts are passed to the LLM as ground truth, not re-derived.
    """
    metrics = {
        "antithesis_count": 0,
        "antithesis_examples": [],
        "banned_phrases_found": [],
        "rhetorical_questions_count": 0,
        "thats_how_thats_the_count": 0,
        "passive_voice_ratio": 0.0,
        "contraction_count": 0,
        "word_count": len(body.split()),
        "sentence_count": 0,
        "avg_sentence_length": 0.0,
        "sentence_length_variance": 0.0,
    }

    # Antithesis patterns (case-insensitive)
    antithesis_patterns = [
        r"it'?s not (?:about |just )?\w+[\.,;] it'?s",
        r"this isn'?t \w+[\.,;] (?:this is|it'?s)",
        r"not \w+,? but \w+",
        r"the real \w+ isn'?t \w+[\.,;] it'?s",
        r"didn'?t just \w+,? \w+ \w+",
        r"less \w+,? more \w+",
        r"stop \w+ing and start \w+ing",
        r"the key isn'?t \w+,? it'?s",
        r"turning \w+ into \w+",
        r"transforms? \w+ into \w+",
        r"\w+ becomes \w+",
    ]
    for pat in antithesis_patterns:
        matches = re.findall(pat, body, re.IGNORECASE)
        metrics["antithesis_count"] += len(matches)
        metrics["antithesis_examples"].extend(matches[:3])  # cap examples

    # Banned phrases (exact match, case-insensitive)
    BANNED_PHRASES = [
        "let that sink in", "read that again", "make no mistake",
        "here's the thing:", "the truth is,", "the reality is,",
        "pro tip:", "hot take:", "plot twist:", "spoiler alert:",
        "in conclusion,", "to sum up,", "in summary,", "all in all,",
        "the bottom line is", "it's worth noting", "it's important to note",
        "now we zoom out", "now layer in one more piece",
        "this is a game-changer", "take your", "to the next level",
        "unlock the power of", "dive deep into", "embark on a journey",
        "navigate the complexities", "leverage the benefits",
        "in today's fast-paced", "when it comes to", "in the realm of",
        "have you ever wondered", "it's no secret that",
        "and that's okay.", "full stop.", "period.",
        "stupid simple", "not gonna lie", "just saying",
        "what nobody tells you", "the hard truth is",
        "what they don't want you to know", "we need to talk about",
        "let me be clear", "i want to be clear",
        "worth keeping in mind", "worth sitting with",
    ]

    # Conditional banlist additions based on voice profile
    niche = (site_context.niche.lower() if site_context else "")
    is_clinical = any(k in niche for k in ["medical", "clinical", "pathology"])

    additional_bans = []
    if not is_clinical:
        # These are fine on clinical sites, slop everywhere else
        additional_bans.extend([
            "furthermore,", "moreover,", "additionally,",
            "interestingly,", "notably,",
        ])

    body_lower = body.lower()
    for phrase in BANNED_PHRASES + additional_bans:
        if phrase in body_lower:
            metrics["banned_phrases_found"].append(phrase)

    # Rhetorical questions in the closing third
    closing_third = body[int(len(body) * 0.66):]
    metrics["rhetorical_questions_count"] = len(re.findall(r"\?", closing_third))

    # "That's how" / "That's the" sentences
    metrics["thats_how_thats_the_count"] = len(re.findall(
        r"\bthat'?s (?:how|the)\b", body, re.IGNORECASE
    ))

    # Contractions (proxy for human voice)
    contraction_pattern = r"\b\w+'(?:t|s|re|ve|ll|d|m)\b"
    metrics["contraction_count"] = len(re.findall(contraction_pattern, body))

    # Sentence stats (rough — split on . ! ?)
    sentences = [s.strip() for s in re.split(r"[.!?]+", body) if s.strip()]
    metrics["sentence_count"] = len(sentences)
    if sentences:
        lengths = [len(s.split()) for s in sentences]
        metrics["avg_sentence_length"] = sum(lengths) / len(lengths)
        if len(lengths) > 1:
            mean = metrics["avg_sentence_length"]
            metrics["sentence_length_variance"] = sum((l - mean) ** 2 for l in lengths) / len(lengths)

    # Passive voice (rough heuristic: "was/were/been + past participle")
    passive_matches = re.findall(
        r"\b(?:was|were|been|being|is|are|am)\s+\w+ed\b", body, re.IGNORECASE
    )
    if metrics["sentence_count"] > 0:
        metrics["passive_voice_ratio"] = len(passive_matches) / metrics["sentence_count"]

    return metrics


def check_hard_fails(slop_metrics: dict, body: str, metadata: dict) -> Optional[dict]:
    """
    Return a pre-built REWRITE verdict dict if the article fails hard gates.
    Return None if the article should proceed to LLM evaluation.
    """
    issues = []

    if slop_metrics["antithesis_count"] >= 5:
        issues.append(f"Antithesis constructions: {slop_metrics['antithesis_count']} (max 3)")

    if len(slop_metrics["banned_phrases_found"]) >= 3:
        issues.append(f"Banned phrases: {slop_metrics['banned_phrases_found']}")

    if slop_metrics["thats_how_thats_the_count"] >= 4:
        issues.append(f"\"That's how/the\" sentences: {slop_metrics['thats_how_thats_the_count']}")

    if slop_metrics["sentence_length_variance"] < 5.0 and slop_metrics["sentence_count"] > 10:
        issues.append("Sentence length variance too low (uniform sentences = AI tell)")

    if not issues:
        return None

    return {
        "verdict": "REWRITE",
        "composite_score": 4.0,
        "scores": {"human_authenticity": 3, "voice_fidelity": 5},
        "feedback": f"Hard-gate failures: {len(issues)} critical issues detected",
        "strengths": [],
        "issues": issues,
        "ai_tells_found": slop_metrics["banned_phrases_found"] + slop_metrics["antithesis_examples"],
        "rewrite_instructions": (
            "Article failed deterministic slop checks. Specifically: " +
            "; ".join(issues) +
            ". Rewrite addressing each issue. Do not use any banned phrases. "
            "Vary sentence length. Replace antithesis constructions with declarative statements."
        ),
    }


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
        "description": "Does this match the voice exemplars? Uses pre-computed slop metrics + LLM judgment on context.",
        "weight": 3.0,  # Highest weight - voice authenticity is critical
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
    "statistical_integrity": {
        "description": "For stats_driven articles: all stat references valid, no fabricated numbers, data years accurate.",
        "weight": 2.5,  # Only applied to stats_driven articles
    },
}

# ── AI Detection Checklist ──────────────────────────────────
# Slimmed down to LLM-judgment categories only.
# Pattern-match categories are now handled by compute_slop_metrics().

AI_DETECTION_CHECKLIST = """
================================================================================
VOICE AUTHENTICITY EVALUATION
================================================================================
The goal is NOT "evade AI detectors" — those tools are noisy and gameable.
The goal IS: "a thoughtful reader of this site would find nothing off about this piece."

The PRE-COMPUTED SLOP METRICS above are facts. Use them as inputs, don't re-derive.
Focus your evaluation on things regex cannot catch:

================================================================================
CATEGORY 1: PSEUDO-ANALYTICAL VOCABULARY (requires context judgment)
================================================================================
Flag when used WITHOUT concrete mechanism, definition, or data.

Watchwords that REQUIRE justification:
- "structural" / "structural shift" / "structurally"
- "reprices" / "violent gaps" / "one-sided equation"
- "order of magnitude" (when not literal 10x)
- "absorb at scale" / "evaporating supply" / "asymmetric"

RULE: If any appear, surrounding 2 sentences MUST contain a number, citation,
or concrete mechanism. Otherwise flag as unearned abstraction.

================================================================================
CATEGORY 2: FAKE-MECHANISM SENTENCES (requires context judgment)
================================================================================
Sentences that sound explanatory but contain zero actual mechanism.
Test: Can you replace the sentence with "trust me, this matters" without losing info?

Examples:
- "That's how markets behave when [X] disappears"
- "That's the imbalance, not priced in"
- "That's how you [achieve outcome]"

RULE: Any "That's how/the" sentence must point to a SPECIFIC mechanism
stated in the prior 3 sentences, not assert one by fiat.

================================================================================
CATEGORY 3: LLM CLOSING SYNTHESIS PATTERN (requires structural reading)
================================================================================
The canonical "let me restate my thesis with hedged certainty" close.

Beats to detect in closing 3 paragraphs:
1. "So what I [just laid out / showed you / explained]..."
2. Restatement of premise as established fact
3. "This is going to [send / drive / push] [X] to [round number]"
4. "We could [very easily] see [maximalist outcome]"
5. Engagement-farming question
6. Call to action

RULE: 3+ beats = flag for rewrite. The article should end on substance.

================================================================================
CATEGORY 4: MIXED-VOICE / REGISTER SEAMS (requires holistic reading)
================================================================================
Mixed-voice articles show register seams: tight abstract antithesis in the body
giving way to colloquial phrasing at open/close.

Signs of register seams:
- Opening/closing use contractions, fragments, personality — body is formal
- Opening/closing have specific anecdotes — body is generic abstraction
- Sudden shift from "I" voice to passive/third-person in the body
- Body has stacked antitheses/tricolons that vanish in bookends

FLAG: Article where the middle 60% reads measurably more abstract than edges.

================================================================================
MISSING HUMAN ELEMENTS (voice check)
================================================================================
- No contractions (humans use don't, won't, can't, it's)
- No first-person experience or opinion (where voice permits)
- No humor, wit, or personality
- No strong stance ("I believe" vs "some may think")
- Overly formal register throughout
- Uniform sentence/paragraph length

================================================================================
SCORING GUIDE
================================================================================
Score against the VOICE EXEMPLARS provided, not against an abstract standard.

- 10: Reads exactly like the site's exemplar prose. No noticeable patterns.
- 8-9: Minor patterns present but unobtrusive. Mostly natural.
- 6-7: Multiple patterns detected. A careful reader would notice. Needs rewrite.
- 4-5: Patterns are obvious. Reads as generic AI output.
- 0-3: Nearly every paragraph contains a pattern. KILL.
"""


def validate_stat_references(body: str, statistics: list[dict]) -> tuple[bool, list[str]]:
    """
    Validate that stat references in article match research statistics.
    Returns (all_valid, list_of_invalid_refs).

    For stats_driven articles, this is a hard gate - if ALL stat references
    are fabricated, auto-reject without entering rewrite loop.
    """
    if not statistics:
        return True, []  # No statistics to validate against

    valid_stat_ids = {s.get("stat_id", "") for s in statistics}
    valid_values = {str(s.get("value", "")).lower() for s in statistics}

    # Simple heuristic: look for stat_id patterns or percentage values
    import re

    # Find stat_id references like stat_001, stat_002
    stat_id_refs = re.findall(r'stat_\d{3}', body, re.IGNORECASE)
    invalid_ids = [sid for sid in stat_id_refs if sid.lower() not in {v.lower() for v in valid_stat_ids}]

    # This is a soft validation - mainly to catch fabricated data
    # QA will do deeper analysis in the scoring prompt
    return len(invalid_ids) == 0, invalid_ids


def build_scoring_prompt(site_context: SiteContext, article_type: dict) -> str:
    """Build the scoring criteria section of the QA prompt."""

    lines = ["Score each dimension from 0-10:\n"]

    is_stats_driven = article_type.get("structure_template") == "stats_driven_v1" or \
                      article_type.get("type_id") == "stats_driven"

    for dim_id, dim in SCORING_DIMENSIONS.items():
        # Skip citation scoring if citations not required
        if dim_id == "citation_quality" and not article_type.get("citation_required", False):
            continue
        # Skip statistical_integrity if not a stats_driven article
        if dim_id == "statistical_integrity" and not is_stats_driven:
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

    # Stats-driven article specific checks
    is_stats_driven = article_type.get("structure_template") == "stats_driven_v1" or \
                      article_type.get("type_id") == "stats_driven"
    if is_stats_driven:
        lines.append("\nSTATISTICAL INTEGRITY CHECKS (CRITICAL for stats_driven):")
        lines.append("  - Every stat_id reference must match the provided statistics array")
        lines.append("  - No fabricated numbers — all statistics must trace to source_ids")
        lines.append("  - Data years must be accurate — don't cite 2024 data as 2026")
        lines.append("  - If ALL stat references are fabricated = automatic KILL (not rewrite)")
        lines.append("  - statistical_integrity < 6 = KILL (data articles require high integrity)")

    # Voice authenticity is handled by dedicated checklist + deterministic checks
    lines.append("\nVOICE AUTHENTICITY (CRITICAL - see full checklist below):")
    lines.append("  - Article MUST read like the target voice exemplars")
    lines.append("  - PRE-COMPUTED SLOP METRICS provide hard pattern counts")
    lines.append("  - Focus LLM judgment on context-dependent issues only")

    return "\n".join(lines)


# ── QA Module ───────────────────────────────────────────────

class QAModule(BaseModule):

    module_name = "qa"
    model = "claude-sonnet-4-7"  # Better instruction-following for structured JSON
    input_module = "write"
    max_retries = 1  # QA itself doesn't retry
    default_max_tokens = 2048

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._current_slop_metrics = None  # Cache for slop metrics between build_prompt and parse_response

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

        # Compute deterministic slop metrics
        slop_metrics = compute_slop_metrics(body, site_context)
        # Store for later use in parse_response
        self._current_slop_metrics = slop_metrics

        # Rewrite count tracking with explicit previous issues
        rewrite_count = metadata.get("rewrite_count", 0)
        max_rewrites = site_context.quality.get("max_rewrites", 2)
        rewrite_note = ""
        if rewrite_count > 0:
            previous_issues = metadata.get("previous_issues", [])
            rewrite_note = f"""
REWRITE ATTEMPT: {rewrite_count} of {max_rewrites}
PREVIOUS ISSUES THAT MUST BE RESOLVED:
{json.dumps(previous_issues, indent=2)}

If ANY of the previous issues are still present in this article, verdict MUST be KILL,
not REWRITE. The writer has had its chance. Do not enter another rewrite cycle for
issues that should already be fixed."""

        # Voice exemplars block
        exemplar_block = ""
        if site_context.voice_exemplars:
            exemplar_block = f"""
=== VOICE EXEMPLARS (the target voice for this site) ===
{site_context.voice_exemplars.strip()}

Score voice_fidelity against these exemplars. The article should match
their cadence, register, and density. Not their phrases — their feel.
"""

        system = f"""You are the QA module of an automated article factory.
Your job: evaluate an article against site standards and issue a verdict.

CRITICAL PRIORITY: Articles must read like the target voice exemplars for this site.
The bar is not "evade detection tools" — those are noisy and gameable. The bar is
"a thoughtful reader of this site would find nothing off about this piece."

SITE: {site_context.site_name} ({site_context.niche})
AUDIENCE: {audience.get('profile', 'General')} | Expertise: {audience.get('expertise_level', 'mixed')}
EXPECTED VOICE: {voice.get('tone', 'professional')} | Persona: {voice.get('persona', 'knowledgeable writer')}
{exemplar_block}
ARTICLE TYPE: {article_type.get('label', 'Article')}
TARGET WORD COUNT: {metadata.get('target_word_count', 'Not specified')}
ACTUAL WORD COUNT: {metadata.get('word_count', 'Unknown')}
{rewrite_note}

{scoring_block}

{checks_block}

{AI_DETECTION_CHECKLIST}

WRITER SELF-AUDIT VERIFICATION:
The Write module reports its own rhetorical-device counts in `self_audit`.
Compare these against the PRE-COMPUTED SLOP METRICS in the user message.

If the writer's counts differ from the deterministic counts by 2+,
note this in `issues` as "Writer self-audit unreliable" and apply -1 to
human_authenticity. A writer that miscounts its own output is more likely
to have other quality issues.

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
    "ai_tells_found": ["list specific AI patterns detected"],
    "rewrite_instructions": "If REWRITE: specific, actionable instructions for the Write module. If PUBLISH/KILL: empty string."
}}

RULES:
1. human_authenticity is the MOST IMPORTANT score. Weight 3x.
2. Voice violations are serious — wrong tone poisons the whole site brand.
3. Factual errors or hallucinated sources are automatic KILL.
4. If word count is >30% off target, note it but don't auto-kill.
5. REWRITE instructions must be specific enough for an LLM to fix the issues.
6. Use the PRE-COMPUTED SLOP METRICS as facts. Don't re-derive pattern counts.
7. Focus your judgment on things regex cannot catch: context, register, fake mechanisms."""

        # Include statistics for stats_driven articles
        statistics = metadata.get("statistics", [])
        stats_section = ""
        if statistics and (article_type.get("structure_template") == "stats_driven_v1" or
                          article_type.get("type_id") == "stats_driven"):
            stats_section = f"""
VALID STATISTICS (verify all stat references match these):
{json.dumps(statistics, indent=2)}
"""

        # Writer's self-audit for verification
        self_audit = metadata.get("self_audit", {})

        user = f"""Evaluate this article:

TITLE: {metadata.get('title', 'Untitled')}
TOPIC: {metadata.get('topic', 'Unknown')}

PRE-COMPUTED SLOP METRICS (these are facts, not estimates):
- Antithesis constructions found: {slop_metrics['antithesis_count']}
- Antithesis examples: {slop_metrics['antithesis_examples'][:3]}
- Banned phrases found: {slop_metrics['banned_phrases_found']}
- "That's how/the" sentences: {slop_metrics['thats_how_thats_the_count']}
- Rhetorical questions in closing third: {slop_metrics['rhetorical_questions_count']}
- Contraction count: {slop_metrics['contraction_count']}
- Sentence count: {slop_metrics['sentence_count']}
- Avg sentence length: {slop_metrics['avg_sentence_length']:.1f}
- Sentence length variance: {slop_metrics['sentence_length_variance']:.1f}
- Passive voice ratio: {slop_metrics['passive_voice_ratio']:.2f}

Use these metrics as inputs to your scoring. Do not re-derive them.

WRITER'S SELF-AUDIT (verify against the body):
{json.dumps(self_audit, indent=2) if self_audit else "Not provided"}

SOURCES THAT SHOULD BE CITED:
{json.dumps(metadata.get('sources', []), indent=2)}
{stats_section}
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

        # Retrieve slop metrics computed in build_prompt
        slop_metrics = getattr(self, '_current_slop_metrics', None)
        if slop_metrics is None:
            # Recompute if not cached (shouldn't happen in normal flow)
            slop_metrics = compute_slop_metrics(input_body, site_context)

        # Check for hard fails before parsing LLM response
        hard_fail = check_hard_fails(slop_metrics, input_body, input_metadata)
        if hard_fail:
            logger.info(f"[qa] Hard-fail gate triggered: {hard_fail['issues']}")
            result = hard_fail
        else:
            # Clean JSON from LLM response
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

        # ── Statistical Integrity Auto-Reject (separate branch) ────
        # For stats_driven articles, low statistical_integrity is an
        # automatic KILL without entering the rewrite loop.
        article_type_id = input_metadata.get("article_type", "")
        article_type = site_context.get_article_type(article_type_id)
        is_stats_driven = article_type and (
            article_type.get("structure_template") == "stats_driven_v1" or
            article_type.get("type_id") == "stats_driven"
        )

        if is_stats_driven:
            stat_integrity_score = scores.get("statistical_integrity", 10)
            if stat_integrity_score < 6:
                logger.warning(
                    f"[qa] ⚠️ Statistical integrity KILL: score {stat_integrity_score} < 6 "
                    f"(stats_driven articles require high data integrity)"
                )
                verdict = "KILL"
                result["feedback"] = (
                    f"STATISTICAL INTEGRITY FAILURE: Score {stat_integrity_score}/10. "
                    f"Stats-driven articles require accurate data references. "
                    f"Original feedback: {result.get('feedback', '')}"
                )

        # Verify verdict against thresholds
        quality = site_context.quality
        publish_threshold = quality.get("publish_score", 7.0)
        rewrite_threshold = quality.get("rewrite_score", 5.0)

        # Override if score contradicts verdict (skip for stats_driven auto-kill)
        if verdict != "KILL":  # Don't override stats_driven auto-kill
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

        # Store slop metrics for trend analysis across runs (Change 8)
        meta["slop_metrics"] = slop_metrics
        meta["self_audit"] = input_metadata.get("self_audit", {})

        # Compute audit discrepancy (Change 4)
        writer_audit = input_metadata.get("self_audit", {})
        writer_antithesis = writer_audit.get("slop_violations", [])
        # Count antitheses in writer's self-reported violations
        writer_antithesis_count = len([v for v in writer_antithesis if "antithesis" in v.lower()]) if writer_antithesis else 0
        deterministic_antithesis = slop_metrics.get("antithesis_count", 0)
        meta["audit_discrepancy"] = {
            "writer_antithesis_count": writer_antithesis_count,
            "deterministic_antithesis_count": deterministic_antithesis,
            "discrepancy_flagged": abs(writer_antithesis_count - deterministic_antithesis) >= 2,
        }

        # Store issues for potential rewrite loop escalation (Change 9)
        meta["previous_issues"] = result.get("issues", [])

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
