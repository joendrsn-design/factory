"""
============================================================
ARTICLE FACTORY — PLANNING TEMPLATE: stats_driven_v1
============================================================

Drop-in planning template for the `stats_driven` article type.
Loaded by planning.py when site DNA specifies:

    article_types:
      - type_id: stats_driven
        structure_template: "stats_driven_v1"

INPUTS (from upstream modules):
  - SiteContext (site DNA, voice, audience, content pillars)
  - Topic artifact: topic, anchor_urls, data_year, primary_keyword
  - Research artifact, including a structured `statistics: [...]`
    array where each entry has at minimum:
      stat_id, value, unit, context, source_id, source_url,
      source_title, year

OUTPUT:
  - Plan artifact (markdown + YAML frontmatter)
  - Frontmatter holds the structured plan as YAML
  - Body is a human-readable rendering for spot-checking in Obsidian

CONTRACT:
  - Every section anchors to at least one stat_id from research
  - Every chart/table references real stat_ids — no fabrication
  - Every external_link references a source_id that exists
  - meta_description: 140–165 chars; seo_title: ≤60 chars
  - QA enforces citation_density_target downstream

INTEGRATION:
  planning.py dispatches by `structure_template`. Add to its
  template registry:

      from planning_templates.stats_driven_v1 import get_template_config
      TEMPLATES = {"stats_driven_v1": get_template_config(), ...}

============================================================
"""

import json
import yaml
from datetime import datetime, timezone


# ── Model configuration ────────────────────────────────────
MODEL = "claude-haiku-4-5-20251001"
MAX_TOKENS = 4096
TEMPERATURE = 0.4  # low — this is structural, not creative


# ── System prompt ──────────────────────────────────────────
SYSTEM_PROMPT = """You are the Planning module for an article factory \
specializing in statistics-driven reference articles. Your job is to \
produce a detailed, machine-readable outline that the Write module \
executes against without ambiguity.

You are NOT writing the article. You are designing it.

# CORE PRINCIPLES

1. EVERY section anchors to real statistics from the research brief. \
You may not propose a section that has no supporting data.
2. EVERY chart, table, and key-stat callout references stat_id values \
that exist in the research artifact's statistics array. You may not \
fabricate data, sources, or stat_ids.
3. The article must function as a CITATION TARGET for journalists, \
other writers, and AI systems. The structure surfaces specific numbers \
prominently with clear attribution and a clear data year.
4. Be decisive about structure. The Write module follows the plan \
exactly. Vague guidance produces vague articles.

# REQUIRED PLAN COMPONENTS

Return a single JSON object with these top-level fields:

- frontmatter: title, slug, meta_description, seo_title, data_year, \
anchor_sources, primary_keyword, secondary_keywords, target_word_count, \
citation_density_target
- tldr_stats: array of 3-5 entries, each {stat_id, framing}
- methodology_block: 1-2 paragraph guidance on writing the "About this \
data" section (sources, scope, year, caveats)
- key_takeaways: array of 5-8 entries, each {framing, stat_ids}
- sections: array of section specs (see SECTION SCHEMA below)
- comparison_tables: array of {title, columns, row_stat_ids, \
placement_section}
- charts: array of {chart_id, title, type, placement_section, stat_ids, \
description, x_axis_label, y_axis_label}
- key_stat_callouts: array of {stat_id, placement_section, framing}
- internal_link_targets: array of {anchor_text_guidance, \
placement_section, link_intent} — Write or Site Empire resolves URLs
- external_authority_links: array of {source_id, anchor_text_guidance, \
placement_section}
- schema_markup: {article: {...}, dataset: {dataset_name, description, \
variable_measured, temporal_coverage, source_org}}

# SECTION SCHEMA

Each section in sections[] has:
  - section_id: stable kebab-case id
  - heading: H2 text (sentence case; include a number if natural)
  - anchor_stat_id: the primary statistic this section is built around
  - supporting_stat_ids: other stats woven in (array, may be empty)
  - target_words: integer
  - structural_notes: 1-2 sentences on what the section accomplishes
  - opens_with: "stat" | "comparison" | "trend" | "context"
  - includes_visual: chart_id or null
  - external_links: array of source_ids to cite (may be empty)

# CHART TYPE GUIDANCE

Pick the chart type from the data shape:
  - bar: discrete categorical comparisons (states, demographics)
  - line: time series across years
  - comparison_bar: two or three series side by side
  - distribution: histogram or percentile breakdown
  - stacked_bar: composition over time or across categories

If the available statistics don't support a chart cleanly, OMIT the \
chart. Do not force visualizations onto data that doesn't merit them.

# STRUCTURAL DEFAULTS BY WORD COUNT

  - 1800-2400 words: 4 H2 sections, 1 chart, 1 table, 5 takeaways
  - 2400-3000 words: 5 H2 sections, 2 charts, 1-2 tables, 6 takeaways
  - 3000-3500 words: 6 H2 sections, 2-3 charts, 2 tables, 7-8 takeaways

Methodology block, TL;DR, and sources footer are always present.

# CONSTRAINTS

  - slug: kebab-case, includes data_year, max 60 chars
  - meta_description: 140-165 chars, leads with the headline statistic
  - seo_title: max 60 chars, includes primary keyword + year
  - citation_density_target: minimum 1 cited stat per 150 body words
  - Every section's anchor_stat_id MUST exist in the research statistics
  - Every external_link source_id MUST exist in the research sources

# ANTI-SLOP STRUCTURAL DISCIPLINE

Stats-driven articles are prone to slop framing because data-heavy content \
lends itself to "shocking statistics" clickbait. Apply these rules strictly:

TITLES — State what the article covers, declaratively:
  - Good: "US Magnesium Intake Statistics 2026: Prevalence and Risk Groups"
  - Bad: "5 Shocking Statistics About Magnesium Deficiency"
  - Bad: "The Hidden Truth About Magnesium Your Doctor Won't Tell You"
  - Banned patterns: "X Things About", "What Nobody Tells You", \
    "The [Hidden/Shocking/Surprising] Truth", "Why X Will Change Everything"

SECTION HEADINGS — Name what the section covers, not tease:
  - Good: "Magnesium intake by age group"
  - Good: "Cardiovascular outcomes associated with low magnesium"
  - Bad: "The Surprising Age Group Most At Risk"
  - Bad: "What These Numbers Really Mean"
  - Banned: "Conclusion", "Final Thoughts", "The Bottom Line", "Why This Matters"

STRUCTURAL_NOTES — Describe what reader LEARNS, not what they FEEL:
  - Good: "Present the prevalence data with demographic breakdown"
  - Bad: "Hook the reader with the shocking headline stat"
  - Bad: "Build suspense before revealing the key finding"
  - Banned phrases: "hook", "tease", "build suspense", "surprising reveal"

WORD COUNT — Default to the middle of the range, not maximum:
  - Only target max when the data genuinely requires it
  - Allocate words to substance (more stats, deeper analysis), not padding
  - If you can't justify a word count with specific data points, lower it

Return ONLY valid JSON. No prose before or after. No markdown fences.
"""


# ── User prompt template ───────────────────────────────────
USER_PROMPT_TEMPLATE = """Plan a stats-driven article with the following inputs.

# SITE DNA
site_id: {site_id}
site_name: {site_name}
domain: {domain}
audience: {audience}
voice_summary: {voice_summary}
content_pillars: {content_pillars}

# TOPIC
topic: {topic}
data_year: {data_year}
anchor_urls: {anchor_urls}
primary_keyword: {primary_keyword}

# ARTICLE TYPE PARAMETERS
target_word_count: {target_word_count}
citation_density_target: {citation_density_target}
stat_density_min: {stat_density_min}

# RESEARCH BRIEF SUMMARY
{research_summary}

# AVAILABLE STATISTICS
The Research module extracted these structured statistics. Plan the
article around these — you may NOT use stats that are not in this list.

{statistics_json}

# AVAILABLE SOURCES
{sources_json}

# INTERNAL LINK CANDIDATES
The following articles already exist on this site or sister sites and
may be relevant for internal linking. Suggest placement guidance only;
do not fabricate URLs.

{internal_link_candidates}

Now produce the JSON plan.
"""


# ── Output schema (light validation contract) ──────────────
PLAN_SCHEMA = {
    "type": "object",
    "required": [
        "frontmatter", "tldr_stats", "methodology_block",
        "key_takeaways", "sections", "schema_markup",
    ],
    "properties": {
        "frontmatter": {
            "type": "object",
            "required": [
                "title", "slug", "meta_description", "seo_title",
                "data_year", "anchor_sources", "primary_keyword",
                "target_word_count", "citation_density_target",
            ],
        },
        "tldr_stats": {
            "type": "array", "minItems": 3, "maxItems": 5,
            "items": {
                "type": "object",
                "required": ["stat_id", "framing"],
            },
        },
        "key_takeaways": {"type": "array", "minItems": 5, "maxItems": 8},
        "sections": {
            "type": "array", "minItems": 4, "maxItems": 6,
            "items": {
                "type": "object",
                "required": [
                    "section_id", "heading", "anchor_stat_id",
                    "target_words", "structural_notes",
                ],
            },
        },
    },
}


# ── Validation ─────────────────────────────────────────────
class PlanValidationError(Exception):
    """Raised when a plan references stat_ids or source_ids that don't
    exist in the research artifact, or violates frontmatter constraints."""


def validate_plan(
    plan: dict,
    statistics: list[dict],
    sources: list[dict],
) -> None:
    """
    Hard-fail validation. Plan may only reference real stat_ids and
    source_ids from the research artifact. No fabrication.

    Caller is responsible for catching PlanValidationError and routing
    to the rewrite loop or quarantine.
    """
    valid_stat_ids = {s["stat_id"] for s in statistics}
    valid_source_ids = {s["source_id"] for s in sources}

    # TL;DR stats
    for entry in plan.get("tldr_stats", []):
        if entry["stat_id"] not in valid_stat_ids:
            raise PlanValidationError(
                f"tldr_stats references unknown stat_id: {entry['stat_id']}"
            )

    # Key takeaways
    for tk in plan.get("key_takeaways", []):
        for sid in tk.get("stat_ids", []):
            if sid not in valid_stat_ids:
                raise PlanValidationError(
                    f"key_takeaways references unknown stat_id: {sid}"
                )

    # Sections
    for section in plan.get("sections", []):
        anchor = section.get("anchor_stat_id")
        if anchor not in valid_stat_ids:
            raise PlanValidationError(
                f"Section '{section.get('section_id')}' anchors to "
                f"unknown stat_id: {anchor}"
            )
        for sid in section.get("supporting_stat_ids", []):
            if sid not in valid_stat_ids:
                raise PlanValidationError(
                    f"Section '{section['section_id']}' references "
                    f"unknown supporting stat_id: {sid}"
                )
        for src in section.get("external_links", []):
            if src not in valid_source_ids:
                raise PlanValidationError(
                    f"Section '{section['section_id']}' references "
                    f"unknown source_id: {src}"
                )

    # Charts
    for chart in plan.get("charts", []):
        for sid in chart.get("stat_ids", []):
            if sid not in valid_stat_ids:
                raise PlanValidationError(
                    f"Chart '{chart.get('chart_id')}' references "
                    f"unknown stat_id: {sid}"
                )

    # Tables
    for table in plan.get("comparison_tables", []):
        for sid in table.get("row_stat_ids", []):
            if sid not in valid_stat_ids:
                raise PlanValidationError(
                    f"Table '{table.get('title')}' references unknown "
                    f"stat_id: {sid}"
                )

    # Callouts
    for co in plan.get("key_stat_callouts", []):
        if co.get("stat_id") not in valid_stat_ids:
            raise PlanValidationError(
                f"key_stat_callouts references unknown stat_id: "
                f"{co.get('stat_id')}"
            )

    # External authority links
    for el in plan.get("external_authority_links", []):
        if el.get("source_id") not in valid_source_ids:
            raise PlanValidationError(
                f"external_authority_links references unknown "
                f"source_id: {el.get('source_id')}"
            )

    # Frontmatter sanity
    fm = plan["frontmatter"]
    md_len = len(fm["meta_description"])
    if md_len < 140 or md_len > 165:
        raise PlanValidationError(
            f"meta_description length {md_len} outside 140-165"
        )
    if len(fm["seo_title"]) > 60:
        raise PlanValidationError(
            f"seo_title length {len(fm['seo_title'])} exceeds 60"
        )
    if len(fm["slug"]) > 60:
        raise PlanValidationError(
            f"slug length {len(fm['slug'])} exceeds 60"
        )


# ── Renderer: JSON plan → factory artifact ─────────────────
def render_plan_artifact(plan: dict, site_id: str, topic: str) -> str:
    """
    Render the plan as a markdown+frontmatter artifact for the factory's
    artifact directory. Frontmatter holds the structured plan; body is
    a human-readable summary so you can spot-check it in Obsidian.
    """
    fm = {
        "site_id": site_id,
        "topic": topic,
        "module": "planning",
        "structure_template": "stats_driven_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "plan": plan,
    }

    body = [
        f"# {plan['frontmatter']['title']}",
        "",
        f"**Slug:** `{plan['frontmatter']['slug']}`  ",
        f"**Data year:** {plan['frontmatter']['data_year']}  ",
        f"**Target words:** {plan['frontmatter']['target_word_count']}  ",
        f"**Citation density:** "
        f"{plan['frontmatter']['citation_density_target']} per 150 words",
        "",
        "## TL;DR Headline Stats",
        "",
    ]
    for s in plan["tldr_stats"]:
        body.append(f"- `{s['stat_id']}` — {s['framing']}")

    body += ["", "## Section Plan", ""]
    for sec in plan["sections"]:
        body.append(
            f"### {sec['heading']}  _(target: {sec['target_words']}w)_"
        )
        body.append(f"- Anchor stat: `{sec['anchor_stat_id']}`")
        body.append(f"- Notes: {sec['structural_notes']}")
        if sec.get("includes_visual"):
            body.append(f"- Visual: `{sec['includes_visual']}`")
        body.append("")

    if plan.get("charts"):
        body += ["## Charts", ""]
        for c in plan["charts"]:
            body.append(
                f"- **{c['title']}** ({c['type']}) — placed in "
                f"`{c['placement_section']}`"
            )
        body.append("")

    if plan.get("comparison_tables"):
        body += ["## Tables", ""]
        for t in plan["comparison_tables"]:
            body.append(
                f"- **{t['title']}** — placed in `{t['placement_section']}`"
            )
        body.append("")

    fm_yaml = yaml.dump(fm, sort_keys=False, default_flow_style=False)
    return f"---\n{fm_yaml}---\n\n" + "\n".join(body) + "\n"


# ── Build messages for the API call ────────────────────────
def build_messages(
    site_ctx: dict,
    topic_artifact: dict,
    research_artifact: dict,
    article_type_cfg: dict,
    internal_link_candidates: list[dict] | None = None,
) -> list[dict]:
    """
    Build the messages array for a single planning request. Used by
    planning.py in both single-shot and batch modes.
    """
    statistics = research_artifact.get("statistics", [])
    sources = research_artifact.get("sources", [])

    user_prompt = USER_PROMPT_TEMPLATE.format(
        site_id=site_ctx["site_id"],
        site_name=site_ctx["site_name"],
        domain=site_ctx["domain"],
        audience=site_ctx.get("audience", "general"),
        voice_summary=site_ctx.get("voice_summary", ""),
        content_pillars=", ".join(site_ctx.get("content_pillars", [])),
        topic=topic_artifact["topic"],
        data_year=topic_artifact.get(
            "data_year", datetime.now(timezone.utc).year
        ),
        anchor_urls=json.dumps(topic_artifact.get("anchor_urls", [])),
        primary_keyword=topic_artifact.get("primary_keyword", ""),
        target_word_count=article_type_cfg.get("word_count_max", 2400),
        citation_density_target=article_type_cfg.get(
            "citation_density_target", 6.7
        ),
        stat_density_min=article_type_cfg.get("requires", {}).get(
            "stat_density_min", 15
        ),
        research_summary=(research_artifact.get("summary") or "")[:3000],
        statistics_json=json.dumps(statistics, indent=2),
        sources_json=json.dumps(sources, indent=2),
        internal_link_candidates=json.dumps(
            internal_link_candidates or [], indent=2
        ),
    )

    return [{"role": "user", "content": user_prompt}]


# ── Few-shot reference output (LampHill example) ───────────
EXAMPLE_OUTPUT = {
    "frontmatter": {
        "title": (
            "Magnesium Deficiency in 2026: Prevalence, Risk Groups, "
            "and What the Latest Data Show"
        ),
        "slug": "magnesium-deficiency-statistics-2026",
        "meta_description": (
            "About 48% of US adults fall below the magnesium RDA. New "
            "2026 NHANES data reveals which groups are most at risk and "
            "what the deficiency gap means for cardiovascular health."
        ),
        "seo_title": "Magnesium Deficiency Statistics 2026 | LampHill",
        "data_year": 2026,
        "anchor_sources": [
            "https://www.cdc.gov/nchs/nhanes/index.htm"
        ],
        "primary_keyword": "magnesium deficiency statistics",
        "secondary_keywords": [
            "magnesium intake by age",
            "magnesium RDA prevalence",
            "subclinical magnesium deficiency",
        ],
        "target_word_count": 2400,
        "citation_density_target": 6.7,
    },
    "tldr_stats": [
        {"stat_id": "stat_001",
         "framing": "Headline prevalence — share of US adults below RDA"},
        {"stat_id": "stat_004",
         "framing": "Demographic disparity — highest-risk age group"},
        {"stat_id": "stat_007",
         "framing": "Cardiovascular linkage — HR for low-intake quartile"},
    ],
    "methodology_block": (
        "Open by naming the dataset (NHANES 2023-2024 cycle, released "
        "2026), the threshold used (USDA RDA: 420 mg men / 320 mg "
        "women), and the analytic scope (US adults 19+). Acknowledge "
        "the gap between dietary intake assessment and serum/RBC "
        "magnesium status — they don't always agree. One paragraph, no "
        "hedging beyond what the data warrants."
    ),
    "key_takeaways": [
        {"framing": "Prevalence headline", "stat_ids": ["stat_001"]},
        {"framing": "Worst-affected demographic",
         "stat_ids": ["stat_004"]},
        {"framing": "Trend direction since 2015",
         "stat_ids": ["stat_002", "stat_003"]},
        {"framing": "Cardiovascular risk association",
         "stat_ids": ["stat_007"]},
        {"framing": "Metabolic syndrome association",
         "stat_ids": ["stat_008"]},
        {"framing": "Food vs supplement contribution",
         "stat_ids": ["stat_010", "stat_011"]},
    ],
    "sections": [
        {
            "section_id": "national-prevalence",
            "heading": "How widespread is magnesium deficiency in 2026?",
            "anchor_stat_id": "stat_001",
            "supporting_stat_ids": ["stat_002", "stat_003"],
            "target_words": 450,
            "structural_notes": (
                "Lead with the headline 48% figure. Compare to 2015 "
                "baseline. Distinguish dietary inadequacy from clinical "
                "hypomagnesemia."
            ),
            "opens_with": "stat",
            "includes_visual": "chart_001",
            "external_links": ["src_nhanes_2026", "src_usda_rda"],
        },
        {
            "section_id": "by-demographics",
            "heading": "Which groups are most at risk?",
            "anchor_stat_id": "stat_004",
            "supporting_stat_ids": ["stat_005", "stat_006"],
            "target_words": 500,
            "structural_notes": (
                "Break down by age, sex, and income quartile. The 71+ "
                "group and lowest income quartile are the two starkest "
                "cuts."
            ),
            "opens_with": "comparison",
            "includes_visual": "chart_002",
            "external_links": ["src_nhanes_2026"],
        },
        {
            "section_id": "cardiovascular-linkage",
            "heading": "What does low magnesium intake mean for the heart?",
            "anchor_stat_id": "stat_007",
            "supporting_stat_ids": ["stat_008"],
            "target_words": 550,
            "structural_notes": (
                "Translate the HR ratio into plain language. Frame as "
                "association, not causation. Cite the meta-analysis "
                "directly. Note arterial stiffness mechanism briefly."
            ),
            "opens_with": "stat",
            "includes_visual": None,
            "external_links": ["src_meta_analysis_2025"],
        },
        {
            "section_id": "food-vs-supplement",
            "heading": "Where Americans actually get their magnesium",
            "anchor_stat_id": "stat_010",
            "supporting_stat_ids": ["stat_011"],
            "target_words": 450,
            "structural_notes": (
                "Diet contributes the majority but supplements close a "
                "meaningful gap. Quantify both. Avoid product mentions "
                "in this section — pillar context only."
            ),
            "opens_with": "comparison",
            "includes_visual": None,
            "external_links": ["src_nhanes_2026"],
        },
    ],
    "comparison_tables": [
        {
            "title": "Magnesium intake adequacy by age group, 2026",
            "columns": [
                "Age group", "% below RDA", "Median intake (mg)", "RDA",
            ],
            "row_stat_ids": ["stat_004", "stat_005", "stat_006"],
            "placement_section": "by-demographics",
        }
    ],
    "charts": [
        {
            "chart_id": "chart_001",
            "title": "US adults below magnesium RDA, 2015-2026",
            "type": "line",
            "placement_section": "national-prevalence",
            "stat_ids": ["stat_002", "stat_003", "stat_001"],
            "description": (
                "Trend showing slight worsening of dietary inadequacy "
                "over the decade"
            ),
            "x_axis_label": "Year",
            "y_axis_label": "% below RDA",
        },
        {
            "chart_id": "chart_002",
            "title": "% below magnesium RDA by age group, 2026",
            "type": "bar",
            "placement_section": "by-demographics",
            "stat_ids": ["stat_004", "stat_005", "stat_006"],
            "description": "Age-stratified inadequacy bar chart",
            "x_axis_label": "Age group",
            "y_axis_label": "% below RDA",
        },
    ],
    "key_stat_callouts": [
        {
            "stat_id": "stat_001",
            "placement_section": "national-prevalence",
            "framing": "Headline percentage callout above section",
        }
    ],
    "internal_link_targets": [
        {
            "anchor_text_guidance": "magnesium and sleep",
            "placement_section": "cardiovascular-linkage",
            "link_intent": (
                "cluster — point to existing LampHill magnesium-sleep "
                "article"
            ),
        }
    ],
    "external_authority_links": [
        {
            "source_id": "src_nhanes_2026",
            "anchor_text_guidance": "NHANES 2023-2024 dietary data",
            "placement_section": "national-prevalence",
        }
    ],
    "schema_markup": {
        "article": {
            "headline": (
                "Magnesium Deficiency in 2026: Prevalence, Risk Groups, "
                "and What the Latest Data Show"
            ),
            "author": "Caleb Newton, MD",
            "datePublished": "2026-04-25",
        },
        "dataset": {
            "dataset_name": (
                "Magnesium intake adequacy in US adults, 2026"
            ),
            "description": (
                "Synthesis of NHANES 2023-2024 dietary intake on "
                "magnesium adequacy by age, sex, and income, with "
                "cardiovascular outcome associations."
            ),
            "variable_measured": [
                "% below magnesium RDA",
                "Median magnesium intake",
                "HR for cardiovascular events by intake quartile",
            ],
            "temporal_coverage": "2015/2026",
            "source_org": "CDC NHANES; USDA",
        },
    },
}


# ── Entry point used by planning.py ────────────────────────
def get_template_config() -> dict:
    """
    Returns the template config that planning.py expects when it
    dispatches by structure_template name. planning.py imports this
    and merges it into its existing message-building flow.
    """
    return {
        "template_id": "stats_driven_v1",
        "model": MODEL,
        "max_tokens": MAX_TOKENS,
        "temperature": TEMPERATURE,
        "system_prompt": SYSTEM_PROMPT,
        "build_messages": build_messages,
        "validate_plan": validate_plan,
        "render_artifact": render_plan_artifact,
        "schema": PLAN_SCHEMA,
        "example": EXAMPLE_OUTPUT,
    }
