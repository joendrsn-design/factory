# Magpie Diagnostics — Pillar Article Type Specs (P1–P4)

The substrate generates **spokes** (Type A: one per histologic entity; Type B: one
per biomarker). These four **pillar** types sit above the spokes as the orienting
entry points for each cancer page. Structure per cancer = 4 pillars + N spokes,
with pillars linking down to spokes and spokes linking up.

Granularity: exactly one of each pillar per cancer (6 cancers × 4 = 24 pillars).

---

## Dependency: new substrate file — `<cancer>_epidemiology.json`

P1 (explainer) needs epidemiological data the classification/biomarker files don't
carry. This is a fourth substrate file per cancer, sourced from open, citable
authorities (SEER, WHO/GLOBOCAN, ACS Facts & Figures, USPSTF for screening). Built
once, refreshed periodically (stats move yearly, not weekly).

```json
{
  "dataset": "breast_epidemiology",
  "source_policy": "SEER, WHO/GLOBOCAN, ACS Facts & Figures, USPSTF. Citable; refreshed yearly. NCCN not used.",
  "epidemiology": {
    "incidence": {
      "us_annual_new_cases": "value + year",
      "lifetime_risk": "e.g. ~1 in 8 (women)",
      "rank_among_cancers": "context",
      "notes": "trend direction, demographic patterns"
    },
    "mortality": {
      "us_annual_deaths": "value + year",
      "five_year_survival_overall": "percent + year",
      "survival_by_stage": {"localized": "...", "regional": "...", "distant": "..."}
    },
    "risk_factors": [
      {"factor": "age", "detail": "...", "modifiable": false},
      {"factor": "germline BRCA1/2", "detail": "...", "modifiable": false},
      {"factor": "...", "detail": "...", "modifiable": true}
    ],
    "screening": {
      "modalities": ["mammography", "..."],
      "guideline_summary": "neutral description; cite USPSTF/ACS, not NCCN",
      "notes": "areas of active debate (e.g., start age)"
    },
    "presentation": "common signs/symptoms at diagnosis, in neutral terms"
  },
  "references": ["seer_breast", "globocan", "acs_facts", "uspstf_breast"],
  "_provenance": "internal audit only"
}
```

Same conventions as the other substrate files: resolved references, `_provenance`
stripped before Write, UNVERIFIED until pathologist sign-off. Stats are the
fastest-aging content here — flag for yearly refresh, and the verify gate should
re-check headline numbers at write time.

---

## P1 — Cancer Explainer / Overview
**One per cancer.** Consumes: `<cancer>_epidemiology.json` + top-level summaries of
the classification and biomarker datasets (for the "what's ahead" orientation).

The lay-and-clinician front door. A reader landing on the cancer page reads this
first. Educational, oriented, links downward into everything else.

Section skeleton:
1. **What it is** — the disease in plain terms; what organ/tissue, what goes wrong.
2. **How common it is** — incidence, lifetime risk, rank, trend (from epi file).
3. **Who's at risk** — risk factors, modifiable vs not.
4. **How it's found** — screening modalities and the gist of current screening
   guidance (cite USPSTF/ACS); common presentation.
5. **Outlook** — survival framed carefully by stage, with the explicit caveat that
   these are population statistics, not individual prognoses.
6. **How it's diagnosed and classified** — 2–3 sentence bridge to the H&E pillar (P2).
7. **How treatment is guided** — 2–3 sentence bridge to the biomarker pillar (P3).
8. **References.**

Guardrails: population statistics ≠ individual prognosis (state this explicitly).
No screening "recommendations" to the individual reader — describe what guidelines
say, neutrally. No treatment advice.

---

## P2 — H&E Diagnosis & Classification Overview
**One per cancer.** Consumes: ALL `classification_records` for the cancer (synthesis,
not one-by-one). This is the hub for the Type A spokes.

How the pathologist approaches the specimen and how the disease is classified.

Section skeleton:
1. **From specimen to diagnosis** — biopsy/resection, what the pathologist examines.
2. **The H&E picture** — what the tumor looks like on the slide; the morphologic
   features that drive classification (synthesized across the subtypes).
3. **The major categories** — the classification axes for this cancer (e.g., breast:
   in situ vs invasive, the special subtypes; ovarian: the five histotypes; prostate:
   grading-as-spine). Each category gets a sentence or two and **links down to its
   subtype spoke (Type A)**.
4. **How grading/staging works** — the grading system(s), in overview.
5. **Why classification matters** — it determines everything downstream.
6. **What's current** — flag where the classification recently changed (WHO5/ICC),
   the freshness signal.
7. **References.**

The internal-link mesh makes this hub auto-link to every Type A spoke for the cancer.
Verify gate: classification-currency claims (WHO5/ICC status) checked at write time.

---

## P3 — Biomarker & Molecular Overview
**One per cancer.** Consumes: ALL `biomarker_records` for the cancer (synthesis).
Hub for the Type B spokes.

The orientation to predictive/prognostic testing for this cancer.

Section skeleton:
1. **Why molecular testing matters here** — the shift from morphology-alone to
   biomarker-guided care.
2. **What gets tested** — the core panel for this cancer, grouped by purpose
   (diagnostic/lineage vs prognostic vs predictive/therapy-gating), each **linking
   down to its biomarker spoke (Type B)**.
3. **How results steer treatment** — the logic connecting a result to a therapy class
   (drug classes, not patient-directed advice).
4. **Specimen and testing realities** — tissue vs liquid, when reflex testing fires.
5. **What's emerging** — fast-moving additions (bridge toward P4 and the Pulse pages).
6. **References.**

Heavily verify-gated (it's all therapy linkage). Auto-links to every Type B spoke.

---

## P4 — Recent Developments
**One per cancer.** Consumes: substrate + **external literature/news** (Research stage
pulls current sources). The freshness pillar.

What's changed recently in diagnosis or biomarker-guided treatment for this cancer.

Section skeleton:
1. **The headline shifts** — the 2–4 most significant recent changes (new approvals,
   reclassifications, new tests), each externally sourced and dated.
2. **What it changes for diagnosis/testing** — grounded in the substrate (which
   records this touches), linking to relevant spokes.
3. **What's still settling** — contested or investigational areas, stated as such.
4. **References** — substrate + external, weighted external.

This is the most time-sensitive type and the one that dates fastest — strongest verify
gate, and a candidate for periodic regeneration rather than write-once. Editorially
adjacent to Digital/Liquid Pulse but tumor-specific.

---

## How pillars relate to the existing types

| Type | Scope | Consumes | Count |
|------|-------|----------|-------|
| P1 Explainer | per cancer | epidemiology file | 6 |
| P2 H&E/Classification | per cancer | all classification records (hub) | 6 |
| P3 Biomarker/Molecular | per cancer | all biomarker records (hub) | 6 |
| P4 Recent Developments | per cancer | substrate + external | 6 |
| A Subtype (spoke) | per record | one classification record | 54 |
| B Biomarker (spoke) | per record | one biomarker record | 56 |
| C Digital Pulse | cross-cancer | IHC/morphology slices + external | n |
| D Liquid Pulse | cross-cancer | ctDNA records + external | n |

Cancer-page total: 24 pillars + 110 spokes = 134 articles, before thematic lines.

## Build order (per the priority decision: pillars first)
1. Build the 6 epidemiology substrate files (new data, SEER/GLOBOCAN/ACS/USPSTF).
2. Wire P1–P4 as factory types (extend the substrate reader to enumerate pillars:
   4 per cancer, sourced from epi file + record-set summaries).
3. P2 and P3 are nearly free (pure synthesis of existing substrate).
4. P1 depends on the epi files. P4 depends on external Research.
5. Spokes (A/B) follow once pillars are live.

## Substrate-reader impact
`magpie_substrate.py` gains a `pillar_topics()` enumerator: for each cancer it emits
4 pillar candidates (P1–P4) with the appropriate consumes-payload (epi file for P1,
full record set for P2/P3, substrate+external flag for P4). Pillars get stable
topic_ids like the spokes so dedup holds. The link mesh extends: pillars link DOWN to
their spokes (P2→all Type A, P3→all Type B for the cancer).
