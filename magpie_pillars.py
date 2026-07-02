"""
magpie_pillars.py — Pillar (P1–P4) topic enumeration for Magpie Diagnostics.

Sits ABOVE the spokes that magpie_substrate.py enumerates (Type A classification,
Type B biomarker). This module does NOT modify magpie_substrate.py — it imports
SubstrateReader and layers the pillar enumeration on top, so the reader stays a
fixed dependency.

Pillars — exactly one of each per cancer (all active):
  P1  cancer explainer                — consumes <cancer>_epidemiology.json
  P2  H&E / classification overview   — consumes ALL classification_records (hub for Type A spokes)
  P3  biomarker / molecular overview  — consumes ALL biomarker_records (hub for Type B spokes)
  P4  recent developments             — substrate slice + EXTERNAL research (needs_external)

enumerate_thematic() adds the cross-cancer Pulse line: D (Liquid Pulse), one theme
per ctDNA-capable marker (also needs_external). C (Digital Pulse) is editorially
curated — it has no substrate enumeration source and is intentionally not produced.

Each pillar gets a stable, deterministic topic_id (so TopicGenerator dedup holds),
a consumes-payload with _provenance stripped, needs_verify=True (pillars make the
fast-moving currency / therapy-linkage claims), and the union of its records'
references. The pillar→spoke link mesh wires P2→every Type A spoke and P3→every
Type B spoke for the cancer (and the reverse edges).

No LLM, no network. Pure transformation — cheap to run every tick, trivial to test.
"""

from __future__ import annotations
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator

# Reuse the fixed reader and its id/strip helpers so pillar ids share the spoke
# id scheme (keeps dedup + the link mesh consistent across both layers).
from magpie_substrate import (
    SubstrateReader,
    CandidateTopic,
    _stable_id,
    _strip_provenance,
)

# Pillar specs: pillar_code -> (article_type, source kind, active?)
# Activate P1/P4 here once their data dependencies are wired.
PILLAR_SPECS = {
    "P2": {"article_type": "P2_classification_overview", "active": True},
    "P3": {"article_type": "P3_biomarker_overview", "active": True},
    "P1": {"article_type": "P1_explainer", "active": True},           # consumes <cancer>_epidemiology.json
    "P4": {"article_type": "P4_recent_developments", "active": True},  # external Research stage assembles sources
}

ACTIVE_PILLAR_CODES = tuple(code for code, s in PILLAR_SPECS.items() if s["active"])


@dataclass(frozen=True)
class PillarTopic:
    """One pillar article candidate (one per cancer per pillar code)."""
    topic_id: str            # stable, deterministic — used for dedup
    cancer: str
    article_type: str        # P2_classification_overview, P3_biomarker_overview, ...
    pillar_code: str         # P1 / P2 / P3 / P4
    title_hint: str          # human-readable seed, NOT the final title
    consumes: dict           # record set / payload for Write, _provenance already stripped
    needs_verify: bool       # pillars make currency/therapy claims → always verify-gated
    references: dict = field(default_factory=dict)  # {ref_key: resolved_reference}
    needs_external: bool = False  # P4/C/D: Research must assemble external sources first
    section: str = ""        # explicit section/category (thematic types; "" -> cancer map)

    def to_topic(self, site: str) -> dict:
        """Shape handed to the factory Topic Generator (mirrors CandidateTopic.to_topic)."""
        return {
            "topic_id": self.topic_id,
            "site": site,
            "article_type": self.article_type,
            "pillar_code": self.pillar_code,
            "cancer": self.cancer,
            "title_hint": self.title_hint,
            "needs_verify": self.needs_verify,
            "needs_external": self.needs_external,
            "consumes": self.consumes,
            "references": self.references,
            "section": self.section,
        }


def _resolve_refs(ref_keys: Iterable[str], references_index: dict) -> dict:
    """Resolve a list of reference keys to their full reference objects."""
    out: dict = {}
    for k in ref_keys:
        if k in references_index and k not in out:
            out[k] = references_index[k]
    return out


def _union_ref_keys(records: list) -> list:
    """Ordered union of all reference keys across a record set."""
    seen: list = []
    for rec in records:
        for k in rec.get("references", []):
            if k not in seen:
                seen.append(k)
    return seen


def pillar_topics(reader: SubstrateReader) -> Iterator[PillarTopic]:
    """Enumerate the active pillars for one cancer from an already-loaded reader."""
    cancer = reader.cancer

    # ── P1: cancer explainer / overview (consumes the epidemiology file) ──
    if PILLAR_SPECS["P1"]["active"]:
        epi_path = reader.dir / f"{cancer}_epidemiology.json"
        if epi_path.exists():
            with open(epi_path, encoding="utf-8") as f:
                epi_doc = _strip_provenance(json.load(f))
            cls_summary = [r.get("entity") for r in reader.classification.get("classification_records", []) if r.get("entity")]
            bio_summary = [r.get("biomarker") for r in reader.biomarkers.get("biomarker_records", []) if r.get("biomarker")]
            yield PillarTopic(
                topic_id=_stable_id(cancer, "P1"),
                cancer=cancer,
                article_type=PILLAR_SPECS["P1"]["article_type"],
                pillar_code="P1",
                title_hint=f"{cancer}: cancer explainer and overview",
                consumes={
                    "cancer": cancer,
                    "epidemiology": epi_doc.get("epidemiology", {}),
                    "classification_summary": cls_summary,   # orientation bridge -> P2
                    "biomarker_summary": bio_summary,         # orientation bridge -> P3
                },
                needs_verify=True,  # headline epidemiology stats are the fastest-aging content
                # epi has its own resolved refs (SEER/ACS/USPSTF), distinct from the spoke refs
                references=epi_doc.get("epidemiology_references", {}),
            )
        # else: epi file absent → P1 held for this cancer (no crash)

    # ── P2: H&E / classification overview (hub for Type A spokes) ──
    if PILLAR_SPECS["P2"]["active"]:
        records = [
            _strip_provenance(r)
            for r in reader.classification.get("classification_records", [])
        ]
        ref_keys = _union_ref_keys(records)
        yield PillarTopic(
            topic_id=_stable_id(cancer, "P2"),
            cancer=cancer,
            article_type=PILLAR_SPECS["P2"]["article_type"],
            pillar_code="P2",
            title_hint=f"{cancer}: H&E diagnosis and classification overview",
            consumes={
                "cancer": cancer,
                "classification_records": records,
                # currency signals the verify gate checks at write time (WHO5/ICC)
                "who5_notes": {
                    k: v for k, v in reader.classification.items()
                    if k not in ("classification_records",)
                },
            },
            needs_verify=True,  # classification-currency (WHO5/ICC) is checked at write time
            references=_resolve_refs(ref_keys, reader.references),
        )

    # ── P3: biomarker / molecular overview (hub for Type B spokes) ──
    if PILLAR_SPECS["P3"]["active"]:
        records = [
            _strip_provenance(r)
            for r in reader.biomarkers.get("biomarker_records", [])
        ]
        ref_keys = _union_ref_keys(records)
        yield PillarTopic(
            topic_id=_stable_id(cancer, "P3"),
            cancer=cancer,
            article_type=PILLAR_SPECS["P3"]["article_type"],
            pillar_code="P3",
            title_hint=f"{cancer}: biomarker and molecular testing overview",
            consumes={
                "cancer": cancer,
                "biomarker_records": records,
            },
            needs_verify=True,  # entirely therapy-linkage → heavily verify-gated
            references=_resolve_refs(ref_keys, reader.references),
        )

    # ── P4: recent developments (substrate slice + EXTERNAL research) ──
    if PILLAR_SPECS["P4"]["active"]:
        cls = reader.classification.get("classification_records", [])
        bio = reader.biomarkers.get("biomarker_records", [])
        substrate_slice = {
            "classification_entities": [r.get("entity") for r in cls if r.get("entity")],
            "biomarkers": [r.get("biomarker") for r in bio if r.get("biomarker")],
            "therapy_linkages": [
                {"biomarker": r.get("biomarker"), "therapy_linkage": r.get("therapy_linkage")}
                for r in bio if r.get("therapy_linkage")
            ],
        }
        yield PillarTopic(
            topic_id=_stable_id(cancer, "P4"),
            cancer=cancer,
            article_type=PILLAR_SPECS["P4"]["article_type"],
            pillar_code="P4",
            title_hint=f"{cancer}: recent developments in diagnosis and biomarker-guided treatment",
            consumes={"cancer": cancer, "substrate_slice": substrate_slice},
            needs_verify=False,        # external source assembly IS the verification for P4
            needs_external=True,       # Research must assemble current external sources first
            references={},             # external sources supply the citations
        )


def enumerate_thematic(substrate_root: str | Path,
                       cancers: Iterable[str]) -> list[PillarTopic]:
    """Cross-cancer thematic candidates that need external research.

    D (Liquid Pulse): one theme per ctDNA-capable marker, derived from the substrate's
    liquid-pulse seeds (biomarker records whose assay/specimen involves ctDNA/plasma).
    C (Digital Pulse): editorially curated themes read from Digital_pulse_themes.json
    (NOT substrate-derivable); skipped if that file is absent.
    """
    from magpie_substrate import liquid_pulse_seeds

    out: list[PillarTopic] = []
    by_marker: dict[str, list] = {}
    for s in liquid_pulse_seeds(substrate_root, cancers):
        name = (s.get("biomarker") or "").strip()
        if name:
            by_marker.setdefault(name, []).append(_strip_provenance(s))
    for marker, group in sorted(by_marker.items()):
        out.append(PillarTopic(
            topic_id=_stable_id("liquid", "D", marker.lower()),
            cancer="",                         # cross-cancer
            article_type="D_liquid",
            pillar_code="D",
            title_hint=f"Liquid Pulse: {marker} in plasma / ctDNA testing",
            consumes={"theme": f"{marker} ctDNA/plasma testing",
                      "ctdna_marker_slice": group},
            needs_verify=False,
            needs_external=True,
            references={},
            section="liquid-pulse",
        ))

    # C (Digital Pulse): editorially curated themes (NOT substrate-derivable). Read from
    # Digital_pulse_themes.json alongside this module; skipped if the file is absent.
    themes_path = Path(__file__).resolve().parent / "Digital_pulse_themes.json"
    if themes_path.exists():
        with open(themes_path, encoding="utf-8") as f:
            themes_doc = json.load(f)
        for t in themes_doc.get("themes", []):
            theme = (t.get("theme") or "").strip()
            if not theme:
                continue
            out.append(PillarTopic(
                topic_id=_stable_id("digital", "C", theme.lower()),
                cancer="",                     # cross-cancer
                article_type="C_digital",
                pillar_code="C",
                title_hint=f"Digital Pulse: {theme}",
                consumes={
                    "theme": theme,
                    # status_hint is an editorial seed only; Research confirms status at write time
                    "substrate_slice": {
                        "markers": t.get("substrate_markers", []),
                        "cancers": t.get("cancers", []),
                        "notes": t.get("notes", ""),
                    },
                },
                needs_verify=False,
                needs_external=True,
                references={},
                section="digital-pulse",
            ))
    return out


def enumerate_pillars(substrate_root: str | Path,
                      cancers: Iterable[str]) -> list[PillarTopic]:
    """Enumerate every active pillar across all provided cancers."""
    out: list[PillarTopic] = []
    for cancer in cancers:
        reader = SubstrateReader(Path(substrate_root) / cancer, cancer)
        out.extend(pillar_topics(reader))
    return out


def pillar_spoke_mesh(pillars: list[PillarTopic],
                      spokes: list[CandidateTopic]) -> dict[str, list[str]]:
    """Hub→spoke link mesh: P2→every Type A spoke, P3→every Type B spoke (cancer-scoped),
    plus the reverse spoke→pillar edges. Returns {topic_id: [linked_topic_id, ...]}.

    Merge this with magpie_substrate.build_link_mesh(spokes) for the full mesh.
    """
    mesh: dict[str, list[str]] = {}

    def add(a: str, b: str) -> None:
        mesh.setdefault(a, [])
        if b not in mesh[a]:
            mesh[a].append(b)

    spokes_by_cancer: dict[str, list[CandidateTopic]] = {}
    for s in spokes:
        spokes_by_cancer.setdefault(s.cancer, []).append(s)

    hub_type_for = {"P2": "A_classification", "P3": "B_biomarker"}
    for p in pillars:
        spoke_type = hub_type_for.get(p.pillar_code)
        if not spoke_type:
            continue  # P1/P4 don't hub to a single spoke type
        for s in spokes_by_cancer.get(p.cancer, []):
            if s.article_type == spoke_type:
                add(p.topic_id, s.topic_id)
                add(s.topic_id, p.topic_id)

    return {k: sorted(set(v)) for k, v in mesh.items()}


if __name__ == "__main__":
    import sys
    from magpie_substrate import enumerate_all, build_link_mesh

    root = sys.argv[1] if len(sys.argv) > 1 else "substrate"
    cancers = ["breast", "lymphoma", "lung", "colon", "prostate", "ovarian"]

    spokes = enumerate_all(root, cancers)
    pillars = enumerate_pillars(root, cancers)

    print(f"Active pillar codes: {ACTIVE_PILLAR_CODES}")
    print(f"Spokes: {len(spokes)}  |  Pillars: {len(pillars)}")
    by_type: dict[str, int] = {}
    for p in pillars:
        by_type[p.article_type] = by_type.get(p.article_type, 0) + 1
    print("Pillars by type:", by_type)

    ps_mesh = pillar_spoke_mesh(pillars, spokes)
    spoke_mesh = build_link_mesh(spokes)
    linked_pillars = sum(1 for p in pillars if ps_mesh.get(p.topic_id))
    print(f"Pillars with >=1 spoke link: {linked_pillars}/{len(pillars)}")
    print(f"Spoke-only mesh entries with links: {sum(1 for v in spoke_mesh.values() if v)}")
