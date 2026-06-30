"""
magpie_substrate.py — Substrate-driven topic enumeration for Magpie Diagnostics sites.

Highest-leverage pipeline piece: turns the per-cancer substrate JSON
(classification + biomarker records) into an enumerable, deduplicated, state-tracked
work queue of candidate articles — one per record.

Designed to be the substrate-source mode for the existing factory Topic Generator.
It does NOT replace TopicGenerator; it feeds it. TopicGenerator still owns
publishing-history dedup and queue emission. This module's job is:

  substrate JSON  ->  normalized candidate topics (id, type, cancer, payload)

INTERFACE ASSUMPTIONS (verify against real factory before wiring):
- TopicGenerator consumes an iterable of "topic" dicts with at least:
    {topic_id, site, article_type, title_hint, source_record}
- Publishing-history dedup lives in TopicGenerator (we expose a stable topic_id
  so it can dedup deterministically).
- Site routing: cancer pages -> their cancer site/section; Digital Pulse and
  Liquid Pulse are cross-cancer and assembled separately (see DIGITAL/LIQUID below).

This module has NO LLM calls and NO network. Pure transformation. That makes it
cheap to run on every pipeline tick and trivial to unit-test.
"""

from __future__ import annotations
import json
import hashlib
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Iterable, Iterator, Literal

ArticleType = Literal["A_classification", "B_biomarker", "C_digital", "D_liquid"]

# Which substrate assay/specimen strings mark a record as ctDNA/plasma-capable
# (feeds Liquid Pulse candidate selection). Lowercased substring match.
LIQUID_MARKERS = ("ctdna", "plasma", "liquid biopsy", "cell-free")


@dataclass(frozen=True)
class CandidateTopic:
    """One enumerable article candidate derived from a substrate record."""
    topic_id: str            # stable, deterministic — used for dedup
    cancer: str              # breast, lung, colon, prostate, ovarian, lymphoma
    article_type: ArticleType
    record_id: str           # e.g. BR-BIO-0003
    title_hint: str          # human-readable seed, NOT the final title
    source_record: dict      # the full record, _provenance already stripped
    needs_verify: bool       # True if any ref is verify-flagged or type is biomarker
    references: list = field(default_factory=list)

    def to_topic(self, site: str) -> dict:
        """Shape expected by the factory Topic Generator (assumed interface)."""
        return {
            "topic_id": self.topic_id,
            "site": site,
            "article_type": self.article_type,
            "title_hint": self.title_hint,
            "record_id": self.record_id,
            "needs_verify": self.needs_verify,
            "source_record": self.source_record,
            "references": self.references,
        }


def _stable_id(*parts: str) -> str:
    """Deterministic short id so re-runs dedup cleanly in TopicGenerator."""
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode()).hexdigest()[:12]


def _strip_provenance(record: dict) -> dict:
    """Remove internal-only audit field before the record reaches any writer."""
    return {k: v for k, v in record.items() if k != "_provenance"}


def _ref_needs_verify(ref_keys: list, references_index: dict) -> bool:
    """A record needs verification if any of its refs carries the structured `verify`
    directive (a truthy value on the reference's `verify` KEY). The directive's text is
    guidance for the verifier (e.g. 'Confirm subtype list and any corrigenda.') and does
    not itself contain the word 'verify', so read the key, not the serialized values."""
    for k in ref_keys:
        entry = references_index.get(k, {})
        if isinstance(entry, dict) and entry.get("verify"):
            return True
    return False


class SubstrateReader:
    """Loads one cancer's three-file substrate and enumerates candidate topics."""

    def __init__(self, substrate_dir: str | Path, cancer: str):
        self.dir = Path(substrate_dir)
        self.cancer = cancer
        self.classification = self._load(f"{cancer}_classification.json")
        self.biomarkers = self._load(f"{cancer}_biomarkers.json")
        self.references = self._load(f"{cancer}_references.json").get("references", {})

    def _load(self, name: str) -> dict:
        path = self.dir / name
        if not path.exists():
            raise FileNotFoundError(f"Missing substrate file: {path}")
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    def classification_topics(self) -> Iterator[CandidateTopic]:
        for rec in self.classification.get("classification_records", []):
            rid = rec.get("id") or rec.get("record_id")
            if not rid:
                continue  # skip a malformed record rather than KeyError-aborting the run
            entity = rec.get("entity", rid)
            refs = rec.get("references", [])
            yield CandidateTopic(
                topic_id=_stable_id(self.cancer, "A", rid),
                cancer=self.cancer,
                article_type="A_classification",
                record_id=rid,
                title_hint=f"{entity}: classification and diagnostic recognition",
                source_record=_strip_provenance(rec),
                needs_verify=_ref_needs_verify(refs, self.references),
                references=refs,
            )

    def biomarker_topics(self) -> Iterator[CandidateTopic]:
        for rec in self.biomarkers.get("biomarker_records", []):
            rid = rec.get("id") or rec.get("record_id")
            if not rid:
                continue  # skip a malformed record rather than KeyError-aborting the run
            marker = rec.get("biomarker", rid)
            refs = rec.get("references", [])
            yield CandidateTopic(
                topic_id=_stable_id(self.cancer, "B", rid),
                cancer=self.cancer,
                article_type="B_biomarker",
                record_id=rid,
                title_hint=f"{marker}: what it tests and what it determines",
                source_record=_strip_provenance(rec),
                # biomarker articles ALWAYS verify (therapy linkage is the
                # fast-moving, liability-bearing claim — see HER2-ultralow catch)
                needs_verify=True,
                references=refs,
            )

    def all_topics(self) -> Iterator[CandidateTopic]:
        yield from self.classification_topics()
        yield from self.biomarker_topics()

    def liquid_candidates(self) -> Iterator[dict]:
        """ctDNA/plasma-capable biomarker records -> Liquid Pulse seeds (cross-cancer)."""
        for rec in self.biomarkers.get("biomarker_records", []):
            blob = json.dumps(rec).lower()
            if any(m in blob for m in LIQUID_MARKERS):
                yield {
                    "cancer": self.cancer,
                    "record_id": rec.get("id") or rec.get("record_id"),
                    "biomarker": rec.get("biomarker"),
                    "source_record": _strip_provenance(rec),
                }


def enumerate_all(substrate_root: str | Path,
                  cancers: Iterable[str]) -> list[CandidateTopic]:
    """Enumerate every cancer-page candidate across all provided cancers."""
    out: list[CandidateTopic] = []
    for cancer in cancers:
        reader = SubstrateReader(Path(substrate_root) / cancer, cancer)
        out.extend(reader.all_topics())
    return out


def liquid_pulse_seeds(substrate_root: str | Path,
                       cancers: Iterable[str]) -> list[dict]:
    """Cross-cancer ctDNA/plasma records that seed Liquid Pulse articles.
    External literature is layered on later by the Research stage."""
    seeds: list[dict] = []
    for cancer in cancers:
        reader = SubstrateReader(Path(substrate_root) / cancer, cancer)
        seeds.extend(reader.liquid_candidates())
    return seeds


def build_link_mesh(topics: list[CandidateTopic]) -> dict[str, list[str]]:
    """Derive the internal-link mesh from the substrate, no manual linking.

    A-record (subtype) links to B-records (biomarkers) whose marker name appears
    in the subtype's ihc_signature or molecular_signature, and vice versa.
    Returns {topic_id: [linked_topic_id, ...]}. Cancer-scoped (no cross-cancer
    links on cancer pages; Digital/Liquid Pulse handle cross-cancer).
    """
    mesh: dict[str, list[str]] = {t.topic_id: [] for t in topics}
    by_cancer: dict[str, list[CandidateTopic]] = {}
    for t in topics:
        by_cancer.setdefault(t.cancer, []).append(t)

    for cancer, group in by_cancer.items():
        subs = [t for t in group if t.article_type == "A_classification"]
        bios = [t for t in group if t.article_type == "B_biomarker"]
        for sub in subs:
            # Guard against a record whose signature field is null or a string (not a
            # list) — otherwise one bad record raises TypeError and collapses the whole mesh.
            ihc = sub.source_record.get("ihc_signature")
            mol = sub.source_record.get("molecular_signature")
            ihc = ihc if isinstance(ihc, list) else []
            mol = mol if isinstance(mol, list) else []
            sig = " ".join(ihc + mol).lower()
            for bio in bios:
                marker = (bio.source_record.get("biomarker") or "").lower()
                # match on the marker's leading token (e.g. "her2", "bcl2", "egfr")
                token = marker.split("(")[0].split("/")[0].strip().split()[0] if marker else ""
                if token and len(token) >= 3 and token in sig:
                    mesh[sub.topic_id].append(bio.topic_id)
                    mesh[bio.topic_id].append(sub.topic_id)
    # dedup
    return {k: sorted(set(v)) for k, v in mesh.items()}


if __name__ == "__main__":
    import sys
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    cancers = ["breast", "lymphoma", "lung", "colon", "prostate", "ovarian"]
    topics = enumerate_all(root, cancers)
    mesh = build_link_mesh(topics)
    liquid = liquid_pulse_seeds(root, cancers)

    print(f"Total cancer-page candidates: {len(topics)}")
    by_type: dict[str, int] = {}
    by_cancer: dict[str, int] = {}
    verify_ct = 0
    for t in topics:
        by_type[t.article_type] = by_type.get(t.article_type, 0) + 1
        by_cancer[t.cancer] = by_cancer.get(t.cancer, 0) + 1
        verify_ct += int(t.needs_verify)
    print("By type:", by_type)
    print("By cancer:", by_cancer)
    print(f"Need verify gate: {verify_ct}/{len(topics)}")
    linked = sum(1 for v in mesh.values() if v)
    print(f"Topics with >=1 internal link: {linked}")
    print(f"Liquid Pulse seeds (ctDNA/plasma records): {len(liquid)}")
