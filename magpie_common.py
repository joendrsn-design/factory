"""
magpie_common.py — shared helpers for the Magpie Diagnostics substrate content line.

Keeps the Magpie-specific branching in the shared factory modules tiny and uniform.
Every shared module guards its Magpie branch with `is_magpie(metadata)`, and copies
the substrate identity/payload forward with `carry_magpie(...)`, so a non-Magpie
artifact takes the exact original code path (carry_magpie is a no-op for it).

The Magpie passthrough blob lives at metadata["magpie"] and carries:
    topic_id        stable substrate id (dedup + link mesh)
    article_type    P2_classification_overview | P3_biomarker_overview | A_* | B_* | ...
    cancer          breast | colon | lung | prostate | lymphoma | ovarian | "" (thematic)
    section         category slug the article publishes under
    needs_verify    bool — gate therapy-linkage / currency claims in Research
    references      {ref_key: resolved_reference}
    pillar_code     P1..P4   (pillars only)
    consumes        record set for Write   (pillars only)
    record_id       substrate record id    (spokes only)
    source_record   single record for Write (spokes only)

`_provenance` is stripped at substrate read; strip_provenance_deep() is belt-and-
suspenders so nothing re-introduces it on the way to Write.
"""
from __future__ import annotations


def is_magpie(metadata: dict) -> bool:
    """True if this artifact belongs to the Magpie substrate content line."""
    return bool(metadata.get("magpie"))


def carry_magpie(input_meta: dict, out_meta: dict) -> dict:
    """Copy the Magpie blob (and mirror topic_id at top level) from input to output.

    No-op for non-Magpie artifacts — so every shared module's non-Magpie path is
    byte-for-byte unchanged.
    """
    blob = input_meta.get("magpie")
    if blob:
        out_meta["magpie"] = blob
        if blob.get("topic_id"):
            out_meta["topic_id"] = blob["topic_id"]
    return out_meta


def strip_provenance_deep(obj):
    """Recursively drop any `_provenance` key. Defensive: the substrate reader already
    strips it, but Write must NEVER see it, so we re-assert on the path."""
    if isinstance(obj, dict):
        return {k: strip_provenance_deep(v) for k, v in obj.items() if k != "_provenance"}
    if isinstance(obj, list):
        return [strip_provenance_deep(v) for v in obj]
    return obj


def references_for_write(references: dict) -> dict:
    """Resolved references minus the internal `verify` guidance field (used by the
    Research verify gate, not by the writer)."""
    out = {}
    for key, ref in (references or {}).items():
        if isinstance(ref, dict):
            out[key] = {k: v for k, v in ref.items() if k != "verify"}
        else:
            out[key] = ref
    return out
