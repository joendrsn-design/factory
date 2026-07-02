"""
============================================================
ARTICLE FACTORY — ARTIFACT I/O
============================================================
Handles reading and writing artifacts between modules.

Format: Markdown files with YAML frontmatter.
  - Frontmatter carries structured data (machine-readable)
  - Body carries content (human-readable, LLM-consumable)
  - Files live in pipeline folders, one per stage

Modules interact with artifacts through this layer only.
Never write raw file I/O in a module.
============================================================
"""

import os
import re
import uuid
import yaml
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("article_factory.artifacts")


# ── LLM JSON Extraction ─────────────────────────────────────
# Shared by planning.py and research.py. LLM responses wrap JSON in ```json
# fences, prepend prose, or omit fences entirely; these recover the JSON object
# and (for two-part responses) the surrounding prose.

_JSON_DECODER = json.JSONDecoder()


def extract_json(text: str) -> Optional[dict]:
    """Return the first JSON object in an LLM response, or None if there isn't one.

    Tolerates ```json / ``` fences (including single-line / missing-newline
    variants) and surrounding prose. The raw_decode scan respects string
    contents, so a "{" or "}" inside a value won't derail extraction.
    """
    if not text:
        return None
    text = text.strip()
    # 1. Fenced code blocks: ```json ... ``` first, then a bare ``` ... ```.
    for pat in (r"```json\s*(.*?)```", r"```\s*(.*?)```"):
        for m in re.finditer(pat, text, re.DOTALL):
            try:
                obj = json.loads(m.group(1).strip())
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                return obj
    # 2. Raw-decode any JSON object found in the text.
    idx = text.find("{")
    while idx != -1:
        try:
            obj, _ = _JSON_DECODER.raw_decode(text, idx)
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass
        idx = text.find("{", idx + 1)
    return None


def strip_json_blocks(text: str) -> str:
    """Remove fenced JSON blocks and a leading unfenced JSON object from an LLM
    response, leaving the prose (e.g. the PART 2 research brief)."""
    if not text:
        return ""
    # Remove fenced metadata block(s): ```json ... ``` or a bare ``` { ... } ```.
    cleaned = re.sub(r"```json\s*.*?```", "", text, flags=re.DOTALL)
    cleaned = re.sub(r"```\s*\{.*?\}\s*```", "", cleaned, flags=re.DOTALL).strip()
    # Remove a leading unfenced JSON object (optionally behind a "PART 1:" header).
    m = re.match(r"\s*(?:PART\s*1\s*:?\s*)?\{", cleaned, flags=re.IGNORECASE)
    if m:
        try:
            _, end = _JSON_DECODER.raw_decode(cleaned, m.end() - 1)
            cleaned = cleaned[end:].strip()
        except json.JSONDecodeError:
            pass
    return cleaned


# ── ID Generation ───────────────────────────────────────────

def new_run_id() -> str:
    return f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

def new_article_id() -> str:
    return f"art_{uuid.uuid4().hex[:12]}"


# ── Markdown + Frontmatter I/O ──────────────────────────────

def save_artifact(metadata: dict, body: str, output_dir: str, filename: str = "") -> Path:
    """
    Save artifact as markdown with YAML frontmatter.
    Returns path to saved file.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if not filename:
        article_id = metadata.get("article_id", new_article_id())
        module = metadata.get("module", "unknown")
        filename = f"{article_id}_{module}.md"

    filepath = output_path / filename
    frontmatter = yaml.dump(metadata, default_flow_style=False, sort_keys=False, allow_unicode=True)
    content = f"---\n{frontmatter}---\n\n{body}"

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    logger.debug(f"Saved artifact: {filepath}")
    return filepath


def load_artifact(filepath: str) -> tuple[dict, str]:
    """
    Load artifact. Returns (metadata_dict, body_string).
    """
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    return parse_frontmatter(content)


def parse_frontmatter(content: str) -> tuple[dict, str]:
    """Parse markdown with YAML frontmatter into (metadata, body)."""
    pattern = r"^---\s*\n(.*?)\n---\s*\n(.*)"
    match = re.match(pattern, content, re.DOTALL)

    if not match:
        return {}, content.strip()

    try:
        metadata = yaml.safe_load(match.group(1)) or {}
    except yaml.YAMLError as e:
        logger.error(f"Failed to parse frontmatter: {e}")
        metadata = {}

    return metadata, match.group(2).strip()


def load_artifacts_from_dir(
    directory: str,
    module_filter: str = None,
    status_filter: str = None,
    site_filter: str = None,
    run_filter: str = None,
) -> list[tuple[dict, str, str]]:
    """
    Load all artifacts from a directory.
    Returns list of (metadata, body, filepath) tuples.
    """
    artifacts = []
    dir_path = Path(directory)

    if not dir_path.exists():
        return artifacts

    for f in sorted(dir_path.glob("*.md")):
        try:
            metadata, body = load_artifact(str(f))
        except Exception as e:
            logger.error(f"Failed to load {f}: {e}")
            continue

        if module_filter and metadata.get("module") != module_filter:
            continue
        if status_filter and metadata.get("status") != status_filter:
            continue
        if site_filter and metadata.get("site_id") != site_filter:
            continue
        if run_filter and metadata.get("run_id") != run_filter:
            continue

        artifacts.append((metadata, body, str(f)))

    return artifacts


# ── Metadata Constructors ───────────────────────────────────

def base_metadata(run_id: str, article_id: str, site_id: str, module: str, article_type: str = "") -> dict:
    return {
        "run_id": run_id,
        "article_id": article_id,
        "site_id": site_id,
        "article_type": article_type,
        "module": module,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "complete",
    }


def topic_metadata(run_id, article_id, site_id, article_type, topic, keywords, angle="") -> dict:
    meta = base_metadata(run_id, article_id, site_id, "topic_generator", article_type)
    meta.update({"topic": topic, "keywords": keywords, "angle": angle})
    return meta


def research_metadata(run_id, article_id, site_id, article_type, topic, research_depth,
                       source_count, key_findings, sources, from_cache=False, statistics=None) -> dict:
    meta = base_metadata(run_id, article_id, site_id, "research", article_type)
    meta.update({
        "topic": topic, "research_depth": research_depth, "source_count": source_count,
        "key_findings": key_findings, "sources": sources, "from_cache": from_cache,
        "statistics": statistics or [],
    })
    return meta


def plan_metadata(run_id, article_id, site_id, article_type, topic, title, slug,
                   target_word_count, seo_title, meta_description, target_keywords,
                   internal_links, section_count) -> dict:
    meta = base_metadata(run_id, article_id, site_id, "planning", article_type)
    meta.update({
        "topic": topic, "title": title, "slug": slug, "target_word_count": target_word_count,
        "seo_title": seo_title, "meta_description": meta_description,
        "target_keywords": target_keywords, "internal_links": internal_links,
        "section_count": section_count,
    })
    return meta


def article_metadata(run_id, article_id, site_id, article_type, title, slug,
                      word_count, seo_title, meta_description, tags,
                      featured_image="", category="") -> dict:
    meta = base_metadata(run_id, article_id, site_id, "write", article_type)
    meta.update({
        "title": title, "slug": slug, "word_count": word_count,
        "seo_title": seo_title, "meta_description": meta_description, "tags": tags,
        "featured_image": featured_image, "category": category,
    })
    return meta


def qa_metadata(run_id, article_id, site_id, article_type, verdict, score,
                 scores_breakdown, feedback="", rewrite_instructions="", rewrite_count=0) -> dict:
    meta = base_metadata(run_id, article_id, site_id, "qa", article_type)
    meta.update({
        "verdict": verdict, "score": score, "scores_breakdown": scores_breakdown,
        "feedback": feedback, "rewrite_instructions": rewrite_instructions,
        "rewrite_count": rewrite_count,
    })
    return meta


def angle_metadata(run_id, article_id, site_id, parent_article_id, expansion_index,
                    angle_description, suggested_title, suggested_category, article_type,
                    topic, unique_hook="", tone_adjustment="", target_audience="",
                    key_findings=None, sources=None) -> dict:
    """Metadata constructor for expansion module output (angle artifacts)."""
    meta = base_metadata(run_id, article_id, site_id, "expansion", article_type)
    meta.update({
        "parent_article_id": parent_article_id,
        "expansion_index": expansion_index,
        "angle_description": angle_description,
        "suggested_title": suggested_title,
        "suggested_category": suggested_category,
        "topic": topic,
        "unique_hook": unique_hook,
        "tone_adjustment": tone_adjustment,
        "target_audience": target_audience,
        "key_findings": key_findings or [],
        "sources": sources or [],
    })
    return meta


# ── Batch Tracking ──────────────────────────────────────────

def save_batch_manifest(batch_id: str, module: str, article_ids: list[str], output_dir: str) -> Path:
    """Save a batch manifest after submitting to Anthropic Batch API."""
    manifest = {
        "batch_id": batch_id,
        "module": module,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
        "article_count": len(article_ids),
        "article_ids": article_ids,
        "status": "submitted",
    }
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    filepath = output_path / f"batch_{batch_id}.json"
    with open(filepath, "w") as f:
        json.dump(manifest, f, indent=2)
    logger.info(f"Saved batch manifest: {filepath}")
    return filepath


def load_batch_manifest(filepath: str) -> dict:
    with open(filepath, "r") as f:
        return json.load(f)


def find_latest_batch_manifest(directory: str, module: str = None) -> Optional[dict]:
    """Find the most recent batch manifest in a directory."""
    dir_path = Path(directory)
    if not dir_path.exists():
        return None
    manifests = []
    for f in dir_path.glob("batch_*.json"):
        manifest = load_batch_manifest(str(f))
        if module and manifest.get("module") != module:
            continue
        manifests.append(manifest)
    if not manifests:
        return None
    return sorted(manifests, key=lambda m: m.get("submitted_at", ""), reverse=True)[0]
