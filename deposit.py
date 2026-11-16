"""
============================================================
ARTICLE FACTORY — DEPOSIT MODULE (API-First)
============================================================
No LLM. Pure publishing.

Takes QA-passed articles (verdict=PUBLISH) and publishes them
to Site Empire via API. Disk write is a FALLBACK for failures
so we never lose work.

What it does:
  1. Scans QA output for PUBLISH verdicts
  2. Builds a publish payload (clean of internal pipeline metadata)
  3. POSTs to Site Empire /api/publish (primary)
  4. On API failure: writes to a quarantine folder so the article isn't lost
  5. Generates a run summary report

Modes (set with DEPOSIT_MODE env var, default 'api'):
  api          — API only. Disk write only on API failure (quarantine).
  api+disk     — API primary, ALSO write a copy to CONTENT_ROOT every time.
  disk         — Disk only (legacy mode, no API push).

Env vars:
  FACTORY_API_KEY      — Required for API mode (bearer token)
  SITE_EMPIRE_URL      — Defaults to https://site-empire.vercel.app
  DEPOSIT_MODE         — api | api+disk | disk (default: api)
  CONTENT_ROOT         — Where to write disk copies (default: sites/)
  DEPOSIT_FAILED_DIR   — Where to quarantine failed publishes (default: pipeline/failed_publishes)

Usage:
    python deposit.py
    python deposit.py --dry-run
    python deposit.py --site lamphill
    python deposit.py --mode api+disk
============================================================
"""

import os
import re
import json
import shutil
import yaml
import logging
import argparse
import tempfile
import requests
import subprocess
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

from site_loader import SiteLoader, SiteContext
from artifacts import load_artifacts_from_dir
from p4_gates import run_p4_gates, update_article_status
from models_config import MODELS

logger = logging.getLogger("article_factory.deposit")


# ── Body Cleanup ────────────────────────────────────────────

def clean_article_body(body: str) -> str:
    """Remove pipeline artifacts from the body."""
    body = re.sub(r"\n---\n\n```json\n.*?\n```", "", body, flags=re.DOTALL)
    body = re.sub(r"\n{4,}", "\n\n\n", body)
    return body.strip()


# ── Deslop (last step before publishing) ────────────────────

def deslop_body(body: str, article_id: str) -> tuple[str, Optional[int]]:
    """
    Final AI-slop pass, run as the last step before an article is published.

    Uses the deterministic, zero-dependency scanner at pipeline/deslop/slop.mjs
    (no LLM, no network). Loops the `--fix` pass (safe word/phrase swaps, in
    place) and re-scores after each, stopping when the score drops below
    DESLOP_TARGET_SCORE, when a pass stops helping (plateau), or at the cap.

    Returns (possibly-cleaned body, max slop score or None). FAILS OPEN: on any
    error (node missing, timeout, unparseable output) it returns the original
    body and None, so tooling problems never block publishing.

    Env controls:
      DESLOP_ENABLED       (default "1")  — "0" bypasses the pass entirely
      DESLOP_FIX           (default "1")  — "0" scores only, no auto-fix
      DESLOP_TARGET_SCORE  (default "15") — loop fixes until below this
      DESLOP_MAX_PASSES    (default "10") — hard cap on fix passes
    """
    if os.environ.get("DESLOP_ENABLED", "1").lower() not in ("1", "true", "yes"):
        return body, None

    scanner = Path(__file__).resolve().parent / "pipeline" / "deslop" / "slop.mjs"
    node = shutil.which("node")
    if not node or not scanner.exists():
        logger.warning("[deslop] node or slop.mjs not found; skipping (fail-open)")
        return body, None

    tmp = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", suffix=".md", delete=False, encoding="utf-8"
        ) as tf:
            tf.write(body)
            tmp = tf.name

        def _score() -> Optional[int]:
            res = subprocess.run(
                [node, str(scanner), tmp, "--json"],
                capture_output=True, text=True, timeout=120,
            )
            try:
                data = json.loads(res.stdout)
                return max((f.get("score", 0) for f in data.get("files", [])), default=0)
            except (ValueError, TypeError):
                return None

        target = int(os.environ.get("DESLOP_TARGET_SCORE", "15"))
        max_passes = int(os.environ.get("DESLOP_MAX_PASSES", "10"))
        do_fix = os.environ.get("DESLOP_FIX", "1").lower() in ("1", "true", "yes")

        # Loop the deterministic --fix pass until the score drops below the
        # target, the fixes stop helping (plateau), or we hit the pass cap.
        # --fix is deterministic, so it converges fast — the plateau check and
        # the cap both guard against an unbounded loop.
        score = _score()
        passes = 0
        while do_fix and score is not None and score >= target and passes < max_passes:
            subprocess.run(
                [node, str(scanner), tmp, "--fix"],
                capture_output=True, timeout=120,
            )
            passes += 1
            new_score = _score()
            if new_score is None or new_score >= score:
                score = new_score if new_score is not None else score
                break  # no further improvement — stop rather than spin
            score = new_score

        with open(tmp, encoding="utf-8") as f:
            cleaned = f.read()
        if passes:
            logger.info(
                f"[deslop] {article_id}: {passes} fix pass(es), final score {score}"
            )
        return cleaned, score
    except Exception as e:  # noqa: BLE001 — never let deslop break a publish
        logger.warning(
            f"[deslop] scan failed for {article_id}; publishing unchanged "
            f"(fail-open): {e}"
        )
        return body, None
    finally:
        if tmp and os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass


def _deslop_tool() -> tuple[Optional[str], Optional[str]]:
    """Locate (node, slop.mjs). Returns (None, None) if either is missing."""
    scanner = Path(__file__).resolve().parent / "pipeline" / "deslop" / "slop.mjs"
    node = shutil.which("node")
    if not node or not scanner.exists():
        return None, None
    return node, str(scanner)


def deslop_rewrite_once(
    body: str, article_id: str, site_context: Optional[SiteContext] = None
) -> Optional[str]:
    """
    One LLM pass that rewrites the structural slop `--fix` can't swap
    (construct / vague-this / wordy / rhythm findings).

    Targets the scanner's own findings and is told to keep every fact, number,
    name, quote, citation, link and heading, preserve the site voice, and hold
    length within ~10%. Returns the rewritten markdown, or None when the rewrite
    is unavailable or looks unsafe (caller then keeps the prior body).

    Env: DESLOP_REWRITE_MODEL (default "haiku").
    """
    node, scanner = _deslop_tool()
    if not node:
        return None

    # Gather the current findings to hand the model.
    tmp, findings = None, []
    try:
        with tempfile.NamedTemporaryFile(
            "w", suffix=".md", delete=False, encoding="utf-8"
        ) as tf:
            tf.write(body)
            tmp = tf.name
        res = subprocess.run(
            [node, scanner, tmp, "--json"],
            capture_output=True, text=True, timeout=120,
        )
        data = json.loads(res.stdout)
        for f in data.get("files", []):
            findings.extend(f.get("findings", []))
    except Exception:
        return None
    finally:
        if tmp and os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass

    if not findings:
        return None

    finding_lines = []
    for f in findings[:60]:
        finding_lines.append(
            f"- [{f.get('category','')}] {f.get('label','')} "
            f"@ L{f.get('line','?')}: \"{(f.get('text','') or '').strip()}\""
        )
    findings_block = "\n".join(finding_lines)

    voice = getattr(site_context, "voice", {}) or {}
    voice_note = (
        f"\nKeep this voice — tone: {voice.get('tone','')}; "
        f"persona: {voice.get('persona','')}."
        if voice else ""
    )

    system_prompt = (
        "You are a line editor removing AI-slop from a publish-ready article. "
        "Rewrite ONLY to eliminate the listed slop. Keep every fact, number, name, "
        "quote, citation, link and heading exactly. Do not touch frontmatter, code "
        "blocks or markdown structure. Keep the author's voice and keep length within "
        "about 10 percent. Replace stock phrases and vague 'this' with the specific "
        "thing meant; turn 'not just X, it's Y' into a direct claim; split sentences "
        "over 35 words; cut filler. Do not introduce new cliches. Return ONLY the "
        "rewritten markdown, nothing else."
    )
    user_message = f"SLOP TO FIX:\n{findings_block}{voice_note}\n\nARTICLE:\n{body}"

    try:
        import anthropic
        client = anthropic.Anthropic()
        model = MODELS.get(
            os.environ.get("DESLOP_REWRITE_MODEL", "haiku"), MODELS["haiku"]
        )
        resp = client.messages.create(
            model=model,
            max_tokens=8000,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )
        out = "".join(
            b.text for b in resp.content if getattr(b, "type", "") == "text"
        ).strip()
    except Exception as e:  # noqa: BLE001 — fail open to the fix-only body
        logger.warning(
            f"[deslop] rewrite call failed for {article_id} (fail-open): {e}"
        )
        return None

    if not out:
        return None
    # Reject a rewrite that changed length wildly (truncation / hallucination).
    ratio = len(out) / max(1, len(body))
    if ratio < 0.6 or ratio > 1.5:
        logger.warning(
            f"[deslop] rewrite for {article_id} rejected: length ratio {ratio:.2f}"
        )
        return None
    return out


def deslop_to_target(
    body: str, article_id: str, site_context: Optional[SiteContext] = None
) -> tuple[str, Optional[int]]:
    """
    Drive the slop score below DESLOP_TARGET_SCORE: run the deterministic
    fix-loop, then (if still at/above target) an LLM rewrite loop. Each rewrite
    is followed by the fix-loop and kept only if it lowers the score; stops at
    the target, when a rewrite fails to help, or at DESLOP_MAX_REWRITES.

    Env: DESLOP_REWRITE (default "1"), DESLOP_MAX_REWRITES (default "3").
    """
    body, score = deslop_body(body, article_id)
    target = int(os.environ.get("DESLOP_TARGET_SCORE", "15"))
    rewrite_on = os.environ.get("DESLOP_REWRITE", "1").lower() in ("1", "true", "yes")
    max_rewrites = int(os.environ.get("DESLOP_MAX_REWRITES", "3"))

    passes = 0
    while rewrite_on and score is not None and score >= target and passes < max_rewrites:
        candidate = deslop_rewrite_once(body, article_id, site_context)
        passes += 1
        if not candidate:
            break  # unavailable or unsafe — keep the best body so far
        cleaned, new_score = deslop_body(candidate, article_id)
        if new_score is None or new_score >= score:
            break  # rewrite didn't help — keep the prior (>=) body
        body, score = cleaned, new_score
        logger.info(f"[deslop] {article_id}: rewrite pass {passes} -> score {score}")

    return body, score


# ── Publish Payload ─────────────────────────────────────────

def build_publish_payload(metadata: dict, body: str, site_context: SiteContext) -> dict:
    """
    Build the publish payload for Site Empire API.
    Canonical shape — see site-empire-brief.md.
    """
    clean_body = clean_article_body(body)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    template = site_context.output.get("frontmatter_template", {}) or {}
    author = template.get("author", site_context.site_name)

    # Get category - prefer metadata over niche fallback
    category = metadata.get("category", "") or site_context.niche

    # Use None for optional empty strings (cleaner API payload)
    featured_image = metadata.get("featured_image", "") or None
    meta_description = metadata.get("meta_description", "") or None
    seo_title = metadata.get("seo_title", "") or None
    article_type = metadata.get("article_type", "") or None
    qa_feedback = metadata.get("feedback", "") or None

    return {
        "site_id": metadata.get("site_id", ""),
        "slug": metadata.get("slug", ""),
        "title": metadata.get("title", "Untitled"),
        "body": clean_body,
        "date": today,
        "draft": False,
        "description": meta_description,
        "meta_description": meta_description,
        "seo_title": seo_title,
        "category": category,
        "tags": metadata.get("tags", []),
        "author": author,
        "article_type": article_type,
        "word_count": metadata.get("word_count", 0),
        "qa_score": metadata.get("score", 0),
        "qa_feedback": qa_feedback,
        "featured_image": featured_image,
        "_factory": {
            "run_id": metadata.get("run_id", ""),
            "article_id": metadata.get("article_id", ""),
            "site_id": metadata.get("site_id", ""),
            "generated": datetime.now(timezone.utc).isoformat(),
        },
    }


# ── Site Empire API Push ────────────────────────────────────

def publish_to_site_empire(payload: dict, timeout: int = 30) -> dict:
    """
    POST the payload to Site Empire's /api/publish endpoint.
    Returns the API response dict on success.
    Raises Exception on failure.
    """
    api_key = os.environ.get("FACTORY_API_KEY")
    if not api_key:
        raise ValueError("FACTORY_API_KEY not set in environment")

    base = os.environ.get("SITE_EMPIRE_URL", "https://site-empire.vercel.app").rstrip("/")
    endpoint = f"{base}/api/publish"

    response = requests.post(
        endpoint,
        json=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        timeout=timeout,
    )

    if response.status_code == 200:
        return response.json()

    try:
        err = response.json()
        msg = err.get("error", str(err))
    except Exception:
        msg = response.text or f"HTTP {response.status_code}"

    raise Exception(f"Site Empire API error ({response.status_code}): {msg}")


# ── Disk Write (Primary or Fallback) ────────────────────────

def build_disk_frontmatter(payload: dict, site_context: SiteContext) -> dict:
    """Build frontmatter for disk write — mirrors what Site Empire stores."""
    fm = {}
    template = site_context.output.get("frontmatter_template", {}) or {}
    if template:
        fm.update(template)

    fm.update({
        "site": payload["site_id"],
        "title": payload["title"],
        "slug": payload["slug"],
        "date": payload["date"],
        "lastmod": payload["date"],
        "draft": payload["draft"],
        "description": payload["description"],
        "seo_title": payload["seo_title"],
        "category": payload["category"],
        "tags": payload["tags"],
        "author": payload["author"],
        "article_type": payload["article_type"],
        "word_count": payload["word_count"],
        "qa_score": payload["qa_score"],
        "_factory": payload["_factory"],
    })

    return fm


def build_filename(payload: dict) -> str:
    """Build the on-disk filename for an article: YYYY-MM-DD-slug.md."""
    slug = payload["slug"] or "untitled"
    return f"{payload['date']}-{slug}.md"


def write_to_disk(payload: dict, site_context: SiteContext, fallback: bool = False) -> Path:
    """
    Write the article to disk.
    fallback=True writes to quarantine for retry; fallback=False writes to CONTENT_ROOT.
    """
    if fallback:
        content_root = os.environ.get("DEPOSIT_FAILED_DIR", "pipeline/failed_publishes")
    else:
        content_root = os.environ.get("CONTENT_ROOT", "sites")

    site_id = payload["site_id"]
    filename = build_filename(payload)

    output_path = Path(content_root) / site_id / "articles" / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fm = build_disk_frontmatter(payload, site_context)
    fm_str = yaml.dump(fm, default_flow_style=False, sort_keys=False, allow_unicode=True)
    content = f"---\n{fm_str}---\n\n{payload['body']}"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    return output_path


# ── Deposit Engine ──────────────────────────────────────────

class DepositEngine:

    def __init__(self, config_dir: str = "config/sites"):
        self.loader = SiteLoader(config_dir=config_dir)
        self.mode = os.environ.get("DEPOSIT_MODE", "api").lower()
        if self.mode not in ("api", "api+disk", "disk"):
            logger.warning(f"[deposit] Unknown DEPOSIT_MODE '{self.mode}', defaulting to 'api'")
            self.mode = "api"
        logger.info(f"[deposit] Mode: {self.mode}")

    def deposit(
        self,
        input_dir: str = "pipeline/qa",
        site_filter: str = "",
        run_filter: str = "",
        dry_run: bool = False,
    ) -> dict:
        """
        Scan QA output, publish PUBLISH-verdict articles to Site Empire (and/or disk).
        """
        artifacts = load_artifacts_from_dir(
            input_dir,
            module_filter="qa",
            site_filter=site_filter or None,
            run_filter=run_filter or None,
        )

        summary = {
            "published": [],
            "skipped_rewrite": [],
            "skipped_kill": [],
            "skipped_deslop": [],
            "errors": [],
            "fallback_to_disk": [],
            "total_scanned": len(artifacts),
            "mode": self.mode,
        }

        for metadata, body, filepath in artifacts:
            verdict = metadata.get("verdict", "")
            article_id = metadata.get("article_id", "unknown")
            site_id = metadata.get("site_id", "")
            title = metadata.get("title", "Untitled")

            # Skip non-PUBLISH verdicts
            if verdict != "PUBLISH":
                if verdict == "REWRITE":
                    summary["skipped_rewrite"].append({
                        "article_id": article_id, "title": title, "score": metadata.get("score", 0),
                    })
                elif verdict == "KILL":
                    summary["skipped_kill"].append({
                        "article_id": article_id, "title": title, "score": metadata.get("score", 0),
                    })
                continue

            # Load site config
            try:
                site_context = self.loader.load(site_id)
            except Exception as e:
                summary["errors"].append({"article_id": article_id, "error": f"Config load failed: {e}"})
                logger.error(f"[deposit] ❌ {title}: site config load failed: {e}")
                continue

            # ── Last step before publishing: deslop ──────────────
            # Loops the deterministic --fix pass, then an LLM rewrite pass, until
            # the slop score drops below DESLOP_TARGET_SCORE (default 15).
            # Anything still at/above that (couldn't be fixed or rewritten to
            # target) is held, not published. Fails open on tooling/API errors.
            body, slop_score = deslop_to_target(body, article_id, site_context)
            deslop_min = int(os.environ.get("DESLOP_TARGET_SCORE", "15"))
            if slop_score is not None and slop_score >= deslop_min:
                summary["skipped_deslop"].append({
                    "article_id": article_id, "title": title,
                    "slop_score": slop_score, "threshold": deslop_min,
                })
                logger.warning(
                    f"[deposit] 🧹 HELD (deslop): {title} scored {slop_score} "
                    f">= {deslop_min}; not published"
                )
                continue

            payload = build_publish_payload(metadata, body, site_context)

            if dry_run:
                summary["published"].append({
                    "article_id": article_id,
                    "title": title,
                    "site_id": site_id,
                    "slug": payload["slug"],
                    "score": payload["qa_score"],
                    "word_count": payload["word_count"],
                    "action": "DRY_RUN",
                    "dry_run": True,
                })
                logger.info(f"[deposit] DRY RUN: {title} ({site_id}/{payload['slug']})")
                continue

            # ── Publish ─────────────────────────────────────────
            record = {
                "article_id": article_id,
                "title": title,
                "site_id": site_id,
                "slug": payload["slug"],
                "score": payload["qa_score"],
                "word_count": payload["word_count"],
            }

            api_succeeded = False
            api_error = None

            # Step 1: API push (skip in disk-only mode)
            if self.mode in ("api", "api+disk"):
                try:
                    result = publish_to_site_empire(payload)
                    api_succeeded = True
                    action = result.get("action", "ok")
                    record["api_action"] = action
                    record["api_response"] = result
                    logger.info(f"[deposit] ✅ API {action.upper()}: {site_id}/{payload['slug']}")

                    # Step 1b: Run P4 deterministic QA gates
                    api_article_id = result.get("article_id")
                    if api_article_id:
                        logger.info(f"[deposit] Running P4 gates on {api_article_id}...")
                        p4_result = run_p4_gates(api_article_id, save_results=True)
                        p4_error = p4_result.get("error")
                        record["p4_passed"] = p4_result.get("passed", False)
                        record["p4_failures"] = p4_result.get("failure_reasons", [])

                        if p4_error:
                            # P4 could not RUN (e.g. site-empire import failed) — an
                            # infrastructure failure, not a content QA failure. Surface it
                            # loudly instead of letting it masquerade as a gate failure
                            # with "0 issues". Article stays parked for review either way.
                            logger.error(
                                f"[deposit] ❌ P4 gates DID NOT RUN for {api_article_id}: {p4_error}"
                            )
                            record["p4_error"] = p4_error
                            record["final_status"] = "pending_review"
                        elif p4_result.get("passed"):
                            logger.info(f"[deposit] ✅ P4 gates passed")
                            # Update status if P4 passed and LLM score is good
                            qa_score = payload.get("qa_score", 0)
                            # threshold=None → update_article_status falls back to
                            # the site's publish_threshold (per-site config is authoritative).
                            status_result = update_article_status(
                                api_article_id, qa_score, threshold=None
                            )
                            record["final_status"] = status_result.get("status")
                            record["status_action"] = status_result.get("action")
                            if status_result.get("status") == "published":
                                logger.info(f"[deposit] ✅ Auto-published: {site_id}/{payload['slug']}")
                        else:
                            logger.warning(
                                f"[deposit] ⚠️  P4 gates failed ({len(p4_result.get('failure_reasons', []))} issues)"
                            )
                            record["final_status"] = "pending_review"

                        # Step 1c: Generate embedding for internal linking (P6)
                        # Runs regardless of P4 pass/fail - embedding aids future internal linking
                        try:
                            from linking.embeddings import EmbeddingService
                            embedding_service = EmbeddingService()
                            embed_success = embedding_service.embed_and_store(
                                api_article_id, payload["title"], payload["body"]
                            )
                            if embed_success:
                                logger.info(f"[deposit] 🔗 Embedding stored for {api_article_id}")
                                record["embedding_stored"] = True
                            else:
                                logger.warning(f"[deposit] ⚠️  Embedding failed for {api_article_id}")
                                record["embedding_stored"] = False
                        except Exception as embed_err:
                            logger.warning(f"[deposit] ⚠️  Embedding error: {embed_err}")
                            record["embedding_error"] = str(embed_err)
                except Exception as e:
                    api_error = str(e)
                    record["api_error"] = api_error
                    logger.warning(f"[deposit] ⚠️  API push failed for {site_id}/{payload['slug']}: {e}")

            # Step 2: Disk write (always in disk/api+disk; fallback in api mode)
            should_write_disk = (
                self.mode == "disk"
                or self.mode == "api+disk"
                or (self.mode == "api" and not api_succeeded)
            )
            is_fallback = (self.mode == "api" and not api_succeeded)

            if should_write_disk:
                try:
                    disk_path = write_to_disk(payload, site_context, fallback=is_fallback)
                    record["disk_path"] = str(disk_path)
                    if is_fallback:
                        record["fallback"] = True
                        logger.warning(f"[deposit] 💾 Quarantined to: {disk_path}")
                    else:
                        logger.info(f"[deposit] 💾 Disk: {disk_path}")
                except Exception as e:
                    record["disk_error"] = str(e)
                    logger.error(f"[deposit] ❌ Disk write failed: {e}")

            # Categorize result
            if api_succeeded:
                summary["published"].append(record)
            elif self.mode == "disk" and "disk_path" in record:
                summary["published"].append(record)
            elif "disk_path" in record:
                # API failed but quarantined successfully
                summary["fallback_to_disk"].append(record)
            else:
                # Total failure
                summary["errors"].append({
                    "article_id": article_id,
                    "title": title,
                    "error": api_error or "Both API and disk failed",
                })

        return summary

    def generate_report(self, summary: dict) -> str:
        """Generate a human-readable run report."""
        lines = ["# Article Factory - Deposit Report"]
        lines.append(f"\n**Date:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        lines.append(f"**Mode:** {summary.get('mode', 'api')}")
        lines.append(f"**Total scanned:** {summary['total_scanned']}")
        lines.append(f"**Deposited:** {len(summary['published'])}")

        # Count P4 results
        p4_passed = sum(1 for item in summary['published'] if item.get('p4_passed') is True)
        p4_failed = sum(1 for item in summary['published'] if item.get('p4_passed') is False)
        auto_published = sum(1 for item in summary['published'] if item.get('final_status') == 'published')

        if p4_passed or p4_failed:
            lines.append(f"**P4 gates passed:** {p4_passed}")
            lines.append(f"**P4 gates failed:** {p4_failed}")
        if auto_published:
            lines.append(f"**Auto-published:** {auto_published}")

        lines.append(f"**Quarantined (API failed):** {len(summary.get('fallback_to_disk', []))}")
        lines.append(f"**Skipped (rewrite):** {len(summary['skipped_rewrite'])}")
        lines.append(f"**Skipped (kill):** {len(summary['skipped_kill'])}")
        lines.append(f"**Errors:** {len(summary['errors'])}")

        if summary["published"]:
            lines.append("\n## Published")
            for item in summary["published"]:
                action = item.get("api_action", "DISK").upper()
                if item.get("dry_run"):
                    action = "DRY_RUN"
                p4_status = ""
                if item.get("p4_passed") is not None:
                    p4_status = " P4:PASS" if item["p4_passed"] else " P4:FAIL"
                final = ""
                if item.get("final_status"):
                    final = f" -> {item['final_status']}"
                lines.append(
                    f"- **{item['title']}** ({item['site_id']}/{item['slug']}) - "
                    f"{action}{p4_status}{final} - score {item['score']}, {item['word_count']}w"
                )

        # P4 gate failures (articles that passed API but failed P4)
        p4_failures = [
            item for item in summary["published"]
            if item.get("p4_passed") is False
        ]
        if p4_failures:
            lines.append("\n## [WARN] P4 Gate Failures (pending review)")
            for item in p4_failures:
                lines.append(f"- **{item['title']}** ({item['site_id']}/{item['slug']})")
                for failure in item.get("p4_failures", [])[:5]:
                    lines.append(f"  - [{failure['category']}.{failure['gate']}] {failure['reason']}")

        if summary.get("fallback_to_disk"):
            lines.append("\n## [WARN] Quarantined (API push failed)")
            for item in summary["fallback_to_disk"]:
                lines.append(
                    f"- **{item['title']}** ({item['site_id']}) -> `{item.get('disk_path', '?')}`"
                )
                if item.get("api_error"):
                    lines.append(f"  - {item['api_error']}")

        if summary["skipped_rewrite"]:
            lines.append("\n## Needs Rewrite")
            for item in summary["skipped_rewrite"]:
                lines.append(f"- {item['title']} - score {item['score']}")

        if summary["skipped_kill"]:
            lines.append("\n## Killed")
            for item in summary["skipped_kill"]:
                lines.append(f"- {item['title']} - score {item['score']}")

        if summary["errors"]:
            lines.append("\n## Errors")
            for item in summary["errors"]:
                lines.append(f"- {item.get('title', item['article_id'])}: {item['error']}")

        return "\n".join(lines)


# ── CLI ─────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="Article Factory — Deposit (API-first)")
    parser.add_argument("--input", default="pipeline/qa", help="QA output directory")
    parser.add_argument("--site", default="", help="Filter by site_id")
    parser.add_argument("--run-id", default="", help="Filter by run_id")
    parser.add_argument("--dry-run", action="store_true", help="Preview without publishing")
    parser.add_argument("--report", default="", help="Save report to this path")
    parser.add_argument("--config", default="config/sites", help="Site config directory")
    parser.add_argument("--mode", default="", help="Override DEPOSIT_MODE: api | api+disk | disk")

    args = parser.parse_args()

    if args.mode:
        os.environ["DEPOSIT_MODE"] = args.mode

    engine = DepositEngine(config_dir=args.config)
    summary = engine.deposit(
        input_dir=args.input,
        site_filter=args.site,
        run_filter=args.run_id,
        dry_run=args.dry_run,
    )

    report = engine.generate_report(summary)
    print(report)

    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        with open(args.report, "w") as f:
            f.write(report)
        print(f"\nReport saved to: {args.report}")

    published = len(summary["published"])
    quarantined = len(summary.get("fallback_to_disk", []))

    if published > 0 and quarantined == 0:
        print(f"\n[OK] Published {published} articles")
    elif published > 0 and quarantined > 0:
        print(f"\n[WARN] Published {published}, {quarantined} quarantined (retry later)")
    elif quarantined > 0:
        print(f"\n[WARN] All {quarantined} articles quarantined - check Site Empire connectivity")
    else:
        print(f"\n[INFO] Nothing to deposit")


if __name__ == "__main__":
    main()