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
import yaml
import logging
import argparse
import requests
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

from site_loader import SiteLoader, SiteContext
from artifacts import load_artifacts_from_dir

logger = logging.getLogger("article_factory.deposit")


# ── Body Cleanup ────────────────────────────────────────────

def clean_article_body(body: str) -> str:
    """Remove pipeline artifacts from the body."""
    body = re.sub(r"\n---\n\n```json\n.*?\n```", "", body, flags=re.DOTALL)
    body = re.sub(r"\n{4,}", "\n\n\n", body)
    return body.strip()


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

    return {
        "site_id": metadata.get("site_id", ""),
        "slug": metadata.get("slug", ""),
        "title": metadata.get("title", "Untitled"),
        "body": clean_body,
        "date": today,
        "draft": False,
        "description": metadata.get("meta_description", ""),
        "meta_description": metadata.get("meta_description", ""),
        "seo_title": metadata.get("seo_title", ""),
        "category": category,
        "tags": metadata.get("tags", []),
        "author": author,
        "article_type": metadata.get("article_type", ""),
        "word_count": metadata.get("word_count", 0),
        "qa_score": metadata.get("score", 0),
        "qa_feedback": metadata.get("feedback", ""),
        "featured_image": metadata.get("featured_image", ""),
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
    slug = payload["slug"] or "untitled"
    date = payload["date"]
    filename = f"{date}-{slug}.md"

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
        lines = ["# Article Factory — Deposit Report"]
        lines.append(f"\n**Date:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        lines.append(f"**Mode:** {summary.get('mode', 'api')}")
        lines.append(f"**Total scanned:** {summary['total_scanned']}")
        lines.append(f"**Published:** {len(summary['published'])}")
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
                lines.append(
                    f"- **{item['title']}** ({item['site_id']}/{item['slug']}) — "
                    f"{action} — score {item['score']}, {item['word_count']}w"
                )

        if summary.get("fallback_to_disk"):
            lines.append("\n## ⚠️ Quarantined (API push failed)")
            for item in summary["fallback_to_disk"]:
                lines.append(
                    f"- **{item['title']}** ({item['site_id']}) → `{item.get('disk_path', '?')}`"
                )
                if item.get("api_error"):
                    lines.append(f"  - {item['api_error']}")

        if summary["skipped_rewrite"]:
            lines.append("\n## Needs Rewrite")
            for item in summary["skipped_rewrite"]:
                lines.append(f"- {item['title']} — score {item['score']}")

        if summary["skipped_kill"]:
            lines.append("\n## Killed")
            for item in summary["skipped_kill"]:
                lines.append(f"- {item['title']} — score {item['score']}")

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
        print(f"\n✅ Published {published} articles")
    elif published > 0 and quarantined > 0:
        print(f"\n⚠️  Published {published}, {quarantined} quarantined (retry later)")
    elif quarantined > 0:
        print(f"\n⚠️  All {quarantined} articles quarantined — check Site Empire connectivity")
    else:
        print(f"\n⚠️  Nothing to deposit")


if __name__ == "__main__":
    main()