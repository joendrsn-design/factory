"""Daily health check across all registered Site Empire domains.

Checks HTTPS connectivity, SSL certificate expiry, and content rendering.
"""
from __future__ import annotations

import os
import ssl
import socket
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from pathlib import Path

import requests
from dotenv import load_dotenv

from sites_registry import all_sites

load_dotenv()

logger = logging.getLogger("article_factory.health_monitor")

CONTENT_MARKER = os.environ.get("HEALTH_CONTENT_MARKER", "")
REQUEST_TIMEOUT = 15


@dataclass
class HealthResult:
    """Result of a health check for a single domain."""
    domain: str
    http_status: int | None
    https_works: bool
    ssl_days_remaining: int | None
    content_marker_found: bool
    response_time_ms: int | None
    error: str | None

    def is_healthy(self) -> bool:
        """Returns True if the domain is fully healthy."""
        return (
            self.http_status == 200
            and self.https_works
            and (not CONTENT_MARKER or self.content_marker_found)
        )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)


def check_https(domain: str) -> tuple[bool, int | None, str | None]:
    """
    Check if HTTPS works and get SSL certificate expiry.

    Args:
        domain: The domain to check

    Returns:
        Tuple of (works, ssl_days_remaining, error_message)
    """
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((domain, 443), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=domain) as ssock:
                cert = ssock.getpeercert()
                expires_str = cert.get("notAfter", "")
                if expires_str:
                    # Parse SSL certificate expiry date
                    expires = datetime.strptime(expires_str, "%b %d %H:%M:%S %Y %Z")
                    expires = expires.replace(tzinfo=timezone.utc)
                    days_left = (expires - datetime.now(timezone.utc)).days
                    return True, days_left, None
                return True, None, None
    except ssl.SSLError as e:
        return False, None, f"SSL error: {e}"
    except socket.timeout:
        return False, None, "Connection timed out"
    except socket.gaierror as e:
        return False, None, f"DNS resolution failed: {e}"
    except Exception as e:
        return False, None, str(e)


def check_domain(domain: str) -> HealthResult:
    """
    Perform a complete health check on a domain.

    Args:
        domain: The domain to check

    Returns:
        HealthResult with all check results
    """
    # Check SSL/HTTPS first
    https_works, ssl_days, ssl_err = check_https(domain)

    # Try HTTP request
    try:
        start = datetime.now()
        r = requests.get(
            f"https://{domain}",
            timeout=REQUEST_TIMEOUT,
            headers={"User-Agent": "SiteEmpire-HealthMonitor/1.0"},
            allow_redirects=True,
        )
        response_time = int((datetime.now() - start).total_seconds() * 1000)
        status = r.status_code

        # Check for content marker
        marker_found = bool(CONTENT_MARKER) and CONTENT_MARKER in r.text

        return HealthResult(
            domain=domain,
            http_status=status,
            https_works=https_works,
            ssl_days_remaining=ssl_days,
            content_marker_found=marker_found,
            response_time_ms=response_time,
            error=ssl_err,
        )
    except requests.Timeout:
        return HealthResult(
            domain=domain,
            http_status=None,
            https_works=https_works,
            ssl_days_remaining=ssl_days,
            content_marker_found=False,
            response_time_ms=None,
            error=f"{ssl_err or ''} | HTTP timeout".strip(" |"),
        )
    except requests.RequestException as e:
        return HealthResult(
            domain=domain,
            http_status=None,
            https_works=https_works,
            ssl_days_remaining=ssl_days,
            content_marker_found=False,
            response_time_ms=None,
            error=f"{ssl_err or ''} | HTTP: {e}".strip(" |"),
        )


def run_all_checks() -> list[HealthResult]:
    """
    Run health checks on all registered sites.

    Returns:
        List of HealthResult objects, one per site
    """
    sites = all_sites()
    results = []

    for site in sites:
        domain = site.get("domain")
        if not domain:
            logger.warning(f"Site {site.get('site_id')} has no domain configured")
            continue

        logger.info(f"Checking {domain}...")
        result = check_domain(domain)
        results.append(result)

        if result.is_healthy():
            logger.debug(f"  ✓ {domain} healthy ({result.response_time_ms}ms)")
        else:
            logger.warning(f"  ✗ {domain} unhealthy: {result.error or f'status={result.http_status}'}")

    return results


def format_report(results: list[HealthResult]) -> str:
    """
    Format health check results as a markdown report.

    Args:
        results: List of HealthResult objects

    Returns:
        Markdown-formatted report string
    """
    lines = [
        f"# Site Empire Health Report",
        f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"**Sites Checked:** {len(results)}",
        "",
    ]

    # Failures section
    failed = [r for r in results if r.http_status != 200 or not r.https_works]
    if failed:
        lines.append("## [FAIL] Failures\n")
        for r in failed:
            lines.append(f"- **{r.domain}**")
            lines.append(f"  - HTTP Status: {r.http_status}")
            lines.append(f"  - HTTPS: {'OK' if r.https_works else 'FAIL'}")
            if r.error:
                lines.append(f"  - Error: {r.error}")
        lines.append("")

    # SSL expiring soon
    expiring = [r for r in results if r.ssl_days_remaining is not None and r.ssl_days_remaining < 30]
    if expiring:
        lines.append("## [WARN] SSL Expiring Soon\n")
        for r in sorted(expiring, key=lambda x: x.ssl_days_remaining or 999):
            lines.append(f"- **{r.domain}** — {r.ssl_days_remaining} days remaining")
        lines.append("")

    # Content marker missing (possible empty render)
    if CONTENT_MARKER:
        missing_marker = [r for r in results if r.http_status == 200 and not r.content_marker_found]
        if missing_marker:
            lines.append("## [WARN] Content Marker Missing\n")
            lines.append(f"_Expected marker: `{CONTENT_MARKER[:50]}...`_\n")
            for r in missing_marker:
                lines.append(f"- {r.domain}")
            lines.append("")

    # Slow responses
    slow = [r for r in results if r.response_time_ms and r.response_time_ms > 3000]
    if slow:
        lines.append("## [SLOW] Slow Responses (>3s)\n")
        for r in sorted(slow, key=lambda x: x.response_time_ms or 0, reverse=True):
            lines.append(f"- {r.domain} — {r.response_time_ms}ms")
        lines.append("")

    # Summary
    healthy = [r for r in results if r.is_healthy()]
    lines.append(f"## [OK] Summary\n")
    lines.append(f"- **Healthy:** {len(healthy)} / {len(results)}")
    lines.append(f"- **Failed:** {len(failed)}")
    lines.append(f"- **SSL Expiring (<30d):** {len(expiring)}")

    return "\n".join(lines)


def post_to_slack(report: str) -> None:
    """
    Post report to Slack webhook if configured.

    Args:
        report: The report text to post
    """
    webhook = os.environ.get("HEALTH_SLACK_WEBHOOK")
    if not webhook:
        logger.debug("No HEALTH_SLACK_WEBHOOK configured — skipping Slack notification")
        return

    try:
        # Slack has a 40k character limit, truncate if needed
        if len(report) > 39000:
            report = report[:39000] + "\n\n_[Report truncated]_"

        requests.post(
            webhook,
            json={"text": f"```\n{report}\n```"},
            timeout=10,
        )
        logger.info("Posted health report to Slack")
    except Exception as e:
        logger.error(f"Failed to post to Slack: {e}")


def write_to_obsidian(report: str) -> None:
    """
    Write the report to Obsidian vault if configured.

    Args:
        report: The report text to write
    """
    vault_path = os.environ.get("OBSIDIAN_VAULT_PATH")
    if not vault_path:
        logger.debug("No OBSIDIAN_VAULT_PATH configured — skipping Obsidian write")
        return

    try:
        date = datetime.now().strftime("%Y-%m-%d")
        out_dir = Path(vault_path) / "site-empire" / "health"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / f"{date}.md"
        out_file.write_text(report, encoding="utf-8")
        logger.info(f"Wrote health report to {out_file}")
    except Exception as e:
        logger.error(f"Failed to write to Obsidian: {e}")


# ── Artifact Cleanup ─────────────────────────────────────────

def cleanup_old_artifacts(
    pipeline_dir: str = "pipeline",
    max_age_days: int = 30,
    dry_run: bool = False,
) -> dict:
    """
    Clean up old pipeline artifacts to prevent disk filling up.

    Args:
        pipeline_dir: Root pipeline directory
        max_age_days: Delete artifacts older than this
        dry_run: If True, only report what would be deleted

    Returns:
        Summary dict with counts per subdirectory
    """
    from datetime import timedelta

    pipeline_path = Path(pipeline_dir)
    if not pipeline_path.exists():
        logger.warning(f"Pipeline directory not found: {pipeline_dir}")
        return {"error": "directory_not_found"}

    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    summary = {}

    # Subdirectories to clean
    subdirs = ["topics", "research", "angles", "plans", "articles", "preqa", "qa", "batches"]

    for subdir in subdirs:
        subdir_path = pipeline_path / subdir
        if not subdir_path.exists():
            continue

        deleted = 0
        kept = 0

        for filepath in subdir_path.glob("*.md"):
            try:
                # Check file modification time
                mtime = datetime.fromtimestamp(filepath.stat().st_mtime, tz=timezone.utc)
                if mtime < cutoff:
                    if dry_run:
                        logger.info(f"[cleanup] Would delete: {filepath}")
                    else:
                        filepath.unlink()
                        logger.debug(f"[cleanup] Deleted: {filepath}")
                    deleted += 1
                else:
                    kept += 1
            except (OSError, ValueError) as e:
                logger.warning(f"[cleanup] Error processing {filepath}: {e}")

        # Also clean up JSONL batch files
        for filepath in subdir_path.glob("*.jsonl"):
            try:
                mtime = datetime.fromtimestamp(filepath.stat().st_mtime, tz=timezone.utc)
                if mtime < cutoff:
                    if dry_run:
                        logger.info(f"[cleanup] Would delete: {filepath}")
                    else:
                        filepath.unlink()
                    deleted += 1
                else:
                    kept += 1
            except (OSError, ValueError) as e:
                logger.warning(f"[cleanup] Error processing {filepath}: {e}")

        summary[subdir] = {"deleted": deleted, "kept": kept}
        if deleted > 0:
            action = "Would delete" if dry_run else "Deleted"
            logger.info(f"[cleanup] {subdir}: {action} {deleted} files, kept {kept}")

    # Clean up failed_publishes if it exists
    failed_dir = pipeline_path / "failed_publishes"
    if failed_dir.exists():
        deleted = 0
        for filepath in failed_dir.glob("*.md"):
            try:
                mtime = datetime.fromtimestamp(filepath.stat().st_mtime, tz=timezone.utc)
                if mtime < cutoff:
                    if not dry_run:
                        filepath.unlink()
                    deleted += 1
            except (OSError, ValueError):
                pass
        summary["failed_publishes"] = {"deleted": deleted}
        if deleted > 0:
            logger.info(f"[cleanup] failed_publishes: Cleaned up {deleted} old files")

    total_deleted = sum(s.get("deleted", 0) for s in summary.values())
    logger.info(f"[cleanup] Total: {'Would delete' if dry_run else 'Deleted'} {total_deleted} files")

    return summary


# CLI for testing
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="Health monitor CLI")
    parser.add_argument("--domain", help="Check a single domain instead of all sites")
    parser.add_argument("--slack", action="store_true", help="Post report to Slack")
    parser.add_argument("--obsidian", action="store_true", help="Write report to Obsidian")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--cleanup", action="store_true", help="Clean up old pipeline artifacts")
    parser.add_argument("--cleanup-days", type=int, default=30, help="Max age in days for cleanup (default: 30)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without deleting")
    args = parser.parse_args()

    if args.cleanup:
        summary = cleanup_old_artifacts(
            pipeline_dir="pipeline",
            max_age_days=args.cleanup_days,
            dry_run=args.dry_run,
        )
        if args.json:
            import json
            print(json.dumps(summary, indent=2))
        else:
            print(f"\nArtifact cleanup {'(dry run)' if args.dry_run else 'complete'}:")
            for subdir, counts in summary.items():
                if isinstance(counts, dict):
                    print(f"  {subdir}: deleted {counts.get('deleted', 0)}, kept {counts.get('kept', 0)}")
    elif args.domain:
        result = check_domain(args.domain)
        if args.json:
            import json
            print(json.dumps(result.to_dict(), indent=2))
        else:
            print(f"\nHealth check for {args.domain}:")
            print(f"  HTTP Status: {result.http_status}")
            print(f"  HTTPS Works: {result.https_works}")
            print(f"  SSL Days Remaining: {result.ssl_days_remaining}")
            print(f"  Response Time: {result.response_time_ms}ms")
            print(f"  Content Marker Found: {result.content_marker_found}")
            if result.error:
                print(f"  Error: {result.error}")
    else:
        results = run_all_checks()
        report = format_report(results)

        if args.json:
            import json
            print(json.dumps([r.to_dict() for r in results], indent=2))
        else:
            print(report)

        if args.slack:
            post_to_slack(report)
        if args.obsidian:
            write_to_obsidian(report)
