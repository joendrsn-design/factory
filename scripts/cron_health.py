#!/usr/bin/env python3
"""Cron entry point — runs daily health monitor and reports out.

Usage:
    python scripts/cron_health.py

Cron example (run daily at 7am):
    0 7 * * * cd /path/to/factory && python scripts/cron_health.py >> /var/log/site-empire-health.log 2>&1
"""
import sys
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from health_monitor import run_all_checks, format_report, post_to_slack, write_to_obsidian

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("article_factory.cron_health")


def main():
    logger.info("Starting health check...")

    try:
        results = run_all_checks()
        report = format_report(results)

        # Always print to stdout (for cron logs)
        print(report)

        # Write to Obsidian if configured
        write_to_obsidian(report)

        # Post to Slack if configured
        post_to_slack(report)

        # Exit with error code if any sites are unhealthy
        unhealthy = [r for r in results if not r.is_healthy()]
        if unhealthy:
            logger.warning(f"{len(unhealthy)} site(s) unhealthy")
            sys.exit(1)

        logger.info("Health check complete — all sites healthy")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        sys.exit(2)


if __name__ == "__main__":
    main()
