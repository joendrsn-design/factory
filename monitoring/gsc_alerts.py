"""
GSC Alerts — Slack notification system for GSC monitoring alerts.

Handles traffic drops, manual actions, and security issues with
formatted Slack blocks and critical page-out capabilities.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("article_factory.monitoring.gsc_alerts")

# Environment variables for Slack webhooks
GSC_ALERT_WEBHOOK_VAR = "GSC_ALERT_SLACK_WEBHOOK"  # Critical alerts (manual actions, security)
HEALTH_WEBHOOK_VAR = "HEALTH_SLACK_WEBHOOK"  # Weekly reports, traffic drops


@dataclass
class SlackBlock:
    """A single Slack block element."""
    type: str
    text: Optional[dict] = None
    elements: Optional[list] = None
    fields: Optional[list] = None
    accessory: Optional[dict] = None

    def to_dict(self) -> dict:
        result = {"type": self.type}
        if self.text:
            result["text"] = self.text
        if self.elements:
            result["elements"] = self.elements
        if self.fields:
            result["fields"] = self.fields
        if self.accessory:
            result["accessory"] = self.accessory
        return result


def get_alert_webhook() -> Optional[str]:
    """Get the GSC alert Slack webhook URL."""
    return os.environ.get(GSC_ALERT_WEBHOOK_VAR)


def get_health_webhook() -> Optional[str]:
    """Get the health report Slack webhook URL."""
    return os.environ.get(HEALTH_WEBHOOK_VAR)


def _post_to_webhook(webhook_url: str, payload: dict) -> bool:
    """
    Post a payload to a Slack webhook.

    Args:
        webhook_url: The Slack webhook URL.
        payload: The JSON payload to post.

    Returns:
        True if successful, False otherwise.
    """
    try:
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        logger.info("Posted to Slack successfully")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to post to Slack: {e}")
        return False


def _severity_emoji(severity: str) -> str:
    """Get emoji for alert severity."""
    return {
        "critical": ":rotating_light:",
        "warning": ":warning:",
        "info": ":information_source:",
    }.get(severity, ":bell:")


def _format_number(n: int) -> str:
    """Format a number with commas."""
    return f"{n:,}"


def post_alert_to_slack(
    alert_type: str,
    severity: str,
    title: str,
    description: str,
    domain: str,
    metrics: dict = None,
    affected_pages: list[str] = None,
    webhook_url: str = None,
) -> bool:
    """
    Post a formatted GSC alert to Slack.

    Args:
        alert_type: Type of alert (traffic_drop, manual_action, etc.)
        severity: Alert severity (critical, warning, info)
        title: Alert title
        description: Detailed description
        domain: The affected domain
        metrics: Optional metrics dict for additional context
        affected_pages: Optional list of affected page URLs
        webhook_url: Optional override for webhook URL

    Returns:
        True if posted successfully, False otherwise.
    """
    webhook = webhook_url or get_alert_webhook() or get_health_webhook()
    if not webhook:
        logger.warning("No Slack webhook configured for alerts")
        return False

    metrics = metrics or {}
    affected_pages = affected_pages or []

    emoji = _severity_emoji(severity)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Build Slack blocks
    blocks = [
        # Header
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} {title}",
                "emoji": True,
            }
        },
        # Domain and timestamp
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Domain:*\n{domain}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Alert Type:*\n{alert_type.replace('_', ' ').title()}"
                },
            ]
        },
        # Description
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": description
            }
        },
    ]

    # Add metrics if available
    if metrics:
        metric_fields = []

        if "previous_clicks" in metrics and "current_clicks" in metrics:
            metric_fields.append({
                "type": "mrkdwn",
                "text": f"*Previous Clicks:*\n{_format_number(metrics['previous_clicks'])}"
            })
            metric_fields.append({
                "type": "mrkdwn",
                "text": f"*Current Clicks:*\n{_format_number(metrics['current_clicks'])}"
            })

        if "change_pct" in metrics:
            change = metrics["change_pct"]
            change_str = f"+{change}%" if change >= 0 else f"{change}%"
            metric_fields.append({
                "type": "mrkdwn",
                "text": f"*Change:*\n{change_str}"
            })

        if metric_fields:
            blocks.append({
                "type": "section",
                "fields": metric_fields[:4]  # Slack max 10 fields
            })

    # Add affected pages if available (max 5)
    if affected_pages:
        pages_text = "\n".join(f"- `{page}`" for page in affected_pages[:5])
        if len(affected_pages) > 5:
            pages_text += f"\n_...and {len(affected_pages) - 5} more_"

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Affected Pages:*\n{pages_text}"
            }
        })

    # Add GSC link
    gsc_url = f"https://search.google.com/search-console?resource_id=sc-domain%3A{domain}"
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"<{gsc_url}|View in Google Search Console>"
        }
    })

    # Context footer
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"Detected at {timestamp} | Site Empire GSC Monitor"
            }
        ]
    })

    # Divider
    blocks.append({"type": "divider"})

    payload = {
        "blocks": blocks,
        "text": f"{emoji} {title} - {domain}",  # Fallback text
    }

    return _post_to_webhook(webhook, payload)


def post_manual_action_alert(
    domain: str,
    action_type: str,
    description: str,
    affected_pages: list[str] = None,
) -> bool:
    """
    Post a critical alert for Google manual action.

    Manual actions require immediate attention and are always critical.

    Args:
        domain: The affected domain.
        action_type: Type of manual action (e.g., "Unnatural links", "Thin content").
        description: Description of the manual action.
        affected_pages: List of affected page URLs.

    Returns:
        True if posted successfully, False otherwise.
    """
    # Use GSC_ALERT_SLACK_WEBHOOK for critical alerts
    webhook = get_alert_webhook()
    if not webhook:
        # Fall back to health webhook
        webhook = get_health_webhook()

    if not webhook:
        logger.warning("No Slack webhook configured for manual action alerts")
        return False

    return post_alert_to_slack(
        alert_type="manual_action",
        severity="critical",
        title=f"MANUAL ACTION: {action_type}",
        description=(
            f":rotating_light: *Google has applied a manual action to {domain}*\n\n"
            f"*Action Type:* {action_type}\n\n"
            f"{description}\n\n"
            f"*Action Required:* Review and fix issues in Google Search Console, "
            f"then submit a reconsideration request."
        ),
        domain=domain,
        affected_pages=affected_pages,
        webhook_url=webhook,
    )


def post_security_issue_alert(
    domain: str,
    issue_type: str,
    description: str,
    affected_pages: list[str] = None,
) -> bool:
    """
    Post a critical alert for security issues.

    Security issues (malware, phishing, etc.) require immediate attention.

    Args:
        domain: The affected domain.
        issue_type: Type of security issue (e.g., "Malware", "Phishing").
        description: Description of the security issue.
        affected_pages: List of affected page URLs.

    Returns:
        True if posted successfully, False otherwise.
    """
    webhook = get_alert_webhook()
    if not webhook:
        webhook = get_health_webhook()

    if not webhook:
        logger.warning("No Slack webhook configured for security alerts")
        return False

    return post_alert_to_slack(
        alert_type="security_issue",
        severity="critical",
        title=f"SECURITY ISSUE: {issue_type}",
        description=(
            f":rotating_light: *Security issue detected on {domain}*\n\n"
            f"*Issue Type:* {issue_type}\n\n"
            f"{description}\n\n"
            f"*Action Required:* Investigate immediately. Google may show warnings "
            f"to users visiting your site."
        ),
        domain=domain,
        affected_pages=affected_pages,
        webhook_url=webhook,
    )


def post_traffic_drop_alert(
    domain: str,
    previous_clicks: int,
    current_clicks: int,
    change_pct: float,
) -> bool:
    """
    Post a warning alert for significant traffic drops.

    Args:
        domain: The affected domain.
        previous_clicks: Clicks in the previous week.
        current_clicks: Clicks in the current week.
        change_pct: Percentage change (negative for drops).

    Returns:
        True if posted successfully, False otherwise.
    """
    webhook = get_health_webhook()
    if not webhook:
        logger.warning("No Slack webhook configured for traffic alerts")
        return False

    return post_alert_to_slack(
        alert_type="traffic_drop",
        severity="warning",
        title=f"Traffic Drop: {domain}",
        description=(
            f"Organic search traffic has dropped significantly.\n\n"
            f"*Week-over-week change:* {change_pct:.1f}%\n"
            f"*Previous week:* {_format_number(previous_clicks)} clicks\n"
            f"*Current week:* {_format_number(current_clicks)} clicks\n\n"
            f"Consider investigating recent changes, Google algorithm updates, "
            f"or technical issues."
        ),
        domain=domain,
        metrics={
            "previous_clicks": previous_clicks,
            "current_clicks": current_clicks,
            "change_pct": round(change_pct, 2),
        },
        webhook_url=webhook,
    )


def post_weekly_report(
    report_data: list[dict],
    unresolved_alerts: list[dict] = None,
) -> bool:
    """
    Post the weekly GSC summary report to Slack.

    Args:
        report_data: List of site summaries with metrics.
        unresolved_alerts: Optional list of unresolved alert summaries.

    Returns:
        True if posted successfully, False otherwise.
    """
    webhook = get_health_webhook()
    if not webhook:
        logger.warning("No Slack webhook configured for weekly reports")
        return False

    unresolved_alerts = unresolved_alerts or []
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Calculate totals
    total_clicks = sum(s.get("total_clicks", 0) for s in report_data)
    total_impressions = sum(s.get("total_impressions", 0) for s in report_data)
    sites_with_drops = sum(1 for s in report_data if s.get("clicks_wow_change", 0) < -10)

    blocks = [
        # Header
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":chart_with_upwards_trend: Weekly GSC Report",
                "emoji": True,
            }
        },
        # Summary stats
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Total Clicks:*\n{_format_number(total_clicks)}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Total Impressions:*\n{_format_number(total_impressions)}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Sites Monitored:*\n{len(report_data)}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Sites with >10% Drop:*\n{sites_with_drops}"
                },
            ]
        },
        {"type": "divider"},
    ]

    # Top performers (top 5 by clicks)
    if report_data:
        top_sites = sorted(report_data, key=lambda x: x.get("total_clicks", 0), reverse=True)[:5]
        top_text = "*Top Performers (by clicks):*\n"
        for i, site in enumerate(top_sites, 1):
            domain = site.get("domain", "Unknown")
            clicks = site.get("total_clicks", 0)
            change = site.get("clicks_wow_change", 0)
            change_str = f"+{change:.0f}%" if change >= 0 else f"{change:.0f}%"
            top_text += f"{i}. *{domain}* — {_format_number(clicks)} clicks ({change_str} WoW)\n"

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": top_text}
        })

    # Sites with significant drops
    drops = [s for s in report_data if s.get("clicks_wow_change", 0) < -20]
    if drops:
        blocks.append({"type": "divider"})
        drops_text = "*:warning: Sites with >20% Traffic Drop:*\n"
        for site in sorted(drops, key=lambda x: x.get("clicks_wow_change", 0)):
            domain = site.get("domain", "Unknown")
            change = site.get("clicks_wow_change", 0)
            drops_text += f"- *{domain}* — {change:.0f}% WoW\n"

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": drops_text}
        })

    # Unresolved alerts
    if unresolved_alerts:
        blocks.append({"type": "divider"})
        alerts_text = f"*:bell: Unresolved Alerts ({len(unresolved_alerts)}):*\n"
        for alert in unresolved_alerts[:10]:
            alert_type = alert.get("alert_type", "unknown").replace("_", " ").title()
            domain = alert.get("domain", "Unknown")
            alerts_text += f"- [{alert_type}] {domain}\n"

        if len(unresolved_alerts) > 10:
            alerts_text += f"_...and {len(unresolved_alerts) - 10} more_\n"

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": alerts_text}
        })

    # Footer
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"Generated at {timestamp} | Site Empire GSC Monitor"
            }
        ]
    })

    payload = {
        "blocks": blocks,
        "text": f"Weekly GSC Report - {len(report_data)} sites, {_format_number(total_clicks)} total clicks",
    }

    return _post_to_webhook(webhook, payload)


def mark_alert_notified(
    supabase_url: str,
    supabase_key: str,
    alert_id: str,
    slack_message_ts: str = None,
) -> bool:
    """
    Mark an alert as notified in the database.

    Args:
        supabase_url: Supabase project URL.
        supabase_key: Supabase service key.
        alert_id: The alert ID to update.
        slack_message_ts: Optional Slack message timestamp.

    Returns:
        True if updated successfully, False otherwise.
    """
    try:
        url = f"{supabase_url}/rest/v1/gsc_alerts?id=eq.{alert_id}"
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }
        data = {
            "slack_notified_at": datetime.now(timezone.utc).isoformat(),
        }
        if slack_message_ts:
            data["slack_message_ts"] = slack_message_ts

        response = requests.patch(url, headers=headers, json=data, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Failed to mark alert {alert_id} as notified: {e}")
        return False


# CLI for testing
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    parser = argparse.ArgumentParser(description="GSC Alerts CLI")
    parser.add_argument("--test-alert", action="store_true", help="Send a test alert")
    parser.add_argument("--test-weekly", action="store_true", help="Send a test weekly report")
    parser.add_argument("--domain", default="example.com", help="Domain for test alerts")
    args = parser.parse_args()

    if args.test_alert:
        success = post_traffic_drop_alert(
            domain=args.domain,
            previous_clicks=1000,
            current_clicks=650,
            change_pct=-35.0,
        )
        print(f"Test alert sent: {success}")

    elif args.test_weekly:
        test_data = [
            {"domain": "example.com", "total_clicks": 5000, "total_impressions": 100000, "clicks_wow_change": 5.2},
            {"domain": "test.org", "total_clicks": 3000, "total_impressions": 60000, "clicks_wow_change": -25.0},
            {"domain": "demo.io", "total_clicks": 1500, "total_impressions": 30000, "clicks_wow_change": 12.0},
        ]
        success = post_weekly_report(test_data)
        print(f"Test weekly report sent: {success}")

    else:
        print("Use --test-alert or --test-weekly to send test notifications")
