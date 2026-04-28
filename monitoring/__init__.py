"""
GSC Monitoring module for Site Empire.

Provides Google Search Console data fetching, alerting, and reporting.
"""

from .gsc_monitor import GSCMonitor
from .gsc_alerts import (
    post_alert_to_slack,
    post_manual_action_alert,
    post_security_issue_alert,
    post_weekly_report,
)

__all__ = [
    "GSCMonitor",
    "post_alert_to_slack",
    "post_manual_action_alert",
    "post_security_issue_alert",
    "post_weekly_report",
]
