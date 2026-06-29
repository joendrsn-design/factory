"""
Search Console client for ongoing GSC monitoring.

The site-provisioning/onboarding tooling (Vercel, Namecheap DNS, Bing,
IndexNow, tenant provisioner) was removed — this directory now only retains
the Search Console client used by monitoring/gsc_monitor.py.
"""

from .errors import (
    OnboardingError,
    SearchConsoleError,
)
from .search_console import SearchConsoleClient

__all__ = [
    "SearchConsoleClient",
    "OnboardingError",
    "SearchConsoleError",
]
