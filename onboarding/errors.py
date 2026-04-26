"""
Custom exception classes for the site onboarding automation.

All exceptions inherit from OnboardingError for easy catching at module boundaries.
Each exception should include relevant context: domain, API name, step, etc.
"""


class OnboardingError(Exception):
    """Base exception for all onboarding-related errors."""
    pass


class ConfigError(OnboardingError):
    """Raised when required configuration is missing or invalid."""
    pass


class VercelError(OnboardingError):
    """Raised when Vercel API calls fail."""
    pass


class NamecheapError(OnboardingError):
    """Raised when Namecheap API calls fail."""
    pass


class SearchConsoleError(OnboardingError):
    """Raised when Google Search Console or Site Verification API calls fail."""
    pass


class DNSPropagationError(OnboardingError):
    """Raised when DNS propagation times out or fails verification."""
    pass


class PreflightError(OnboardingError):
    """Raised when preflight checks fail (domain not owned, already exists, etc.)."""
    pass


class BingWebmasterError(OnboardingError):
    """Raised when Bing Webmaster API calls fail."""
    pass
