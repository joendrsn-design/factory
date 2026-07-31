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
    """Raised when Vercel API calls fail.

    Carries the structured error details from the Vercel response so callers
    can branch on the HTTP status and error code rather than substring-matching
    the message text.
    """

    def __init__(self, message: str, status_code: int | None = None,
                 code: str | None = None, body: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.body = body or {}


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
