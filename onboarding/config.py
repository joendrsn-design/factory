"""
Configuration loader for site onboarding.

Loads environment variables from .env and validates that all required
secrets are present. Raises ConfigError with ALL missing variables at once.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from .errors import ConfigError


@dataclass(frozen=True)
class Config:
    """Immutable configuration for onboarding APIs."""
    vercel_api_token: str
    vercel_project_id: str
    vercel_team_id: str | None
    namecheap_api_user: str
    namecheap_api_key: str
    namecheap_username: str
    namecheap_client_ip: str
    google_sa_json_path: str
    bing_api_key: str | None  # Optional: Bing Webmaster Tools API key


def _get_env(name: str, fallback_name: str | None = None, required: bool = True) -> str | None:
    """
    Get environment variable with optional fallback name.

    Supports both spec-defined names and existing factory .env names.
    """
    value = os.getenv(name)
    if not value and fallback_name:
        value = os.getenv(fallback_name)
    return value


def load_config() -> Config:
    """
    Load and validate configuration from environment variables.

    Loads .env file from the factory root directory, then validates
    all required variables are present.

    Returns:
        Config dataclass with all configuration values.

    Raises:
        ConfigError: If any required environment variables are missing.
                     Lists ALL missing variables, not just the first.
    """
    # Load .env from factory root (parent of onboarding/)
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path)

    # Collect all values, tracking missing ones
    missing: list[str] = []

    # Vercel config (support both spec names and existing factory names)
    vercel_api_token = _get_env("VERCEL_API_TOKEN", "VERCEL_TOKEN")
    if not vercel_api_token:
        missing.append("VERCEL_API_TOKEN (or VERCEL_TOKEN)")

    vercel_project_id = _get_env("VERCEL_PROJECT_ID")
    if not vercel_project_id:
        missing.append("VERCEL_PROJECT_ID")

    # Team ID is optional (empty string if not using a team)
    vercel_team_id = _get_env("VERCEL_TEAM_ID", required=False)

    # Namecheap config
    namecheap_api_user = _get_env("NAMECHEAP_API_USER")
    if not namecheap_api_user:
        missing.append("NAMECHEAP_API_USER")

    namecheap_api_key = _get_env("NAMECHEAP_API_KEY")
    if not namecheap_api_key:
        missing.append("NAMECHEAP_API_KEY")

    namecheap_username = _get_env("NAMECHEAP_USERNAME")
    if not namecheap_username:
        missing.append("NAMECHEAP_USERNAME")

    namecheap_client_ip = _get_env("NAMECHEAP_CLIENT_IP")
    if not namecheap_client_ip:
        missing.append("NAMECHEAP_CLIENT_IP")

    # Google service account (support both spec and existing names)
    google_sa_json_path = _get_env("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "GSC_SERVICE_ACCOUNT_JSON")
    if not google_sa_json_path:
        missing.append("GOOGLE_SERVICE_ACCOUNT_JSON_PATH (or GSC_SERVICE_ACCOUNT_JSON)")

    # Bing Webmaster API key (optional - skip Bing verification if not set)
    bing_api_key = _get_env("BING_WEBMASTER_API_KEY", "BING_API_KEY", required=False)

    # Fail with all missing variables at once
    if missing:
        raise ConfigError(
            f"Missing required environment variables:\n  - " + "\n  - ".join(missing)
        )

    # Resolve relative paths for service account JSON
    sa_path = Path(google_sa_json_path)
    if not sa_path.is_absolute():
        # Resolve relative to factory root
        sa_path = Path(__file__).parent.parent / sa_path

    # Note: File existence is checked when SearchConsoleClient is initialized,
    # allowing --skip-search-console to work without the file present.

    return Config(
        vercel_api_token=vercel_api_token,
        vercel_project_id=vercel_project_id,
        vercel_team_id=vercel_team_id if vercel_team_id else None,
        namecheap_api_user=namecheap_api_user,
        namecheap_api_key=namecheap_api_key,
        namecheap_username=namecheap_username,
        namecheap_client_ip=namecheap_client_ip,
        google_sa_json_path=str(sa_path),
        bing_api_key=bing_api_key if bing_api_key else None,
    )
