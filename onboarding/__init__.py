"""
Site onboarding automation module.

Provides clients for domain provisioning, DNS configuration,
and search engine verification.
"""

from .config import Config, load_config
from .errors import (
    BingWebmasterError,
    ConfigError,
    DNSPropagationError,
    NamecheapError,
    OnboardingError,
    PreflightError,
    SearchConsoleError,
    VercelError,
)
from .vercel import VercelClient
from .search_console import SearchConsoleClient
from .bing import BingWebmasterClient
from .namecheap import NamecheapClient
from .provisioner import (
    TenantManifest,
    TenantProvisioner,
    ProvisioningResult,
    provision_tenant,
)
from .indexnow import (
    generate_key as generate_indexnow_key,
    get_key_file_path as get_indexnow_key_path,
    get_key_file_content as get_indexnow_key_content,
    submit_url as submit_indexnow_url,
    submit_urls as submit_indexnow_urls,
)

__all__ = [
    # Config
    "Config",
    "load_config",
    # Clients
    "VercelClient",
    "SearchConsoleClient",
    "BingWebmasterClient",
    "NamecheapClient",
    # Provisioner
    "TenantManifest",
    "TenantProvisioner",
    "ProvisioningResult",
    "provision_tenant",
    # IndexNow
    "generate_indexnow_key",
    "get_indexnow_key_path",
    "get_indexnow_key_content",
    "submit_indexnow_url",
    "submit_indexnow_urls",
    # Errors
    "OnboardingError",
    "ConfigError",
    "VercelError",
    "NamecheapError",
    "SearchConsoleError",
    "BingWebmasterError",
    "DNSPropagationError",
    "PreflightError",
]
