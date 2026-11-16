"""
============================================================
ARTICLE FACTORY — MODEL CONFIGURATION
============================================================
Single source of truth for model IDs, per-module defaults, and pricing.

The pipeline modules import MODELS from here (write.py, research.py, qa.py) so
there is ONE place to bump models. Per-article-type overrides live in the site
YAML (`article_types[].model_override`) and are applied by WriteModule.get_model().

Model IDs verified against the Claude API model reference (2026-11). Use the
exact alias strings — do NOT append date suffixes.
============================================================
"""

# Current model IDs (aliases — never append a date suffix)
MODELS = {
    # Fast, cheap — simple tasks
    "haiku": "claude-haiku-5-5",
    # Balanced — research, QA, and ALL article writing (reflections + deep dives).
    # Held at 4.6: the 5.x models think-by-default and can't disable it, which
    # starves the structured JSON output this pipeline's parsers expect (research
    # truncated on 5.5 even at 16k tokens + effort:low + between_tools). 4.6 has
    # thinking off by default and works cleanly. Revisit 5.x with per-call effort
    # tuning if desired.
    "sonnet": "claude-sonnet-4-6",
    # Most capable — available for premium per-type overrides (not a default)
    "opus": "claude-opus-4-8",
}

# Default model tier per module. Per-site/article-type overrides can still win
# via `model_override` in the site config (see WriteModule.get_model).
MODULE_DEFAULTS = {
    "topic_generator": "haiku",
    "research": "sonnet",
    "expansion": "haiku",
    "planning": "haiku",
    "write": "sonnet",
    "preqa": "haiku",
    "qa": "sonnet",
}

# Cost per 1K tokens, in CENTS (input, output). Source: Claude API pricing.
#   Opus 5.5    $4 / $20 per 1M     ->  0.4¢  / 2.0¢  per 1K
#   Sonnet 5.5  $2 / $10 per 1M     ->  0.2¢  / 1.0¢  per 1K
#   Haiku 5.5   $0.10 / $0.50 per 1M ->  0.01¢ / 0.05¢ per 1K
COST_PER_1K_TOKENS = {
    "haiku": {"input": 0.01, "output": 0.05},
    "sonnet": {"input": 0.2, "output": 1.0},
    "opus": {"input": 0.4, "output": 2.0},
}


def get_model(module_name: str, site_config: dict = None) -> str:
    """
    Get the model ID for a module, with optional site-level override.

    Args:
        module_name: The module (e.g., "write", "qa")
        site_config: Optional site config dict with model overrides

    Returns:
        Anthropic model ID string
    """
    # Check for site-level override
    if site_config:
        overrides = site_config.get("model_overrides", {})
        if module_name in overrides:
            override_key = overrides[module_name]
            if override_key in MODELS:
                return MODELS[override_key]
            # Allow direct model ID override
            return override_key

    # Use module default
    default_key = MODULE_DEFAULTS.get(module_name, "sonnet")
    return MODELS.get(default_key, MODELS["sonnet"])


def estimate_cost_cents(
    module_name: str,
    input_tokens: int,
    output_tokens: int,
    site_config: dict = None,
) -> float:
    """
    Estimate cost in cents for an API call.

    Args:
        module_name: The module name
        input_tokens: Number of input tokens
        output_tokens: Number of output tokens
        site_config: Optional site config for model override

    Returns:
        Estimated cost in cents
    """
    model_id = get_model(module_name, site_config)

    # Find the tier based on model ID
    tier = "sonnet"  # default
    for key, mid in MODELS.items():
        if mid == model_id:
            tier = key
            break

    rates = COST_PER_1K_TOKENS.get(tier, COST_PER_1K_TOKENS["sonnet"])

    input_cost = (input_tokens / 1000) * rates["input"]
    output_cost = (output_tokens / 1000) * rates["output"]

    return input_cost + output_cost
