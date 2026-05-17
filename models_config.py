"""
============================================================
ARTICLE FACTORY — MODEL CONFIGURATION
============================================================
Centralized model configuration to avoid hardcoded model IDs.
Update this file when Anthropic releases new models.
============================================================
"""

# Model IDs - update when new versions are released
MODELS = {
    # Fast, cheap model for simple tasks
    "haiku": "claude-haiku-4-5-20251001",

    # Balanced model for research and QA
    "sonnet": "claude-sonnet-4-5-20251022",

    # Best model for writing
    "opus": "claude-opus-4-5-20251101",
}

# Default model assignments per module
# Override in site config if needed
MODULE_DEFAULTS = {
    "topic_generator": "haiku",
    "research": "sonnet",
    "expansion": "haiku",
    "planning": "haiku",
    "write": "opus",
    "preqa": "haiku",
    "qa": "sonnet",
}

# Cost estimates per 1K tokens (input/output) in cents
# Used for tracking and budgeting
COST_PER_1K_TOKENS = {
    "haiku": {"input": 0.025, "output": 0.125},
    "sonnet": {"input": 0.3, "output": 1.5},
    "opus": {"input": 1.5, "output": 7.5},
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
