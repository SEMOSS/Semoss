# This is a temporary object to define model limits while we transition SMSS files
# to use the correct variable names for context_window and max_completion_tokens

MODEL_LIMITS_CONFIG = {
    "gpt-4o": {"context_window": 128000, "max_completion_tokens": 16384},
    "gpt-4o-mini": {"context_window": 128000, "max_completion_tokens": 16384},
    "o1-preview": {"context_window": 128000, "max_completion_tokens": 32768},
    "o1-mini": {"context_window": 128000, "max_completion_tokens": 65536},
    "gpt-4-turbo": {"context_window": 128000, "max_completion_tokens": 4096},
    "gpt-4": {"context_window": 8192, "max_completion_tokens": 8192},
    "gpt-3.5-turbo": {"context_window": 16385, "max_completion_tokens": 4096},
    "meta-llama/Meta-Llama-3.1-8B-Instruct": {
        "context_window": 128000,
        "max_completion_tokens": 4096,
    },
}

FALLBACK_CONFIG = {"context_window": 8192, "max_completion_tokens": 2048}


def get_model_limits(model_name: str):
    # Retrieve model-specific token limits, falling back to default if not found.
    return MODEL_LIMITS_CONFIG.get(model_name, FALLBACK_CONFIG)
