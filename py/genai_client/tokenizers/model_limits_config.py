from pydantic import BaseModel


class ModelLimits(BaseModel):
    """Model limits for context window and max completion tokens."""

    context_window: int
    max_completion_tokens: int


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


def get_model_limits(model_name: str) -> ModelLimits:
    """Retrieve model-specific token limits, falling back to default if not found."""
    model_limits_dict = MODEL_LIMITS_CONFIG.get(model_name, FALLBACK_CONFIG)
    return ModelLimits(**model_limits_dict)


OPENAI_MODELS = [
    "o4-mini",
    "o3",
    "o3-mini",
    "o1",
    "o1-mini",
    "o1-pro",
    "gpt-4o",
    "gpt-4o-mini",
    "gpt-4.1",
    "o1-preview",
    "gpt-4-turbo",
    "gpt-4",
    "gpt-3.5-turbo",
    "gpt-4.1-nano",
    "text-embedding-ada-002",
    "text-embedding-3-small",
    "text-embedding-3-large",
]
