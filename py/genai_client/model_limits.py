from pydantic import BaseModel


class IndividualModelLimits(BaseModel):
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


def get_model_limits(model_name: str) -> IndividualModelLimits:
    """Retrieve model-specific token limits, falling back to default if not found."""
    model_limits_dict = MODEL_LIMITS_CONFIG.get(model_name, FALLBACK_CONFIG)
    return IndividualModelLimits(**model_limits_dict)


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


class ModelLimits:
    def __init__(
        self,
        model_name: str,
        context_window_smss: int = None,
        max_tokens_call_param: int = None,
        max_completion_tokens_call_param: int = None,
        max_tokens_smss: int = None,
        max_completion_tokens_smss: int = None,
    ):
        self.model_name = model_name
        # Handling model limits based on the model config and any limits set in SMSS
        model_limits = self._resolve_model_limits(
            self.model_name,
            context_window_smss,
            max_tokens_call_param,
            max_completion_tokens_call_param,
            max_tokens_smss,
            max_completion_tokens_smss,
        )

        self.context_window = model_limits.context_window
        self.max_completion_tokens = model_limits.max_completion_tokens

    def _resolve_model_limits(
        self,
        model_name: str,
        context_window_smss: int = None,
        max_tokens_call_param: int = None,
        max_completion_tokens_call_param: int = None,
        max_tokens_smss: int = None,
        max_completion_tokens_smss: int = None,
    ):
        """
        Resolving the model limits based on the model config and any limits set in SMSS.
        If the limits are set in the SMSS and are less than the config limits, we will use the SMSS limits.
        Args:
            model_name (str): Name of model
            context_window (int, optional): context window if set in SMSS. Defaults to None.
            max_completion_tokens (int, optional): max completion tokens if set in SMSS. Defaults to None.
        Returns:
            _type_: ModelLimits
        """
        # The model limits we set in our config
        model_limits = get_model_limits(model_name)

        # We will resolve this value based on the smss or any params passed in the model call
        # We will first honor the model call params and then the smss values
        # If neither is set we will use the model limits config
        if max_completion_tokens_call_param is not None:
            max_token_setting = max_completion_tokens_call_param
        elif max_tokens_call_param is not None:
            max_token_setting = max_tokens_call_param
        elif max_completion_tokens_smss is not None:
            max_token_setting = max_completion_tokens_smss
        elif max_tokens_smss is not None:
            max_token_setting = max_tokens_smss
        else:
            max_token_setting = model_limits.max_completion_tokens

        # Context window is simpler, we just check for an smss value and if not set we use the model limits config
        if context_window_smss is not None:
            context_window_setting = context_window_smss
        else:
            context_window_setting = model_limits.context_window

        return IndividualModelLimits(
            context_window=context_window_setting,
            max_completion_tokens=max_token_setting,
        )
