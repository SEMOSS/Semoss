from typing import Optional
from .abstract_tokenizer import AbstractTokenizer


class LazyTokenizer:
    """
    A lightweight carrier that stores tokenizer construction arguments and
    defers the expensive import/construction until ``resolve()`` is called.

    Callers should replace their reference with the resolved tokenizer::

        if isinstance(self.tokenizer, LazyTokenizer):
            self.tokenizer = self.tokenizer.resolve()
    """

    _TYPE_MAP = {
        "EMBEDDED": ("genai_client.tokenizers.huggingface_tokenizer", "HuggingfaceTokenizer"),
        "TEXT_GENERATION": ("genai_client.tokenizers.huggingface_tokenizer", "HuggingfaceTokenizer"),
        "OPEN_AI": ("genai_client.tokenizers.openai_tokenizer", "OpenAiTokenizer"),
        "VERTEX": ("genai_client.tokenizers.openai_tokenizer", "OpenAiTokenizer"),
        "BEDROCK": ("genai_client.tokenizers.openai_tokenizer", "OpenAiTokenizer"),
    }

    def __init__(self, tokenizer_type: str, encoder_name: str, max_tokens: int, **extra_kwargs):
        self.tokenizer_type = tokenizer_type
        self._init_kwargs = {
            "encoder_name": encoder_name,
            "max_tokens": max_tokens,
            **extra_kwargs,
        }

    def resolve(self) -> AbstractTokenizer:
        """Construct and return the real tokenizer."""
        import importlib
        entry = self._TYPE_MAP.get(self.tokenizer_type)
        if entry is None:
            raise ValueError(f"Tokenizer type has not been defined: {self.tokenizer_type}")
        module_path, class_name = entry
        mod = importlib.import_module(module_path)
        cls = getattr(mod, class_name)
        return cls(**self._init_kwargs)
