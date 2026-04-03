from .abstract_tokenizer import AbstractTokenizer


class LazyTokenizer:
    """
    A transparent proxy that stores tokenizer construction arguments and
    defers the expensive import/construction until the first attribute access
    or ``isinstance()`` check.

    Consumers do not need special handling — attribute access and
    ``isinstance`` checks resolve the real tokenizer automatically::

        tokenizer.count_tokens("hello")                      # triggers resolve
        isinstance(tokenizer, HuggingfaceTokenizer)           # also triggers resolve
    """

    _TYPE_MAP = {
        "EMBEDDED": ("genai_client.tokenizers.huggingface_tokenizer", "HuggingfaceTokenizer"),
        "TEXT_GENERATION": ("genai_client.tokenizers.huggingface_tokenizer", "HuggingfaceTokenizer"),
        "OPEN_AI": ("genai_client.tokenizers.openai_tokenizer", "OpenAiTokenizer"),
        "VERTEX": ("genai_client.tokenizers.openai_tokenizer", "OpenAiTokenizer"),
        "BEDROCK": ("genai_client.tokenizers.openai_tokenizer", "OpenAiTokenizer"),
    }

    def __init__(self, tokenizer_type: str, encoder_name: str, max_tokens: int, **extra_kwargs):
        object.__setattr__(self, '_tokenizer_type', tokenizer_type)
        object.__setattr__(self, '_init_kwargs', {
            "encoder_name": encoder_name,
            "max_tokens": max_tokens,
            **extra_kwargs,
        })
        object.__setattr__(self, '_resolved', None)

    def _ensure_resolved(self):
        if self._resolved is not None:
            return
        import importlib
        entry = self._TYPE_MAP.get(self._tokenizer_type)
        if entry is None:
            raise ValueError(f"Tokenizer type has not been defined: {self._tokenizer_type}")
        module_path, class_name = entry
        mod = importlib.import_module(module_path)
        cls = getattr(mod, class_name)
        object.__setattr__(self, '_resolved', cls(**self._init_kwargs))

    # --- transparent proxy ------------------------------------------------

    @property
    def __class__(self):
        """Allow ``isinstance(proxy, RealTokenizer)`` to work after resolution."""
        self._ensure_resolved()
        return type(self._resolved)

    def __getattr__(self, name):
        self._ensure_resolved()
        return getattr(self._resolved, name)

    def __repr__(self):
        if self._resolved is not None:
            return repr(self._resolved)
        return f"LazyTokenizer(type={self._tokenizer_type!r}, unresolved)"
