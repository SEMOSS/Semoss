from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union
from .abstract_tokenizer import AbstractTokenizer

if TYPE_CHECKING:
    from transformers import AutoTokenizer
    from .local_tokenizer import LocalWordCountTokenizer


class HuggingfaceTokenizer(AbstractTokenizer):

    def __init__(
        self,
        encoder_name: str,
        max_tokens: int,
    ):
        self.encoder_name = encoder_name
        super().__init__(
            encoder_name=encoder_name,
            max_tokens=max_tokens,
        )

    def _get_tokenizer(
        self, encoder_name: str
    ) -> Union[AutoTokenizer, LocalWordCountTokenizer]:
        """
        Returns the appropriate encoding based on the given encoding type (either an encoding string or a model name).
        """
        try:
            from transformers import AutoTokenizer

            return AutoTokenizer.from_pretrained(encoder_name)
        except Exception:
            from .local_tokenizer import LocalWordCountTokenizer

            return LocalWordCountTokenizer()

    def count_tokens(self, input_data: str) -> int:
        return len(self.tokenizer.encode(input_data))

    def get_max_token_length(self, *args: Any, **kwargs: Any) -> int:
        return self.max_tokens if self.max_tokens else 8192
