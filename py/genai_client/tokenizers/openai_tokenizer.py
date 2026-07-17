from __future__ import annotations

from typing import Union, Optional, TYPE_CHECKING
from .abstract_tokenizer import AbstractTokenizer

if TYPE_CHECKING:
    from tiktoken import Encoding
    from .local_tokenizer import LocalWordCountTokenizer


class OpenAiTokenizer(AbstractTokenizer):

    def __init__(
        self,
        encoder_name: str,
        max_tokens: Optional[int],
    ):
        super().__init__(
            encoder_name=encoder_name,
            max_tokens=max_tokens,
        )

    def _get_tokenizer(
        self, encoder_name: str
    ) -> Union[Encoding, LocalWordCountTokenizer]:
        try:
            import tiktoken

            return tiktoken.encoding_for_model(encoder_name)
        except KeyError:
            from .local_tokenizer import LocalWordCountTokenizer

            return LocalWordCountTokenizer()

    def count_tokens(self, input_data: str) -> int:
        return len(self.tokenizer.encode(input_data))

    def get_max_token_length(self) -> int:
        if self.max_tokens == None and hasattr(self.tokenizer, "max_token_value"):
            return self.tokenizer.max_token_value
        elif self.max_tokens:
            return self.max_tokens
        else:
            return 8192
