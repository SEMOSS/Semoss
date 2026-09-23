from typing import Any, Optional
from abc import ABC, abstractmethod


class AbstractTokenizer(ABC):

    def __init__(
        self,
        encoder_name: str,
        max_tokens: Optional[int],
    ):
        self.tokenizer = self._get_tokenizer(encoder_name)
        self.max_tokens = max_tokens

    @abstractmethod
    def _get_tokenizer(self, *args: Any, **kwargs: Any) -> Any:
        pass

    @abstractmethod
    def count_tokens(self, *args: Any, **kwargs: Any) -> int:
        pass

    @abstractmethod
    def get_max_token_length(self, *args: Any, **kwargs: Any) -> int:
        pass
