from typing import Any
from abc import ABC, abstractmethod

class AbstractTokenizer(ABC):

    def __init__(
        self, 
        encoder_name:str,
        encoder_max_tokens:int
    ):
        self.tokenizer = self._get_tokenizer(encoder_name)
        self.max_tokens = encoder_max_tokens

    @abstractmethod
    def count_tokens(self, *args: Any, **kwargs: Any) -> str:
        pass

    @abstractmethod
    def get_tokens_ids(self, *args: Any, **kwargs: Any) -> str:
        pass

    @abstractmethod
    def get_tokens(self, *args: Any, **kwargs: Any) -> str:
        pass

    @abstractmethod
    def _get_tokenizer(self, *args: Any, **kwargs: Any) -> str:
        pass

    @abstractmethod
    def get_max_token_length(self, *args: Any, **kwargs: Any) -> str:
        pass