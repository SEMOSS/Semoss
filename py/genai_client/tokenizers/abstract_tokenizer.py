from typing import Any, List, Optional
from abc import ABC, abstractmethod


class AbstractTokenizer(ABC):

    def __init__(
        self,
        encoder_name: str,
        max_tokens: Optional[int],
        max_input_tokens: Optional[int] = None,
        context_window: Optional[int] = None,
        max_completion_tokens: Optional[int] = None,
    ):
        self.tokenizer = self._get_tokenizer(encoder_name)
        self.max_tokens = max_tokens  # Deprecated
        self.max_input_tokens = max_input_tokens  # Deprecated
        self.max_completion_tokens = max_completion_tokens  # Same as max_input_tokens

    @abstractmethod
    def _get_tokenizer(self, *args: Any, **kwargs: Any) -> Any:
        pass

    @abstractmethod
    def count_tokens(self, *args: Any, **kwargs: Any) -> int:
        pass

    @abstractmethod
    def get_max_token_length(self, *args: Any, **kwargs: Any) -> int:
        """
        Returns an integer representing the maximum tokens to be used, following this priority order:
        1. max_input_tokens, if not None.
        2. max_tokens, if not None.
        3. tokenizer_max_tokens (the tokenizer's maximum tokens), if not None -- hopefully not.

        If all of the above values are None, it returns None.

        Parameters:
        - max_input_tokens (int or None): The maximum tokens specified for input.
        - max_tokens (int or None): The maximum tokens specified globally.
        - tokenizer_max_tokens (int or None): The maximum tokens specified by the tokenizer.

        Returns:
        - int or None: The chosen maximum tokens value.
        """
        pass

    # @abstractmethod
    # def decode_token_ids(self, *args: Any, **kwargs: Any) -> str:
    #     pass

    # @abstractmethod
    # def get_tokens(self, *args: Any, **kwargs: Any) -> List[str]:
    #     pass
