from typing import Optional, Union, List, Dict, Any
import tiktoken
from .abstract_tokenizer import AbstractTokenizer
import logging

logger = logging.getLogger(__name__)


class OpenAiTokenizer(AbstractTokenizer):

    def __init__(
        self,
        encoder_name: str,
        max_tokens: int,
        max_input_tokens: int = None
    ):
        super().__init__(
            encoder_name=encoder_name,
            max_tokens=max_tokens,
            max_input_tokens=max_input_tokens,
        )

        self.tokens_per_message = 0
        self.tokens_per_name = 0
        # https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
        if ("gpt-4" in encoder_name) or ("gpt-3.5-turbo" in encoder_name):
            self.tokens_per_message = 3
            self.tokens_per_name = 1
        elif (encoder_name == "gpt-3.5-turbo-0301"):
            # every message follows <|start|>{role/name}\n{content}<|end|>\n
            self.tokens_per_message = 4
            self.tokens_per_name = -1  # if there's a name, the role is omitted

    def _get_tokenizer(self, encoder_name: str):
        """
        Returns the appropriate encoding based on the given encoding type (either an encoding string or a model name).
        """
        if "k_base" in encoder_name:
            return tiktoken.get_encoding(encoder_name)
        else:
            try:
                return tiktoken.encoding_for_model(encoder_name)
            except KeyError:
                logger.warning(
                    "Warning: model not found. Using cl100k_base encoding.")
                return tiktoken.get_encoding("cl100k_base")

    def count_tokens(self, input: Union[List[Dict], str]) -> int:
        num_tokens = 0
        if isinstance(input, list):
            for message in input:
                num_tokens += self.tokens_per_message
                for key, value in message.items():
                    num_tokens += len(self.get_tokens_ids(input=value))

                    if key == "name":
                        num_tokens += self.tokens_per_name
            num_tokens += 3  # every reply is primed with <|start|>assistant<|message|>
        elif isinstance(input, dict):
            num_tokens += self.tokens_per_message
            for key, value in input.items():
                num_tokens += len(self.get_tokens_ids(input=value))

                if key == "name":
                    num_tokens += self.tokens_per_name

            num_tokens += 3  # every reply is primed with <|start|>assistant<|message|>
        elif isinstance(input, str):
            num_tokens = len(self.get_tokens_ids(input=input))

        return num_tokens

    def get_tokens_ids(self, input: Union[List[Dict], str]) -> List[int]:
        if isinstance(input, list):
            input = " ".join([message.get("content") or message.get(
                "text") for message in input if "content" in message or "text" in message])
        elif isinstance(input, dict):
            input = input["content"]

        return self.tokenizer.encode(input)

    def get_tokens(self, input: Union[List[Dict], str]) -> List[str]:
        return [self.tokenizer.decode([tokenId]) for tokenId in self.get_tokens_ids(input)]

    def get_max_token_length(self) -> int:
        if self.max_tokens == None:
            # lets hope the third party packages are correct
            return self.tokenizer.max_token_value
        else:
            return self.max_tokens

    def get_max_input_token_length(self) -> int:
        return self.max_input_tokens

    def decode_token_ids(self, input: List[int]) -> str:
        return self.tokenizer.decode(input)
