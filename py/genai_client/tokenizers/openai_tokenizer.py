from typing import Optional, Union, List, Dict, Any
import tiktoken
from .abstract_tokenizer import AbstractTokenizer

class OpenAiTokenizer(AbstractTokenizer):

    def _get_tokenizer(self, encoder_name:str):
        """
        Returns the appropriate encoding based on the given encoding type (either an encoding string or a model name).
        """
        if "k_base" in encoder_name:
            return tiktoken.get_encoding(encoder_name)
        else:
            return tiktoken.encoding_for_model(encoder_name)

    def count_tokens(self, input: Union[List[Dict],str]) -> int:
        input_tokens = self.get_tokens_ids(input=input)
        return len(input_tokens)

    def get_tokens_ids(self, input: Union[List[Dict],str]) -> List[int]:
        if isinstance(input, list):
            input = " ".join([message["content"] for message in input])

        return self.tokenizer.encode(input)

    def get_tokens(self, input: Union[List[Dict],str]) -> List[str]:
        return [self.tokenizer.decode([tokenId]) for tokenId in self.get_tokens_ids(input)]

    def get_max_token_length(self) -> int:
        if self.max_tokens == None:
            # lets hope the third party packages are correct
            return self.tokenizer.max_token_value
        else:
            return self.max_tokens