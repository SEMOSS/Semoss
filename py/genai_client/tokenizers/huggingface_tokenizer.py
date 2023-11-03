from typing import Optional, Union, List, Dict, Any
from transformers import AutoTokenizer
from .abstract_tokenizer import AbstractTokenizer

class HuggingfaceTokenizer(AbstractTokenizer):

    def _get_tokenizer(self, encoder_name:str):
        """
        Returns the appropriate encoding based on the given encoding type (either an encoding string or a model name).
        """
        return AutoTokenizer.from_pretrained(encoder_name)

    def count_tokens(self, input:str) -> int:
        '''Use the model tokenizer to get the number of tokens'''
        input_tokens = self.get_tokens(input=input)
        return len(input_tokens)

    def get_tokens_ids(self, input:str) -> List[int]:
        return self.tokenizer.encode(input)

    def get_tokens(self, input:str) -> List[str]:
        return self.tokenizer.tokenize(input)

    def get_max_token_length(self) -> int:
        if self.max_tokens == None:
            # lets hope the third party packages are correct
            return self.tokenizer.model_max_length
        else:
            return self.max_tokens