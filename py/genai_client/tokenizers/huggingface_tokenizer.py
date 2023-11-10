from typing import Optional, Union, List, Dict, Any
from transformers import AutoTokenizer
from .abstract_tokenizer import AbstractTokenizer

class HuggingfaceTokenizer(AbstractTokenizer):

    def _get_tokenizer(self, encoder_name:str):
        """
        Returns the appropriate encoding based on the given encoding type (either an encoding string or a model name).
        """
        try:
            return AutoTokenizer.from_pretrained(encoder_name)
        except:
            # this is the defacto default tokenizer
            return AutoTokenizer.from_pretrained("bert-base-uncased")

    def count_tokens(self, input:str) -> int:
        '''Use the model tokenizer to get the number of tokens'''
        input_tokens_ids = self.get_tokens_ids(input=input)
        return len(input_tokens_ids)

    def get_tokens_ids(self, input:str, add_special_tokens:bool = False) -> List[int]:
        return self.tokenizer.encode(input, add_special_tokens = add_special_tokens)

    def get_tokens(self, input:str) -> List[str]:
        return self.tokenizer.tokenize(input)

    def get_max_token_length(self) -> int:
        if (self.max_tokens == None):
            # lets hope the third party packages are correct
            return self.tokenizer.model_max_length
        else:
            return self.max_tokens

    def get_max_input_token_length(self) -> int:
        return self.max_input_tokens
    
    def decode_token_ids(self, input:List[int]) -> str:
        return self.tokenizer.decode(input)