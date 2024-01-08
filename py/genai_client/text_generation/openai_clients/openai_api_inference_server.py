import openai

from .openai_chat_completion_client import OpenAiChatCompletion
from .openai_completion_client import OpenAiCompletion
from ...tokenizers.huggingface_tokenizer import HuggingfaceTokenizer
from ...constants import (
    MAX_TOKENS,
    MAX_INPUT_TOKENS
)

class OpenAiChatCompletionServer(OpenAiChatCompletion):
    def __init__(
        self, 
        endpoint:str, 
        model_name:str = None, 
        api_key:str = "EMPTY", 
        **kwargs
    ):
        super().__init__(
            api_key = api_key,
            model_name = model_name,
            base_url = endpoint,
            **kwargs
        )
        
    def _get_tokenizer(self, init_args):
        return HuggingfaceTokenizer(
            encoder_name = self.model_name, 
            max_tokens = init_args.pop(
                MAX_TOKENS, 
                None
            ),
            max_input_tokens = init_args.pop(
                MAX_INPUT_TOKENS, 
                None
            )
        )
        
class OpenAiCompletionServer(OpenAiCompletion):
    def __init__(
        self, 
        endpoint:str, 
        model_name:str = None, 
        api_key:str = "EMPTY", 
        **kwargs
    ):
        super().__init__(
            api_key = api_key,
            model_name = model_name,
            base_url = endpoint,
            **kwargs
        )
        
    def _get_tokenizer(self, init_args):
        return HuggingfaceTokenizer(
            encoder_name = self.model_name, 
            max_tokens = init_args.pop(
                MAX_TOKENS, 
                None
            ),
            max_input_tokens = init_args.pop(
                MAX_INPUT_TOKENS, 
                None
            )
        )