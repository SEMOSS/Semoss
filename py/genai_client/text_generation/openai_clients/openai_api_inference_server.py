from .openai_chat_completion_client import OpenAiChatCompletion
from .openai_completion_client import OpenAiCompletion
from ...tokenizers import HuggingfaceTokenizer
import openai

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
            **kwargs
        )
        
        openai.api_base = endpoint
        
    def _get_tokenizer(self):
        return HuggingfaceTokenizer(
            encoder_name = self.model_name, 
            max_tokens = kwargs.pop(
                'max_tokens', 
                None
            ),
            max_input_tokens = kwargs.pop(
                'max_input_tokens', 
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
            **kwargs
        )
        
        openai.api_base = endpoint
        
    def _get_tokenizer(self, init_args):
        return HuggingfaceTokenizer(
            encoder_name = self.model_name, 
            max_tokens = init_args.pop(
                'max_tokens', 
                None
            ),
            max_input_tokens = init_args.pop(
                'max_input_tokens', 
                None
            )
        )