
import openai
from .openai_chat_completion_client import OpenAiChatCompletion
import tiktoken

class AzureOpenAiChatCompletion(OpenAiChatCompletion):
  
    def __init__(
        self, 
        endpoint:str, 
        model_name:str = None, 
        api_key:str = "EMPTY", 
        api_version='2023-07-01-preview', 
        **kwargs
    ):
        assert endpoint != None

        super().__init__(
            api_key = api_key,
            model_name = model_name,
            api_version = api_version,
            azure_endpoint = endpoint,
            **kwargs
        )

    def _get_tokenizer(self, init_args):
        try:
            tiktoken.encoding_for_model(self.model_name)
            return super()._get_tokenizer(init_args)
        except:
            init_args['tokenizer_name'] = init_args.pop(
                'openai_model_name', 
                "gpt-3.5-turbo"
            )
            return super()._get_tokenizer(init_args)
        
    def _get_client(self, api_key, **kwargs):
        from openai import AzureOpenAI
        return AzureOpenAI(
            api_key=api_key,
            **kwargs
        )
