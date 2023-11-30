from typing import Union, List, Dict, Any
from .openai_embedder import OpenAiEmbedder
import tiktoken

class AzureOpenAiEmbedder(OpenAiEmbedder):
    
    def __init__(
        self,
        model_name:str, 
        api_key:str,
        endpoint:str,
        api_version:str,
        **kwargs
    ):  
        try:
            tiktoken.encoding_for_model(model_name)
            super().__init__(
                model_name = model_name,
                api_key = api_key,
                azure_endpoint = endpoint,
                api_version = api_version,
                **kwargs
            )
        except:
            super().__init__(
                model_name = kwargs.pop(
                    'openai_model_name', 
                    'text-embedding-ada-002'
                ),
                api_key = api_key,
                azure_endpoint = endpoint,
                api_version = api_version,
                **kwargs
            )
        
        self.model_name = model_name
        
    def _get_client(self, api_key, **kwargs):
        from openai import AzureOpenAI
        return AzureOpenAI(
            api_key=api_key,
            **kwargs
        )