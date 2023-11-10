from typing import Union, List, Dict, Any
from .openai_embedder import OpenAiEmbedder
import openai
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
                **kwargs
            )
        except:
            super().__init__(
                model_name = kwargs.pop(
                    'openai_model_name', 
                    'text-embedding-ada-002'
                ),
                api_key = api_key,
                **kwargs
            )
        
        self.model_name = model_name
        openai.api_key = api_key
        openai.api_type = kwargs.pop(
            'api_type', 
            'azure'
        )
        openai.api_base = endpoint
        openai.api_version = api_version
        
    def _make_openai_embedding_call(self, list_of_text:List[str]):
        '''this method is responsible for making the openai embeddings call. it takes in a single'''
        return openai.Embedding.create(input = list_of_text, deployment_id = self.model_name)