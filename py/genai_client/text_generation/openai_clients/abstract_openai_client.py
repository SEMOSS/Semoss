from typing import Dict, Any
from openai import OpenAI
from ..base_client import BaseClient
from ...tokenizers.openai_tokenizer import OpenAiTokenizer
from abc import ABC, abstractmethod

class AbstractOpenAiClient(BaseClient, ABC):
    
    def __init__(
        self, 
        model_name:str = None, 
        api_key:str = None, 
        **kwargs
    ):
        assert api_key != None

        super().__init__(
            template = kwargs.pop(
                'template', 
                None
            ),
            template_name = kwargs.pop(
                'template_name', 
                None
            )
        )

        self.model_name = model_name

        self.tokenizer = self._get_tokenizer(kwargs)

        self.client = self._get_client(
            api_key=api_key,
            **kwargs
        )
        
    @abstractmethod
    def ask(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        pass
    
    def _get_tokenizer(self, init_args):
        return OpenAiTokenizer(
            encoder_name = init_args.pop('tokenizer_name', None) or self.model_name, 
            max_tokens = init_args.pop(
                'max_tokens', 
                None
            )
        )
        
    def _get_client(self, api_key, **kwargs):
        from openai import OpenAI
        return OpenAI(
            api_key=api_key,
            **kwargs
        )