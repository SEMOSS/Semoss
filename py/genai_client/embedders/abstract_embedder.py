from typing import Dict, Any, TypeVar
from abc import ABC, abstractmethod
from ..constants import MODEL_NAME
from ..tokenizers.abstract_tokenizer import AbstractTokenizer

class AbstractEmbedder(ABC):
    
    def __init__(
        self,
        **kwargs
    ):
        self.model_name = kwargs[MODEL_NAME]
        self.tokenizer = self._get_tokenizer(kwargs)

    @abstractmethod
    def _get_tokenizer(self, init_args: Dict) -> TypeVar("T", bound="AbstractTokenizer"):
        pass

    @abstractmethod
    def embeddings(self, *args: Any, **kwargs: Any) -> Dict:
        pass

    def ask(
        self, 
        *args, 
        **kwargs
    ) -> Dict:
        response = 'This model does not support text generation.'
        output_payload = {
            'response':response,
            'numberOfTokensInPrompt': 0,
            'numberOfTokensInResponse': self.tokenizer.count_tokens(response)
        }
        return output_payload