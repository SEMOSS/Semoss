from typing import Any
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...tokenizers.openai_tokenizer import OpenAiTokenizer
from abc import ABC, abstractmethod
from ...constants import AskModelEngineResponse


class AbstractOpenAiClient(AbstractTextGenerationClient, ABC):

    def __init__(self, model_name: str = None, api_key: str = None, **kwargs):
        assert api_key != None

        super().__init__(
            template=kwargs.pop("template", None),
            template_name=kwargs.pop("template_name", None),
        )

        self.model_name = model_name

        self.tokenizer = self._get_tokenizer(kwargs)

        self.client = self._get_client(api_key=api_key, **kwargs)

    @abstractmethod
    def ask_call(self, *args: Any, **kwargs: Any) -> AskModelEngineResponse:
        pass

    def _get_tokenizer(self, init_args):
        return OpenAiTokenizer(
            encoder_name=init_args.pop("tokenizer_name", None) or self.model_name,
            max_tokens=init_args.pop("max_tokens", None),
        )

    def _get_client(self, api_key, **kwargs):
        from openai import OpenAI

        return OpenAI(api_key=api_key, **kwargs)
