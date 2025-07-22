from typing import Any, Optional
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...tokenizers.openai_tokenizer import OpenAiTokenizer
from abc import ABC, abstractmethod
from ...constants import AskModelEngineResponse
from pydantic import BaseModel


class ModelSettings(BaseModel):
    """These are attributes I want set in the SMSS file for each model"""

    model_name: str
    context_window: Optional[int] = None
    max_completion_tokens: Optional[int] = None
    max_input_tokens: Optional[int] = None
    ai_role: Optional[str] = None
    user_role: Optional[str] = None
    system_role: Optional[str] = None
    model_type: Optional[str] = None
    chat_type: Optional[str] = None
    tokens_param_name: Optional[str] = None


class AbstractOpenAiClient(AbstractTextGenerationClient, ABC):

    def __init__(self, api_key: str, **kwargs):
        assert api_key != None

        super().__init__(
            template=kwargs.pop("template", None),
            template_name=kwargs.pop("template_name", None),
            **kwargs
        )

        self.model_type = self.model_settings.model_type

        self.use_max_tokens_param = kwargs.pop("use_max_tokens", False)

        self.tokenizer = self._get_tokenizer(kwargs)
        self.client = self._get_client(api_key=api_key, **kwargs)
        if self.model_type == None:
            self._temp_model_identifier()

    @abstractmethod
    def ask_call(self, *args: Any, **kwargs: Any) -> AskModelEngineResponse:
        pass

    def _get_tokenizer(self, init_args):
        return OpenAiTokenizer(
            encoder_name=init_args.pop("tokenizer_name", None) or self.model_name,
            max_tokens=init_args.pop("max_tokens", None),
            max_input_tokens=init_args.pop("max_input_tokens", None),
            context_window=init_args.pop("context_window", None),
            max_completion_tokens=init_args.pop("max_completion_tokens", None),
        )

    def _get_client(self, api_key, **kwargs):
        from openai import OpenAI

        return OpenAI(api_key=api_key, **kwargs)

    def _temp_model_identifier(self):
        """
        I need to identify the model_type for structured outputs. The solution to this is updating the SMSS files
        to pass the MODEL_TYPE parameter in the init command. This method will temporarily identify the model_type
        until we update these files by using a substring since I really only need to OpenAI models..
        """
        if "gpt-4o" in self.model_name or "o1" in self.model_name:
            self.model_type = "OPEN_AI"
