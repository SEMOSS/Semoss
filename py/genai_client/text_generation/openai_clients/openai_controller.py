from typing import List, Dict
from .openai_completion_client import OpenAiCompletion
from .azure_openai_completion import AzureOpenAiCompletion
from .openai_api_inference_server import (
    OpenAiServer,
    OpenAiCompletionServer,
)
from .openai_client import OpenAiClient


class OpenAiClientController:
    def __init__(self, **kwargs):
        self.chat_type = kwargs.get("chat_type", "chat-completion")
        kwargs["chat_type"] = self.chat_type
        endpoint = kwargs.pop("endpoint", None)

        if (endpoint is not None) and (endpoint != "https://api.openai.com/v1"):
            if self.chat_type == "completions":
                self.openai_class = OpenAiCompletionServer(endpoint=endpoint, **kwargs)
            else:
                self.openai_class = OpenAiServer(endpoint=endpoint, **kwargs)
        else:
            if self.chat_type == "completions":
                self.openai_class = OpenAiCompletion(**kwargs)
            else:
                self.openai_class = OpenAiClient(False, **kwargs)

    def ask(self, **kwargs) -> Dict:
        return self.openai_class.ask(**kwargs)

    def embeddings(self, **kwargs) -> List[float]:
        return self.openai_class.embeddings(**kwargs)


class AzureOpenAiClientController:
    def __init__(self, **kwargs):
        self.chat_type = kwargs.pop("chat_type", "chat-completion")
        kwargs["chat_type"] = self.chat_type
        if self.chat_type == "completions":
            self.azure_openai_class = AzureOpenAiCompletion(**kwargs)
        else:
            self.azure_openai_class = OpenAiClient(True, **kwargs)

    def ask(self, **kwargs) -> Dict:
        return self.azure_openai_class.ask(**kwargs)

    def embeddings(self, **kwargs) -> List[float]:
        return self.azure_openai_class.embeddings(**kwargs)
