from typing import List, Dict
from .openai_chat_completion_client import OpenAiChatCompletion
from .openai_completion_client import OpenAiCompletion
from .azure_openai_chat_completion import AzureOpenAiChatCompletion
from .azure_openai_completion import AzureOpenAiCompletion
from .openai_api_inference_server import (
    OpenAiChatCompletionServer,
    OpenAiCompletionServer,
)


class OpenAiClientController:

    def __init__(self, **kwargs):

        self.chat_type = kwargs.pop("chat_type", "chat-completion")

        endpoint = kwargs.pop("endpoint", None)

        if (endpoint != None) and (endpoint != "https://api.openai.com/v1"):
            if self.chat_type == "chat-completion":
                self.openai_class = OpenAiChatCompletionServer(
                    endpoint=endpoint, **kwargs
                )
            else:
                self.openai_class = OpenAiCompletionServer(endpoint=endpoint, **kwargs)
        else:
            if self.chat_type == "chat-completion":
                self.openai_class = OpenAiChatCompletion(**kwargs)
            else:
                self.openai_class = OpenAiCompletion(**kwargs)

    def ask(self, **kwargs) -> Dict:
        return self.openai_class.ask(**kwargs)

    def embeddings(self, **kwargs) -> List[float]:
        return self.openai_class.embeddings(**kwargs)


class AzureOpenAiClientController:

    def __init__(self, **kwargs):

        self.chat_type = kwargs.pop("chat_type", "chat-completion")

        if self.chat_type == "chat-completion":
            self.azure_openai_class = AzureOpenAiChatCompletion(**kwargs)
        else:
            self.azure_openai_class = AzureOpenAiCompletion(**kwargs)

    def ask(self, **kwargs) -> Dict:
        return self.azure_openai_class.ask(**kwargs)

    def embeddings(self, **kwargs) -> List[float]:
        return self.azure_openai_class.embeddings(**kwargs)
