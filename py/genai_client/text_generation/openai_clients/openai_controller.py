from typing import List, Dict
from .openai_chat_completion_client import OpenAiChatCompletion
from .openai_completion_client import OpenAiCompletion
from .openai_response_client import OpenAIResponses
from .azure_openai_chat_completion import AzureOpenAiChatCompletion
from .azure_openai_completion import AzureOpenAiCompletion
from .azure_openai_response import AzureOpenAIResponses
from .openai_api_inference_server import (
    OpenAiChatCompletionServer,
    OpenAiCompletionServer,
    OpenAiResponsesServer,
)
from .openai_clients_v2.openai_client_v3 import FinalOpenAiClient


class OpenAiClientController:
    def __init__(self, **kwargs):
        self.chat_type = kwargs.get("chat_type", "chat-completion")
        kwargs["chat_type"] = self.chat_type
        endpoint = kwargs.pop("endpoint", None)

        if (endpoint is not None) and (endpoint != "https://api.openai.com/v1"):
            if self.chat_type == "chat-completion":
                self.openai_class = OpenAiChatCompletionServer(
                    endpoint=endpoint, **kwargs
                )
            elif self.chat_type == "responses":
                self.openai_class = OpenAiResponsesServer(endpoint=endpoint, **kwargs)
            else:
                self.openai_class = OpenAiCompletionServer(endpoint=endpoint, **kwargs)
        else:
            # if self.chat_type == "chat-completion":
            #     self.openai_class = OpenAiChatCompletion(**kwargs)
            # elif self.chat_type == "responses":
            #     self.openai_class = OpenAIResponses(**kwargs)
            # else:
            #     self.openai_class = OpenAiCompletion(**kwargs)
            if self.chat_type == "completions":
                self.openai_class = OpenAiCompletion(**kwargs)
            else:
                self.openai_class = FinalOpenAiClient(False, **kwargs)

    def ask(self, **kwargs) -> Dict:
        return self.openai_class.ask(**kwargs)

    def embeddings(self, **kwargs) -> List[float]:
        return self.openai_class.embeddings(**kwargs)


class AzureOpenAiClientController:
    def __init__(self, **kwargs):
        self.chat_type = kwargs.pop("chat_type", "chat-completion")
        kwargs["chat_type"] = self.chat_type
        # if self.chat_type == "chat-completion":
        #     self.azure_openai_class = AzureOpenAiChatCompletion(**kwargs)
        # elif self.chat_type == "responses":
        #     self.azure_openai_class = AzureOpenAIResponses(**kwargs)
        # else:
        #     self.azure_openai_class = AzureOpenAiCompletion(**kwargs)
        if self.chat_type == "completions":
            self.azure_openai_class = AzureOpenAiCompletion(**kwargs)
        else:
            self.azure_openai_class = FinalOpenAiClient(True, **kwargs)

    def ask(self, **kwargs) -> Dict:
        return self.azure_openai_class.ask(**kwargs)

    def embeddings(self, **kwargs) -> List[float]:
        return self.azure_openai_class.embeddings(**kwargs)
