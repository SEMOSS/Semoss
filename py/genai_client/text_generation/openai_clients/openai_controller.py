from typing import List, Dict
from .openai_client import OpenAiClient


class OpenAiClientController:
    def __init__(self, **kwargs):
        self.openai_class = OpenAiClient(False, **kwargs)

    def ask(self, **kwargs) -> Dict:
        return self.openai_class.ask(**kwargs)

    def embeddings(self, **kwargs) -> List[float]:
        return self.openai_class.embeddings(**kwargs)

    def submit_batch(self, **kwargs):
        return self.openai_class.submit_batch(**kwargs)

    def get_batch_status(self, **kwargs):
        return self.openai_class.get_batch_status(**kwargs)

    def get_batch_results(self, **kwargs):
        return self.openai_class.get_batch_results(**kwargs)

    def list_batches(self, **kwargs):
        return self.openai_class.list_batches(**kwargs)

    def cancel_batch(self, **kwargs):
        return self.openai_class.cancel_batch(**kwargs)


class AzureOpenAiClientController:
    def __init__(self, **kwargs):
        self.chat_type = kwargs.pop("chat_type", "chat-completion")
        kwargs["chat_type"] = self.chat_type
        self.azure_openai_class = OpenAiClient(True, **kwargs)

    def ask(self, **kwargs) -> Dict:
        return self.azure_openai_class.ask(**kwargs)

    def embeddings(self, **kwargs) -> List[float]:
        return self.azure_openai_class.embeddings(**kwargs)

    def submit_batch(self, **kwargs):
        return self.azure_openai_class.submit_batch(**kwargs)

    def get_batch_status(self, **kwargs):
        return self.azure_openai_class.get_batch_status(**kwargs)

    def get_batch_results(self, **kwargs):
        return self.azure_openai_class.get_batch_results(**kwargs)

    def list_batches(self, **kwargs):
        return self.azure_openai_class.list_batches(**kwargs)

    def cancel_batch(self, **kwargs):
        return self.azure_openai_class.cancel_batch(**kwargs)
