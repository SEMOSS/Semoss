from .openai_embedder import OpenAiEmbedder
from ..utils import azure_openai_client


class AzureOpenAiEmbedder(OpenAiEmbedder):

    def __init__(
        self, model_name: str, api_key: str, endpoint: str, api_version: str, **kwargs
    ):
        tokenizer_model = kwargs.pop("openai_model_name", model_name)
        super().__init__(
            model_name=tokenizer_model,
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=api_version,
            **kwargs
        )

        self.model_name = model_name

    def _get_client(self, api_key, **kwargs):
        return azure_openai_client(api_key, kwargs.pop("azure_endpoint"), **kwargs)
