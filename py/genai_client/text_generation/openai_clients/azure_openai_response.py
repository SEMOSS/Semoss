import tiktoken
from .openai_response_client import OpenAIResponses


class AzureOpenAIResponses(OpenAIResponses):
    def __init__(
        self,
        endpoint: str,
        model_name: str = None,
        api_key: str = "EMPTY",
        api_version="2025-03-01-preview",
        **kwargs
    ):
        if not endpoint:
            raise ValueError("Azure endpoint cannot be None or empty.")

        super().__init__(
            api_key=api_key,
            model_name=model_name,
            api_version=api_version,
            azure_endpoint=endpoint,
            **kwargs
        )
