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

    def _get_tokenizer(self, init_args):
        """Retrieve the appropriate tokenizer for the model."""
        try:
            tiktoken.encoding_for_model(self.model_name)
        except Exception:
            init_args["tokenizer_name"] = init_args.pop(
                "openai_model_name", "gpt-3.5-turbo"
            )
        return super()._get_tokenizer(init_args)

    def _get_client(self, api_key, **kwargs):
        """Initialize the Azure OpenAI client."""
        kwargs.pop("model_name", None)
        from openai import AzureOpenAI

        return AzureOpenAI(api_key=api_key, **kwargs)
