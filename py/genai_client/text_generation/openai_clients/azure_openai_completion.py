import openai
from .openai_completion_client import OpenAiCompletion
import tiktoken


class AzureOpenAiCompletion(OpenAiCompletion):
    def __init__(
        self,
        endpoint: str = None,
        model_name: str = None,
        api_key: str = "EMPTY",
        api_version="2023-07-01-preview",
        **kwargs
    ):
        if not endpoint:
            raise ValueError("Azure endpoint cannot be None or empty.")

        super().__init__(api_key=api_key, model_name=model_name, **kwargs)

        openai.api_base = endpoint
        openai.api_type = "azure"
        openai.api_version = api_version

    def _inference_call(self, prefix: str, kwargs):
        final_query = ""
        responses = openai.Completion.create(
            engine=self.model_name, stream=True, **kwargs
        )

        for chunk in responses:
            if chunk.choices and len(chunk.choices) > 0:
                content = chunk.choices[0].get("delta", {}).get("content")
                if content != None:
                    final_query += content
                    print(prefix + content, end="")

        return final_query

    def _get_tokenizer(self, init_args):
        try:
            tiktoken.encoding_for_model(self.model_name)
        except Exception:
            init_args["tokenizer_name"] = init_args.pop(
                "openai_model_name", "gpt-3.5-turbo"
            )
        return super()._get_tokenizer(init_args)
