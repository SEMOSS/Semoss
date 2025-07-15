from ....message_builders.openai.openai_message_builder import OpenAIMessageBuilder
from ..abstract_openai_client import AbstractOpenAiClient
from ....constants import AskModelEngineResponse


class OpenAIChatCompletionV2(AbstractOpenAiClient):
    def __init__(self, client):
        # I won't need to do this in the future
        self.cfg_client = client
        self.message_builder = OpenAIMessageBuilder(self.cfg_client.model_settings)

    def ask_call(self, prefix: str = "", **kwargs) -> AskModelEngineResponse:
        self.ask_settings = self.get_ask_settings(**kwargs)

        data = self.message_builder.build_request(self.ask_settings.semoss_messages)

        response = self.cfg_client.client.chat.completions.create(
            model=data.pop("model"), **data
        )

        if data.get("stream", False):
            return self.handle_streaming_response(data, prefix=prefix)

        final_query = response.choices[0].message.content
        response_tokens = response.usage.completion_tokens

        return AskModelEngineResponse(
            response=final_query, response_tokens=response_tokens, prompt_tokens=0
        )

    def handle_streaming_response(
        self,
        data,
        prefix: str = "",
    ) -> AskModelEngineResponse:
        response = self.cfg_client.client.chat.completions.create(
            model=data.pop("model"), **data
        )

        final_query = ""
        for chunk in response:
            if chunk.choices and (len(chunk.choices) > 0):
                content = chunk.choices[0].delta.content
                if content != None:
                    final_query += content
                    print(prefix + content, end="")

        return AskModelEngineResponse(
            response=final_query, response_tokens=0, prompt_tokens=0
        )