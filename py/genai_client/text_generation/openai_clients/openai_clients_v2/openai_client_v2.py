from typing import Any, Dict
from ....message_builders.openai.openai_message_builder import OpenAIMessageBuilder
from ..abstract_openai_client import AbstractOpenAiClient
from ....constants import AskModelEngineResponse


class OpenAIClientV2(AbstractOpenAiClient):
    def __init__(self, client, chat_type: str):
        # I won't need to do this in the future
        self.cfg_client = client
        self.chat_type = chat_type
        self.message_builder = OpenAIMessageBuilder(
            self.cfg_client.model_settings, chat_type
        )

    def ask_call(self, prefix: str = "", **kwargs) -> AskModelEngineResponse:
        self.ask_settings = self.get_ask_settings(
            self.cfg_client.model_settings, **kwargs
        )

        if self.ask_settings.semoss_messages is None:
            raise ValueError("semoss_messages is required")

        request_params = self.message_builder.build_request(
            self.ask_settings.semoss_messages
        )

        if self.chat_type == "chat-completion":
            return self.handle_chat_completion_response(request_params, prefix=prefix)
        elif self.chat_type == "responses":
            return self.handle_responses_response(request_params, prefix=prefix)
        elif self.chat_type == "completions":
            raise ValueError("Completions are not supported")
        else:
            raise ValueError("Invalid chat type")

    def handle_responses_response(
        self,
        request: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse:
        response = self.cfg_client.client.responses.create(
            model=self.cfg_client.model_settings.model_name, **request
        )
        if request.get("stream", False):
            final_query = ""
            for chunk in response:
                if "delta" in chunk.type:
                    content = chunk.delta
                    if content != None:
                        final_query += content
                        print(prefix + content, end="")
            return AskModelEngineResponse(
                response=final_query, response_tokens=0, prompt_tokens=0
            )
        else:
            final_query = response.output_text
            response_tokens = response.usage.output_tokens
            input_tokens = response.usage.input_tokens
        return AskModelEngineResponse(
            response=final_query,
            response_tokens=response_tokens,
            prompt_tokens=input_tokens,
        )

    def handle_chat_completion_response(
        self,
        request: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse:

        response = self.cfg_client.client.chat.completions.create(
            model=self.cfg_client.model_settings.model_name, **request
        )

        if request.get("stream", False):
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
        else:

            final_query = response.choices[0].message.content
            response_tokens = response.usage.completion_tokens

            return AskModelEngineResponse(
                response=final_query, response_tokens=response_tokens, prompt_tokens=0
            )
