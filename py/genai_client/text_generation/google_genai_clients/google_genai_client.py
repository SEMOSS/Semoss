from typing import List, Optional, Dict
from pydantic import BaseModel
from google.genai import types
from ...clients.google_clients import (
    GoogleClient,
    GoogleClientConfig,
    GoogleClientType,
)
from ...constants import AskModelEngineResponse, TEMPLATE, TEMPLATE_NAME
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...message_builders.google_genai.google_genai_builder import (
    GoogleGenAIMessageBuilder,
)
from ...retry_handler import RetryHandler


class UsageMetadata(BaseModel):
    candidates_token_count: int
    prompt_token_count: int


class StreamingResponse(BaseModel):
    text: str
    usage_metadata: Optional[UsageMetadata] = None

    class Config:
        arbitrary_types_allowed = True


class GoogleGenAiTextClient(AbstractTextGenerationClient):
    def __init__(
        self,
        service_account_credentials: Dict = None,
        service_account_key_file: str = None,
        region: str = None,
        project: str = None,
        api_key: str = None,
        safety_settings: dict = None,
        **kwargs,
    ):
        super().__init__(
            template=kwargs.pop(TEMPLATE, None),
            template_name=kwargs.pop(TEMPLATE_NAME, None),
            **kwargs,
        )
        self.client_config = GoogleClientConfig(
            type=GoogleClientType.GOOGLE,
            service_account_credentials=service_account_credentials,
            service_account_key_file=service_account_key_file,
            region=region,
            project=project,
            api_key=api_key,
        )
        self.client = GoogleClient(config=self.client_config).client

        self.safety_settings = safety_settings

        retries = kwargs.get("retries", 0)
        self.retry_handler = RetryHandler(max_retries=retries)

    def ask_call(
        self,
        prefix="",
        **kwargs,
    ):
        if self.client is None:
            raise ValueError("Google Gen AI client is not initialized.")

        semoss_messages = self.build_semoss_messages(self.model_settings, **kwargs)

        try:
            response = GoogleGenAIMessageBuilder().build_messages(semoss_messages)
            google_messages = response["messages"]
            provider_config = response["provider_config"]
            stream = response["stream"]
        except Exception as e:
            raise RuntimeError(f"Failed to build messages from SEMOSS messages: {e}")

        if stream:

            def streaming_call():
                return self._handle_streaming(
                    prefix=prefix,
                    contents=google_messages,
                    config=provider_config,
                )

            model_response = self.generate_with_retry(streaming_call)
        else:

            def call_generate_content():
                return self.client.models.generate_content(
                    model=self.model_name,
                    contents=google_messages,
                    config=provider_config,
                )

            model_response = self.generate_with_retry(call_generate_content)

        response_tokens = model_response.usage_metadata.candidates_token_count
        prompt_tokens = model_response.usage_metadata.prompt_token_count

        if len(getattr(model_response, "function_calls", None) or []) > 0:
            return self._parse_tools_call_response(
                response=model_response,
                response_tokens=response_tokens,
                prompt_tokens=prompt_tokens,
            )

        return AskModelEngineResponse(
            response=model_response.text,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            messageType="CHAT",
        )

    def generate_with_retry(self, generate_func, *args, **kwargs):
        """Helper to run a generation call with retry."""
        if callable(generate_func):
            wrapped = self.retry_handler.retry(generate_func)
            return wrapped(*args, **kwargs)
        return generate_func

    def _parse_tools_call_response(
        self,
        response: types.GenerateContentResponse,
        response_tokens: int,
        prompt_tokens: int,
    ) -> AskModelEngineResponse:
        tools_result = []
        for i, function_call in enumerate(response.function_calls):
            function_id = str(i)

            tools_result.append(
                {
                    "id": function_id,
                    "type": "function",
                    "name": function_call.name,
                    "arguments": getattr(function_call, "args", {}),
                }
            )
        return AskModelEngineResponse(
            response=tools_result,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            messageType="TOOL",
        )

    def _handle_streaming(
        self,
        contents: List[types.Content],
        config: types.GenerateContentConfig,
        prefix: Optional[str] = "",
    ) -> StreamingResponse:
        final_response = ""

        for chunk in self.client.models.generate_content_stream(
            model=self.model_name, contents=contents, config=config
        ):
            final_response += chunk.text
            print(prefix + chunk.text, end="")

        input_tokens = self._count_tokens(contents)

        response_content = [
            types.Content(
                role="model", parts=[types.Part.from_text(text=final_response)]
            )
        ]
        output_tokens = self._count_tokens(response_content)

        usage_metadata = UsageMetadata(
            candidates_token_count=output_tokens,
            prompt_token_count=input_tokens,
        )

        return StreamingResponse(text=final_response, usage_metadata=usage_metadata)

    def _count_tokens(self, contents: List[types.Content]) -> int:
        try:
            response = self.client.models.count_tokens(
                model=self.model_name,
                contents=contents,
            )
            return response.total_tokens
        except Exception as e:
            raise RuntimeError(f"Failed to count tokens: {e}")
