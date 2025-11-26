import json, base64
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
from smss_thread_local import get_smss_stream
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil


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
        service_account_credentials: Optional[Dict] = None,
        service_account_key_file: Optional[str] = None,
        region: Optional[str] = None,
        project: Optional[str] = None,
        api_key: Optional[str] = None,
        safety_settings: Optional[dict] = None,
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
            response = GoogleGenAIMessageBuilder().build_messages(
                semoss_messages, self.model_settings
            )
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

            return self.generate_with_retry(streaming_call)
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

        thinking_text = ""
        image_data = []

        if hasattr(model_response, "candidates") and len(model_response.candidates) > 0:
            first = model_response.candidates[0]
            if getattr(first, "content", None) and getattr(
                first.content, "parts", None
            ):
                for part in first.content.parts:
                    if getattr(part, "text", False) and getattr(part, "thought", False):
                        thinking_text += getattr(part, "text", "")
                    if part.inline_data:
                        image_data.append(
                            self._create_image_url(
                                mime_type=part.inline_data.mime_type,
                                image_bytes=part.inline_data.data,
                            )
                        )

        if thinking_text == "":
            thinking_text = None

        text_response = model_response.text if model_response.text else ""

        return AskModelEngineResponse(
            response=text_response,
            response_media=image_data,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            messageType="CHAT",
            thinking=thinking_text,
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
    ) -> AskModelEngineResponse:

        smss_stream = get_smss_stream()
        final_response = ""
        thinking_response = ""
        input_tokens = 0
        output_tokens = 0

        content_array = []
        this_content_block = {}

        tool_result = []

        try:
            stream = self.client.models.generate_content_stream(
                model=self.model_name, contents=contents, config=config
            )

            for event in stream:
                if hasattr(event, "candidates") and event.candidates:
                    candidate = event.candidates[0]
                    if hasattr(candidate, "content") and hasattr(
                        candidate.content, "parts"
                    ):
                        for part in candidate.content.parts:
                            if (
                                hasattr(part, "thought")
                                and part.thought
                                and hasattr(part, "text")
                            ):
                                thinking_response += part.text

                if event.text:
                    this_content_block["final_response"] = ""
                    text_chunk = event.text
                    this_content_block["final_response"] += text_chunk

                    data = StreamUtil.create_content_chunk(text_chunk)
                    smss_stream(data, stream_type="content")
                    print(prefix + text_chunk, end="", flush=True)

                    response_content = [
                        types.Content(
                            role="model",
                            parts=[
                                types.Part.from_text(
                                    text=this_content_block["final_response"]
                                )
                            ],
                        )
                    ]

                    output_tokens = self._count_tokens(response_content)

                    content_array.append(this_content_block)
                    this_content_block = {}

                if len(getattr(event, "function_calls", None) or []) > 0:
                    for i, function_call in enumerate(event.function_calls):
                        function_id = str(i)
                        this_content_block.update(
                            {
                                "id": function_id,
                                "type": "function",
                                "function": {"name": None, "arguments": ""},
                            }
                        )
                        this_content_block["function"]["name"] = function_call.name
                        data = StreamUtil.create_tool_id_chunk(
                            index=len(tool_result), tool_id=function_call.id
                        )
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                        data = StreamUtil.create_tool_type_chunk(index=len(tool_result))
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                        data = StreamUtil.create_function_name_chunk(
                            index=len(tool_result),
                            function_name=function_call.name,
                        )
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                        this_content_block["function"]["arguments"] = function_call.args

                        data = StreamUtil.create_function_arguments_chunk(
                            index=len(tool_result),
                            arguments_chunk=function_call.args,
                        )
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                        if isinstance(
                            this_content_block["function"]["arguments"], dict
                        ):
                            arguments = this_content_block["function"]["arguments"]
                        else:
                            try:
                                arguments = json.loads(
                                    this_content_block["function"]["arguments"]
                                )
                            except Exception:
                                arguments = this_content_block["function"]["arguments"]

                        tool_result.append(
                            {
                                "id": this_content_block["id"],
                                "type": this_content_block["type"],
                                "name": this_content_block["function"]["name"],
                                "arguments": arguments,
                            }
                        )

                        content_array.append(this_content_block)
                        this_content_block = {}

            input_tokens = self._count_tokens(contents)

            if tool_result:
                data = StreamUtil.create_finish_reason_chunk()
                smss_stream(data, stream_type="tool", interim=False)
            else:
                data = StreamUtil.create_finish_reason_chunk("stop")
                smss_stream(data, stream_type="content", interim=False)

            # aggregate text blocks
            for content in content_array:
                if content.get("final_response", None):
                    final_response += content.get("final_response")

            if tool_result:
                if config.response_schema:
                    is_schema, json_str = self._flatten_schema_tool(
                        tool_result, "return_json"
                    )
                    if is_schema:
                        return AskModelEngineResponse(
                            response=json_str,
                            response_tokens=output_tokens,
                            prompt_tokens=input_tokens,
                            messageType="CHAT",
                            thinking=thinking_response if thinking_response else None,
                        )
                else:
                    return AskModelEngineResponse(
                        response=tool_result,
                        response_tokens=output_tokens,
                        prompt_tokens=input_tokens,
                        messageType="TOOL",
                    )
            else:
                return AskModelEngineResponse(
                    response=final_response,
                    thinking=thinking_response if thinking_response else None,
                    response_tokens=output_tokens,
                    prompt_tokens=input_tokens,
                    messageType="CHAT",
                )
        except Exception as e:
            raise RuntimeError(f"Error during streaming: {e}")

    def _count_tokens(self, contents: List[types.Content]) -> int:
        try:
            response = self.client.models.count_tokens(
                model=self.model_name,
                contents=contents,
            )
            return response.total_tokens
        except Exception as e:
            raise RuntimeError(f"Failed to count tokens: {e}")

    def _flatten_schema_tool(self, tools_result, schema_tool_name: str = "return_json"):
        """
        If all tool_use entries are the schema pseudo-tool, return (True, json_str).
        If mixed tools or different tool, return (False, None).
        """
        if not tools_result:
            return False, None

        if any(tr.get("name") != schema_tool_name for tr in tools_result):
            return False, None

        payloads = [tr.get("arguments") for tr in tools_result]

        norm = []
        for p in payloads:
            if isinstance(p, (dict, list)):
                norm.append(p)
            elif isinstance(p, str):
                try:
                    norm.append(json.loads(p))
                except Exception:
                    norm.append(p)
            else:
                norm.append(p)

        if len(norm) == 1:
            final_py = norm[0]
        else:
            if all(isinstance(x, dict) for x in norm):
                merged = {}
                for d in norm:
                    merged.update(d)
                final_py = merged
            elif all(isinstance(x, list) for x in norm):
                arr = []
                for a in norm:
                    arr.extend(a)
                final_py = arr
            else:
                final_py = norm

        try:
            json_str = json.dumps(final_py, ensure_ascii=False)
        except Exception:
            json_str = str(final_py)

        return True, json_str

    def _create_image_url(self, mime_type: str, image_bytes: str):
        """Creating base64 string URL for generated image from bytes."""
        return (
            f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('utf-8')}"
        )
