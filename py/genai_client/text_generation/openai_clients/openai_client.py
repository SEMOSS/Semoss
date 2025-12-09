from typing import Any, Dict, TYPE_CHECKING, Union

if TYPE_CHECKING:

    def smss_stream(
        data: Any, stream_type: str = "content", interim: bool = True
    ) -> None: ...


import json
from openai import OpenAI, AzureOpenAI
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...constants import AskModelEngineResponse
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil
from ...message_builders.openai.openai_message_builder import OpenAIMessageBuilder
from smss_thread_local import get_smss_stream
from .openai_image_client import OpenAiImageClient
from .openai_audio_client import OpenAiAudioClient
from ...tokenizers.vllm_tokenizer import VLLMTokenizer
from ...tokenizers.tgi_tokenizer import TGITokenizer
from ...tokenizers.openai_tokenizer import OpenAiTokenizer
from ...tokenizers.huggingface_tokenizer import HuggingfaceTokenizer


class OpenAiClient(AbstractTextGenerationClient):
    PARENT_PARAMS = {
        "template",
        "template_name",
        "model_name",
        "model_type",
        "context_window",
        "max_input_tokens",
        "max_tokens",
        "max_completion_tokens",
        "ai_role",
        "user_role",
        "system_role",
        "chat_type",
        "tokens_param_name",
        "thinking",
        "thinking_budget",
        "global_param_override",
    }

    def __init__(
        self,
        is_azure: bool,
        api_key: str,
        **kwargs,
    ):
        self.is_azure = is_azure
        self.endpoint = kwargs.pop("endpoint", None)
        if self.endpoint:
            kwargs["base_url"] = self.endpoint
        chat_type = kwargs.pop("chat_type", "chat-completion")
        kwargs["chat_type"] = chat_type
        self.deployment_type = kwargs.pop("deployment_type", "openai").lower()
        parent_kwargs = {k: v for k, v in kwargs.items() if k in self.PARENT_PARAMS}
        client_kwargs = {k: v for k, v in kwargs.items() if k not in self.PARENT_PARAMS}

        super().__init__(**parent_kwargs)

        self.chat_type = self.model_settings.chat_type
        self.tokenizer = self._get_tokenizer(kwargs)
        self.client = self._get_client(api_key, is_azure, **client_kwargs)

        self.message_builder = OpenAIMessageBuilder(self.model_settings, self.chat_type)
        self.image_client = OpenAiImageClient(client=self)
        self.audio_client = OpenAiAudioClient(client=self)

    def _get_tokenizer(
        self, init_args: Dict = {}
    ) -> Union[VLLMTokenizer, OpenAiTokenizer]:
        if not self.is_azure and self.endpoint and self.endpoint.strip():
            if self.deployment_type == "vllm":
                return VLLMTokenizer(
                    model_name=self.model_settings.model_name,
                    endpoint=self.endpoint,
                    api_key=init_args.get("api_key", "EMPTY"),
                )
            elif self.deployment_type == "tgi":
                return TGITokenizer(
                    endpoint=self.endpoint, api_key=init_args.get("api_key", "EMPTY")
                )
            else:
                return HuggingfaceTokenizer(
                    encoder_name=init_args.get("tokenizer_name", None)
                    or self.model_settings.model_name,
                    max_tokens=self.model_settings.max_completion_tokens,
                    max_input_tokens=self.model_settings.max_input_tokens,
                    context_window=self.model_settings.context_window,
                    max_completion_tokens=self.model_settings.max_completion_tokens,
                )
        return OpenAiTokenizer(
            encoder_name=init_args.get("tokenizer_name", None)
            or self.model_settings.model_name,
            max_tokens=self.model_settings.max_completion_tokens,
            max_input_tokens=self.model_settings.max_input_tokens,
            context_window=self.model_settings.context_window,
            max_completion_tokens=self.model_settings.max_completion_tokens,
        )

    def _get_client(
        self, api_key: str, is_azure: bool, **kwargs
    ) -> Union[OpenAI, AzureOpenAI]:
        if is_azure:
            endpoint = kwargs.pop("endpoint", None)
            if endpoint is None:
                endpoint = kwargs.pop("base_url", None)
            kwargs["azure_endpoint"] = endpoint
            return AzureOpenAI(api_key=api_key, **kwargs)
        return OpenAI(api_key=api_key, **kwargs)

    def ask_call(self, prefix: str = "", **kwargs) -> AskModelEngineResponse:
        # Remove chat_type from kwargs if present - it's for routing only, not for OpenAI API
        chat_type = kwargs.pop("chat_type", None)

        # Raw relay mode for Responses API: If raw_passthrough flag is set,
        # skip all message building and send Codex's request directly to OpenAI
        if chat_type == "responses" and kwargs.get("raw_passthrough", False):
            kwargs.pop("raw_passthrough")  # Remove the flag itself
            passthrough_request = self._prepare_responses_passthrough_request(dict(kwargs))
            return self.handle_responses_response(passthrough_request, prefix=prefix)

        semoss_messages = self.build_semoss_messages(
            model_settings=self.model_settings, **kwargs
        )

        if self.model_settings.model_type == "audio":
            last_message = semoss_messages[-1]
            text = last_message.content if hasattr(last_message, "content") else ""
            return self.audio_client.ask(text, **kwargs)

        try:
            openai_messages = self.message_builder.build_request(semoss_messages)
        except Exception as e:
            raise ValueError(f"Error building OpenAI messages: {e}") from e

        # moving streaming param into openai_messages rather than kwargs
        streaming = kwargs.pop("stream", True)
        if self.chat_type == "chat-completion" and streaming:
            openai_messages.update(
                {"stream": True, "stream_options": {"include_usage": True}}
            )
        elif self.chat_type == "responses" and streaming:
            openai_messages.update({"stream": True})

        if (
            hasattr(self.model_settings, "global_param_override")
            and self.model_settings.global_param_override
        ):
            openai_messages.update(self.model_settings.global_param_override)

        if self.model_settings.model_type == "image":
            return self.image_client.ask(openai_messages, **kwargs)
        if self.chat_type == "chat-completion":
            return self.handle_chat_completion_response(openai_messages, prefix=prefix)
        elif self.chat_type == "responses":
            return self.handle_responses_response(openai_messages, prefix=prefix)
        elif self.chat_type == "completions":
            return self.handle_completions_response(openai_messages, prefix=prefix)
        else:
            raise ValueError("Invalid chat type")

    def handle_completions_response(
        self,
        request: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse:
        response = self.client.completions.create(
            model=self.model_settings.model_name, **request
        )
        if request.get("stream", True):
            final_query = ""
            for chunk in response:
                if "text" in chunk:
                    content = chunk.choices[0].text
                    if content != None:
                        final_query += content
                        print(prefix + content, end="")
            response_tokens = 0
            input_tokens = 0
        else:
            final_query = response.choices[0].text
            response_tokens = response.usage.completion_tokens
            input_tokens = response.usage.prompt_tokens

        model_engine_response = AskModelEngineResponse(
            response=final_query,
            response_tokens=response_tokens,
            prompt_tokens=input_tokens,
        )

        return model_engine_response

    def handle_responses_response(
        self,
        request: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse:
        smss_stream = get_smss_stream()

        # Always override model with the engine-configured value
        request = dict(request)
        request["model"] = self.model_settings.model_name

        if request.get("stream", False):
            with self.client.responses.with_streaming_response.create(
                **request
            ) as stream_response:
                response_tokens = 0
                input_tokens = 0

                streamed_tools = {}
                finish_reason = None
                aggregated_content = ""
                aggregated_thinking = ""
                response_stream = stream_response.parse()
                for chunk in response_stream:
                    # Convert chunk to dict for passthrough
                    chunk_dict = chunk.model_dump() if hasattr(chunk, 'model_dump') else chunk.dict()

                    # Pass through raw chunk as SSE event
                    smss_stream(chunk_dict, stream_type="raw_sse")

                    # Still aggregate for final response tracking
                    if "response.completed" in chunk.type:
                        response_tokens = chunk.response.usage.output_tokens
                        input_tokens = chunk.response.usage.input_tokens
                        finish_reason = chunk.response.status

                    # Aggregate text content for final response
                    if "response.output_text.delta" in chunk.type:
                        content = chunk.delta
                        if content is not None:
                            aggregated_content += content
                            print(prefix + content, end="")

                    # Aggregate thinking/reasoning for final response
                    if "response.reasoning_summary_text.delta" in chunk.type:
                        content = chunk.delta
                        if content is not None:
                            aggregated_thinking += content
                            print(prefix + content, end="")

                    # Track tool calls for final response
                    if hasattr(chunk, "type") and chunk.type == "response.output_item.done":
                        item = getattr(chunk, "item", None)
                        if item and getattr(item, "type", None) == "function_call":
                            idx = getattr(chunk, "output_index", 0)
                            if idx not in streamed_tools:
                                streamed_tools[idx] = {
                                    "id": None,
                                    "type": None,
                                    "name": None,
                                    "arguments": "",
                                }
                            if hasattr(item, "id") and item.id is not None:
                                streamed_tools[idx]["id"] = item.id
                                print(prefix + str({"id": item.id}), end="")

                            if hasattr(item, "type") and item.type is not None:
                                streamed_tools[idx]["type"] = item.type
                                print(prefix + str({"type": item.type}), end="")

                            if hasattr(item, "name") and item.name is not None:
                                streamed_tools[idx]["name"] = item.name
                                print(prefix + str({"name": item.name}), end="")

                            if hasattr(item, "arguments") and item.arguments is not None:
                                streamed_tools[idx]["arguments"] += item.arguments
                                print(prefix + str({"arguments": item.arguments}), end="")

                if streamed_tools:
                    data = StreamUtil.create_finish_reason_chunk(finish_reason)
                    smss_stream(data, stream_type="tool", interim=False)
                    final_tool_calls = [
                        streamed_tools[idx] for idx in sorted(streamed_tools.keys())
                    ]
                    tool_result = []
                    for tool_call in final_tool_calls:
                        try:
                            arguments = json.loads(tool_call["arguments"])
                        except json.decoder.JSONDecodeError:
                            arguments = tool_call["arguments"]

                        tool_result.append(
                            {
                                "id": tool_call["id"],
                                "type": tool_call["type"],
                                "name": tool_call["name"],
                                "arguments": arguments,
                            }
                        )

                    return AskModelEngineResponse(
                        response=tool_result,
                        prompt_tokens=input_tokens,
                        response_tokens=response_tokens,
                        thinking=aggregated_thinking,
                        messageType="TOOL",
                    )
                else:
                    data = StreamUtil.create_finish_reason_chunk(finish_reason)
                    smss_stream(data, stream_type="content", interim=False)

                    result = AskModelEngineResponse(
                        response=aggregated_content,
                        response_tokens=response_tokens,
                        prompt_tokens=input_tokens,
                        thinking=aggregated_thinking,
                    )
                    return result
        else:
            raw_response = self.client.responses.with_raw_response.create(
                **request
            )
            response = raw_response.parse()

            response_tokens = response.usage.output_tokens
            input_tokens = response.usage.input_tokens

            final_content = response.output_text

            for output in response.output:
                if output.type == "function_call":
                    return self._parse_tools_call_response(
                        response=response,
                        response_tokens=response_tokens,
                        prompt_tokens=input_tokens,
                    )
            return AskModelEngineResponse(
                response=final_content,
                response_tokens=response_tokens,
                prompt_tokens=input_tokens,
                thinking=self._extract_reasoning_summary(response),
            )

    def handle_chat_completion_response(
        self,
        request: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse:
        smss_stream = get_smss_stream()

        response = self.client.chat.completions.create(
            model=self.model_settings.model_name, **request
        )

        if request.get("stream", False):
            response_tokens = 0
            prompt_tokens = 0

            streamed_tools = {}
            finish_reason = None
            aggregated_content = ""
            for chunk in response:
                # Usage info typically comes in the final chunk
                if hasattr(chunk, "usage") and chunk.usage is not None:
                    response_tokens = chunk.usage.completion_tokens
                    prompt_tokens = chunk.usage.prompt_tokens

                if chunk.choices and (len(chunk.choices) > 0):
                    # streaming text
                    if chunk.choices[0].delta.content:
                        content = chunk.choices[0].delta.content
                        if content is not None:
                            aggregated_content += content
                            data = StreamUtil.create_content_chunk(content)
                            smss_stream(data, stream_type="content")
                            print(prefix + content, end="")

                    if chunk.choices[0].delta.tool_calls:
                        tool_calls = chunk.choices[0].delta.tool_calls
                        if tool_calls:
                            for tool_call in tool_calls:
                                idx = tool_call.index
                                if idx not in streamed_tools:
                                    streamed_tools[idx] = {
                                        "id": None,
                                        "type": None,
                                        "function": {"name": None, "arguments": ""},
                                    }

                                if (
                                    hasattr(tool_call, "id")
                                    and tool_call.id is not None
                                ):
                                    streamed_tools[idx]["id"] = tool_call.id
                                    data = StreamUtil.create_tool_id_chunk(
                                        idx, tool_call.id
                                    )
                                    smss_stream(data, stream_type="tool")
                                    print(prefix + str(data), end="")

                                if (
                                    hasattr(tool_call, "type")
                                    and tool_call.type is not None
                                ):
                                    streamed_tools[idx]["type"] = tool_call.type
                                    data = StreamUtil.create_tool_type_chunk(
                                        idx, tool_call.type
                                    )
                                    smss_stream(data, stream_type="tool")
                                    print(prefix + str(data), end="")

                                if hasattr(tool_call, "function"):
                                    fn = tool_call.function
                                    if hasattr(fn, "name") and fn.name is not None:
                                        streamed_tools[idx]["function"][
                                            "name"
                                        ] = fn.name
                                        data = StreamUtil.create_function_name_chunk(
                                            idx, fn.name
                                        )
                                        smss_stream(data, stream_type="tool")
                                        print(prefix + str(data), end="")

                                    if (
                                        hasattr(fn, "arguments")
                                        and fn.arguments is not None
                                    ):
                                        streamed_tools[idx]["function"][
                                            "arguments"
                                        ] += fn.arguments
                                        data = (
                                            StreamUtil.create_function_arguments_chunk(
                                                idx, fn.arguments
                                            )
                                        )
                                        smss_stream(data, stream_type="tool")
                                        print(prefix + str(data), end="")

                    # Check if this chunk has a finish_reason
                    if chunk.choices[0].finish_reason:
                        finish_reason = chunk.choices[0].finish_reason

            if streamed_tools:
                data = StreamUtil.create_finish_reason_chunk(finish_reason)
                smss_stream(data, stream_type="tool", interim=False)
                final_tool_calls = [
                    streamed_tools[idx] for idx in sorted(streamed_tools.keys())
                ]
                # we flatten out the tool calls
                tool_result = []
                for tool_call in final_tool_calls:
                    # tool_call is a normal dict, need to use [] to pull keys
                    try:
                        arguments = json.loads(tool_call["function"]["arguments"])
                    except json.decoder.JSONDecodeError:
                        arguments = tool_call["function"]["arguments"]

                    tool_result.append(
                        {
                            "id": tool_call["id"],
                            "type": tool_call["type"],
                            "name": tool_call["function"]["name"],
                            "arguments": arguments,
                        }
                    )

                return AskModelEngineResponse(
                    response=tool_result,
                    prompt_tokens=prompt_tokens,
                    response_tokens=response_tokens,
                    messageType="TOOL",
                )
            else:
                data = StreamUtil.create_finish_reason_chunk(finish_reason)
                smss_stream(data, stream_type="content", interim=False)

                return AskModelEngineResponse(
                    response=aggregated_content,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                    thinking=self._extract_reasoning_summary_chat(response),
                )
        else:
            response_tokens = response.usage.completion_tokens
            prompt_tokens = response.usage.prompt_tokens

            final_content = response.choices[0].message.content
            tool_calls = response.choices[0].message.tool_calls
            if tool_calls:
                return self._parse_tools_call_response(
                    response=response,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                )
            else:
                return AskModelEngineResponse(
                    response=final_content,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                    thinking=self._extract_reasoning_summary_chat(response),
                )

    def _parse_tools_call_response(
        self,
        response,
        response_tokens: int,
        prompt_tokens: int,
    ) -> AskModelEngineResponse:
        tools_result = []

        if self.chat_type == "chat-completion":
            for i, tool_call in enumerate(response.choices[0].message.tool_calls):
                try:
                    arguments = json.loads(tool_call.function.arguments)
                except json.decoder.JSONDecodeError:
                    arguments = tool_call.function.arguments

                tools_result.append(
                    {
                        "id": tool_call.id,
                        "type": tool_call.type,
                        "name": tool_call.function.name,
                        "arguments": arguments,
                    }
                )

        elif self.chat_type == "responses":
            for tool_call in response.output:
                # Only process items where type == "function_call"
                if getattr(tool_call, "type", None) != "function_call":
                    continue

                if isinstance(tool_call.arguments, str):
                    try:
                        arguments = json.loads(tool_call.arguments)
                    except json.decoder.JSONDecodeError:
                        arguments = tool_call.arguments
                else:
                    # Already a dict/object
                    arguments = tool_call.arguments

                tools_result.append(
                    {
                        "id": tool_call.call_id,
                        "type": tool_call.type,
                        "name": tool_call.name,
                        "arguments": arguments,
                    }
                )

        return AskModelEngineResponse(
            response=tools_result,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            messageType="TOOL",
        )

    def _extract_reasoning_summary(self, response) -> str:
        """Extract reasoning summary from Responses API response."""
        output = getattr(response, "output", None)
        if not output:
            return ""

        for item in output:
            if getattr(item, "type", None) != "reasoning":
                continue

            summaries = getattr(item, "summary", None) or []
            texts = [getattr(s, "text", None) for s in summaries]
            # TODO: adjust joining of summary texts as needed
            if texts:
                return "\n\n".join(texts)

        return ""

    def _extract_reasoning_summary_chat(self, response) -> str:
        """Extract reasoning metadata from Chat Completion API response.

        Note: Chat-completion API does not expose reasoning text,
        only token counts in usage stats.
        """
        if not hasattr(response, "usage"):
            return ""

        usage = response.usage
        if hasattr(usage, "completion_tokens_details"):
            details = usage.completion_tokens_details
            reasoning_tokens = getattr(details, "reasoning_tokens", 0)
            if reasoning_tokens > 0:
                return f"Reasoning used {reasoning_tokens} tokens - text not available via chat-completion API"

        return ""

    def _prepare_responses_passthrough_request(
        self, request_kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Prepare passthrough payload, only touching model and token fields."""

        request_params: Dict[str, Any] = dict(request_kwargs)
        dropped_params = []

        for reserved in ("chat_type", "raw_passthrough", "model"):
            if reserved in request_params:
                request_params.pop(reserved, None)

        if request_params.pop("message_json", None) is not None:
            dropped_params.append("message_json")

        max_output_tokens = request_params.get("max_output_tokens")
        if max_output_tokens is None:
            for legacy_field in (
                "max_completion_tokens",
                "max_tokens",
                "max_new_tokens",
            ):
                if legacy_field in request_params:
                    max_output_tokens = request_params.pop(legacy_field)
                    dropped_params.append(legacy_field)
                    break
        else:
            for legacy_field in (
                "max_completion_tokens",
                "max_tokens",
                "max_new_tokens",
            ):
                if legacy_field in request_params:
                    request_params.pop(legacy_field, None)
                    dropped_params.append(legacy_field)

        if max_output_tokens is not None:
            request_params["max_output_tokens"] = max_output_tokens

        return request_params
