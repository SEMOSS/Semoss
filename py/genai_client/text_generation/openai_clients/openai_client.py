from typing import Any, Dict, TYPE_CHECKING, Union

if TYPE_CHECKING:
    # Tokenizers are imported lazily inside _get_tokenizer() to keep heavy
    # dependencies (e.g. transformers, pulled in by HuggingfaceTokenizer) off
    # the client-construction path. These TYPE_CHECKING imports keep the type
    # hints valid without paying the runtime import cost.
    from ...tokenizers.vllm_tokenizer import VLLMTokenizer
    from ...tokenizers.openai_tokenizer import OpenAiTokenizer

    def smss_stream(
        data: Any, stream_type: str = "content", interim: bool = True
    ) -> None: ...


import json
from openai import OpenAI, AzureOpenAI
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...constants import AskModelEngineResponse2
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil
from ...message_builders.openai.openai_message_builder import OpenAIMessageBuilder
from smss_thread_local import get_smss_stream
from .openai_image_client import OpenAiImageClient
from .openai_audio_client import OpenAiAudioClient
from ..model_engine_exception import ModelEngineException, ErrorDetails
from ...utils import string_to_bool


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
        "simplify_messages",
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
        stream_kwarg = kwargs.pop("stream", None)
        if stream_kwarg is not None:
            override = kwargs.setdefault("global_param_override", {})
            if isinstance(override, dict):
                override.setdefault("stream", stream_kwarg)
        parent_kwargs = {k: v for k, v in kwargs.items() if k in self.PARENT_PARAMS}
        client_kwargs = {k: v for k, v in kwargs.items() if k not in self.PARENT_PARAMS}

        super().__init__(**parent_kwargs)

        self.chat_type = self.model_settings.chat_type
        self.tokenizer = self._get_tokenizer(kwargs)
        self.client = self._get_client(api_key, is_azure, **client_kwargs)
        self.simplify_messages = string_to_bool(
            parent_kwargs.get("simplify_messages", False)
        )

        self.message_builder = OpenAIMessageBuilder(
            self.model_settings, self.chat_type, self.simplify_messages
        )
        self.image_client = OpenAiImageClient(client=self)
        self.audio_client = OpenAiAudioClient(client=self)

    def _get_tokenizer(
        self, init_args: Dict = {}
    ) -> "Union[VLLMTokenizer, OpenAiTokenizer]":
        # Tokenizers are imported lazily so that constructing an OpenAI/Azure
        # client does not pull in heavy deps it never uses. In particular
        # HuggingfaceTokenizer imports `transformers` (~2.5s warm / much more
        # cold), which the default OpenAiTokenizer (tiktoken) path never needs.
        if not self.is_azure and self.endpoint and self.endpoint.strip():
            if self.deployment_type == "vllm":
                from ...tokenizers.vllm_tokenizer import VLLMTokenizer

                return VLLMTokenizer(
                    model_name=self.model_settings.model_name,
                    endpoint=self.endpoint,
                    api_key=init_args.get("api_key", "EMPTY"),
                )
            elif self.deployment_type == "tgi":
                from ...tokenizers.tgi_tokenizer import TGITokenizer

                return TGITokenizer(
                    endpoint=self.endpoint, api_key=init_args.get("api_key", "EMPTY")
                )
            else:
                from ...tokenizers.huggingface_tokenizer import HuggingfaceTokenizer

                return HuggingfaceTokenizer(
                    encoder_name=init_args.get("tokenizer_name", None)
                    or self.model_settings.model_name,
                    max_tokens=self.model_settings.max_completion_tokens,
                    max_input_tokens=self.model_settings.max_input_tokens,
                    context_window=self.model_settings.context_window,
                    max_completion_tokens=self.model_settings.max_completion_tokens,
                )
        from ...tokenizers.openai_tokenizer import OpenAiTokenizer

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

    def ask_call(
        self, prefix: str = "", **kwargs
    ) -> AskModelEngineResponse2 | ErrorDetails:
        try:
            semoss_messages = self.build_semoss_messages(
                model_settings=self.model_settings, **kwargs
            )

            if self.model_settings.model_type == "audio":
                return self.audio_client.ask(semoss_messages, **kwargs)

            if self.model_settings.model_type == "image":
                return self.image_client.ask_call(semoss_messages, **kwargs)

            try:
                openai_messages = self.message_builder.build_request(semoss_messages)
            except Exception as e:
                raise ValueError(f"Error building OpenAI messages: {e}") from e
            if self.chat_type == "chat-completion":
                return self.handle_chat_completion_response(
                    openai_messages, prefix=prefix
                )
            elif self.chat_type == "responses":
                return self.handle_responses_response(openai_messages, prefix=prefix)
            elif self.chat_type == "completions":
                return self.handle_completions_response(openai_messages, prefix=prefix)
            else:
                raise ValueError("Invalid chat type")
        except Exception as e:
            return ModelEngineException(
                error=e, client="openai", model=self.model_settings.model_name
            ).parse_error()

    def handle_completions_response(
        self,
        request: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse2:
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

        model_engine_response = AskModelEngineResponse2(
            response=final_query,
            response_tokens=response_tokens,
            prompt_tokens=input_tokens,
            schemaVersion=2,
            io="OUTPUT",
            parts=[{"type": "TEXT", "text": final_query}] if final_query else [],
        )

        return model_engine_response

    def handle_responses_response(
        self,
        request: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse2:
        smss_stream = get_smss_stream()

        response = self.client.responses.create(
            model=self.model_settings.model_name, **request
        )

        if request.get("stream", False):
            response_tokens = 0
            input_tokens = 0
            cache_read_tokens = None
            thinking_tokens = None

            streamed_tools = {}
            finish_reason = None
            aggregated_content = ""
            aggregated_thinking = ""
            for chunk in response:
                # Usage info typically comes in the final chunk
                if "response.completed" in chunk.type:
                    response_tokens = chunk.response.usage.output_tokens
                    input_tokens = chunk.response.usage.input_tokens
                    cache_read_tokens = self._extract_cached_tokens(
                        chunk.response.usage, details_attr="input_tokens_details"
                    )
                    thinking_tokens = self._extract_thinking_tokens(
                        chunk.response.usage, details_attr="output_tokens_details"
                    )
                    finish_reason = chunk.response.status

                    smss_stream(
                        StreamUtil.create_usage_chunk(
                            input_tokens=input_tokens,
                            output_tokens=response_tokens,
                            cache_read_input_tokens=cache_read_tokens,
                            reasoning_tokens=thinking_tokens,
                        ),
                        stream_type="usage",
                    )

                # streaming text and schema
                if "response.output_text.delta" in chunk.type:
                    content = chunk.delta
                    if content is not None:
                        aggregated_content += content
                        data = StreamUtil.create_content_chunk(content)
                        smss_stream(data, stream_type="content")
                        print(prefix + content, end="")

                # streaming text and schema
                if "response.reasoning_summary_text.delta" in chunk.type:
                    content = chunk.delta
                    if content is not None:
                        aggregated_thinking += content
                        data = StreamUtil.create_thinking_chunk(content)
                        smss_stream(data, stream_type="thinking")
                        print(prefix + content, end="")

                # streaming tool calls
                if hasattr(chunk, "type") and chunk.type == "response.output_item.done":
                    # if "response.function_call_arguments.done" in chunk.type:
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
                            data = StreamUtil.create_tool_id_chunk(idx, item.id)
                            smss_stream(data, stream_type="tool")
                            print(prefix + str(data), end="")

                        if hasattr(item, "type") and item.type is not None:
                            streamed_tools[idx]["type"] = item.type
                            data = StreamUtil.create_tool_type_chunk(idx, item.type)
                            smss_stream(data, stream_type="tool")
                            print(prefix + str(data), end="")

                        if hasattr(item, "name") and item.name is not None:
                            streamed_tools[idx]["name"] = item.name
                            data = StreamUtil.create_function_name_chunk(idx, item.name)
                            smss_stream(data, stream_type="tool")
                            print(prefix + str(data), end="")

                        if hasattr(item, "arguments") and item.arguments is not None:
                            streamed_tools[idx]["arguments"] += item.arguments
                            data = StreamUtil.create_function_arguments_chunk(
                                idx, item.arguments
                            )
                            smss_stream(data, stream_type="tool")
                            print(prefix + str(data), end="")

            if streamed_tools:
                data = StreamUtil.create_finish_reason_chunk(finish_reason)
                smss_stream(data, stream_type="tool", interim=False)
                final_tool_calls = [
                    streamed_tools[idx] for idx in sorted(streamed_tools.keys())
                ]
                # we flatten out the tool calls
                tool_result = []
                for tool_call in final_tool_calls:
                    try:
                        arguments = tool_call["arguments"]
                        # Return empty dict if arguments is empty string
                        if arguments == "":
                            arguments = {}
                        else:
                            arguments = json.loads(arguments)
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

                return AskModelEngineResponse2(
                    response=tool_result,
                    prompt_tokens=input_tokens,
                    response_tokens=response_tokens,
                    cache_read_tokens=cache_read_tokens,
                    thinking_tokens=thinking_tokens,
                    messageType="TOOL",
                    schemaVersion=2,
                    io="OUTPUT",
                    parts=(
                        (
                            [{"type": "THINKING", "thinking": aggregated_thinking}]
                            if aggregated_thinking
                            else []
                        )
                        # preamble text the model emitted alongside the tool calls
                        + (
                            [{"type": "TEXT", "text": aggregated_content}]
                            if aggregated_content
                            else []
                        )
                        + [{"type": "TOOL_CALL", "tool_call": t} for t in tool_result]
                    ),
                )
            else:
                data = StreamUtil.create_finish_reason_chunk(finish_reason)
                smss_stream(data, stream_type="content", interim=False)

                return AskModelEngineResponse2(
                    response=aggregated_content,
                    response_tokens=response_tokens,
                    prompt_tokens=input_tokens,
                    cache_read_tokens=cache_read_tokens,
                    thinking_tokens=thinking_tokens,
                    schemaVersion=2,
                    io="OUTPUT",
                    parts=(
                        (
                            [{"type": "THINKING", "thinking": aggregated_thinking}]
                            if aggregated_thinking
                            else []
                        )
                        + (
                            [{"type": "TEXT", "text": aggregated_content}]
                            if aggregated_content
                            else []
                        )
                    ),
                )
        else:
            response_tokens = response.usage.output_tokens
            input_tokens = response.usage.input_tokens
            cache_read_tokens = self._extract_cached_tokens(
                response.usage, details_attr="input_tokens_details"
            )
            thinking_tokens = self._extract_thinking_tokens(
                response.usage, details_attr="output_tokens_details"
            )
            final_content = response.output_text

            # non-stream tool calls
            for output in response.output:
                if output.type == "function_call":
                    return self._parse_tools_call_response(
                        response=response,
                        response_tokens=response_tokens,
                        prompt_tokens=input_tokens,
                        cache_read_tokens=cache_read_tokens,
                        thinking_tokens=thinking_tokens,
                    )
            else:
                reasoning = self._extract_reasoning_summary(response)
                return AskModelEngineResponse2(
                    response=final_content,
                    response_tokens=response_tokens,
                    prompt_tokens=input_tokens,
                    cache_read_tokens=cache_read_tokens,
                    thinking_tokens=thinking_tokens,
                    schemaVersion=2,
                    io="OUTPUT",
                    parts=(
                        (
                            [{"type": "THINKING", "thinking": reasoning}]
                            if reasoning
                            else []
                        )
                        + (
                            [{"type": "TEXT", "text": final_content}]
                            if final_content
                            else []
                        )
                    ),
                )

    def handle_chat_completion_response(
        self,
        request: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse2:
        smss_stream = get_smss_stream()

        response = self.client.chat.completions.create(
            model=self.model_settings.model_name, **request
        )

        if request.get("stream", False):
            response_tokens = 0
            prompt_tokens = 0
            cache_read_tokens = None
            thinking_tokens = None

            streamed_tools = {}
            finish_reason = None
            aggregated_content = ""
            for chunk in response:
                # Usage info typically comes in the final chunk
                if hasattr(chunk, "usage") and chunk.usage is not None:
                    response_tokens = chunk.usage.completion_tokens
                    prompt_tokens = chunk.usage.prompt_tokens
                    cache_read_tokens = self._extract_cached_tokens(chunk.usage)
                    thinking_tokens = self._extract_thinking_tokens(chunk.usage)

                    smss_stream(
                        StreamUtil.create_usage_chunk(
                            input_tokens=prompt_tokens,
                            output_tokens=response_tokens,
                            cache_read_input_tokens=cache_read_tokens,
                            reasoning_tokens=thinking_tokens,
                        ),
                        stream_type="usage",
                    )

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
                    try:
                        arguments = tool_call["function"]["arguments"]
                        # Return empty dict if arguments is empty string
                        if arguments == "":
                            arguments = {}
                        else:
                            arguments = json.loads(arguments)
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

                parts = (
                    [{"type": "TEXT", "text": aggregated_content}]
                    if aggregated_content
                    else []
                )
                parts += [{"type": "TOOL_CALL", "tool_call": t} for t in tool_result]

                return AskModelEngineResponse2(
                    response=tool_result,
                    prompt_tokens=prompt_tokens,
                    response_tokens=response_tokens,
                    cache_read_tokens=cache_read_tokens,
                    thinking_tokens=thinking_tokens,
                    messageType="TOOL",
                    schemaVersion=2,
                    io="OUTPUT",
                    parts=parts,
                )
            else:
                data = StreamUtil.create_finish_reason_chunk(finish_reason)
                smss_stream(data, stream_type="content", interim=False)

                reasoning = self._extract_reasoning_summary_chat(response)
                return AskModelEngineResponse2(
                    response=aggregated_content,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                    cache_read_tokens=cache_read_tokens,
                    thinking_tokens=thinking_tokens,
                    schemaVersion=2,
                    io="OUTPUT",
                    parts=(
                        (
                            [{"type": "THINKING", "thinking": reasoning}]
                            if reasoning
                            else []
                        )
                        + (
                            [{"type": "TEXT", "text": aggregated_content}]
                            if aggregated_content
                            else []
                        )
                    ),
                )
        else:
            if response.usage:
                response_tokens = response.usage.completion_tokens
                prompt_tokens = response.usage.prompt_tokens
                cache_read_tokens = self._extract_cached_tokens(response.usage)
                thinking_tokens = self._extract_thinking_tokens(response.usage)
            else:
                response_tokens = 0
                prompt_tokens = 0
                cache_read_tokens = None
                thinking_tokens = None

            final_content = response.choices[0].message.content
            tool_calls = response.choices[0].message.tool_calls
            if tool_calls:
                return self._parse_tools_call_response(
                    response=response,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                    cache_read_tokens=cache_read_tokens,
                    thinking_tokens=thinking_tokens,
                )
            else:
                reasoning = self._extract_reasoning_summary_chat(response)
                return AskModelEngineResponse2(
                    response=final_content,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                    cache_read_tokens=cache_read_tokens,
                    thinking_tokens=thinking_tokens,
                    schemaVersion=2,
                    io="OUTPUT",
                    parts=(
                        (
                            [{"type": "THINKING", "thinking": reasoning}]
                            if reasoning
                            else []
                        )
                        + (
                            [{"type": "TEXT", "text": final_content}]
                            if final_content
                            else []
                        )
                    ),
                )

    def _parse_tools_call_response(
        self,
        response,
        response_tokens: int,
        prompt_tokens: int,
        cache_read_tokens: "int | None" = None,
        thinking_tokens: "int | None" = None,
    ) -> AskModelEngineResponse2:
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

        # preamble text the model emitted alongside the tool calls
        preamble_text = None
        if self.chat_type == "chat-completion":
            preamble_text = response.choices[0].message.content
        elif self.chat_type == "responses":
            preamble_text = getattr(response, "output_text", None)

        text_parts = [{"type": "TEXT", "text": preamble_text}] if preamble_text else []

        return AskModelEngineResponse2(
            response=tools_result,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            cache_read_tokens=cache_read_tokens,
            thinking_tokens=thinking_tokens,
            messageType="TOOL",
            schemaVersion=2,
            io="OUTPUT",
            parts=text_parts
            + [{"type": "TOOL_CALL", "tool_call": t} for t in tools_result],
        )

    @staticmethod
    def _extract_cached_tokens(
        usage, details_attr: str = "prompt_tokens_details"
    ) -> "int | None":
        # usage.prompt_tokens_details.cached_tokens (Chat) / input_tokens_details (Responses); None if caching off
        if usage is None:
            return None
        details = getattr(usage, details_attr, None)
        if details is None:
            return None
        cached = getattr(details, "cached_tokens", None)
        return cached or None

    @staticmethod
    def _extract_thinking_tokens(
        usage, details_attr: str = "completion_tokens_details"
    ) -> "int | None":
        # usage.completion_tokens_details.reasoning_tokens (Chat) / output_tokens_details (Responses); None for non-reasoning models
        if usage is None:
            return None
        details = getattr(usage, details_attr, None)
        if details is None:
            return None
        reasoning = getattr(details, "reasoning_tokens", None)
        return reasoning or None

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
