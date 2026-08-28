from typing import Any, Dict, TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:

    def smss_stream(
        data: Any, stream_type: str = "content", interim: bool = True
    ) -> None: ...


import json
from openai import OpenAI, AzureOpenAI, omit
from openai.types import Batch, BatchRequestCounts
from openai.types.completion_usage import CompletionUsage
from openai.types.responses.response_usage import ResponseUsage
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
        self.client = self._get_client(is_azure, **client_kwargs)
        self.simplify_messages = string_to_bool(
            parent_kwargs.get("simplify_messages", False)
        )

        self.message_builder = OpenAIMessageBuilder(
            self.model_settings, self.chat_type, self.simplify_messages
        )
        self.image_client = OpenAiImageClient(client=self)
        self.audio_client = OpenAiAudioClient(client=self)

    def _get_client(self, is_azure: bool, **kwargs) -> Union[OpenAI, AzureOpenAI]:
        provider = (kwargs.pop("provider", None) or "").lower()
        if provider == "bedrock":
            return self._get_bedrock_client(**kwargs)
        api_key = kwargs.pop("api_key", None)
        if not api_key:
            raise ValueError("api_key is required for OpenAI and Azure OpenAI clients")
        if is_azure:
            endpoint = kwargs.pop("endpoint", None)
            if endpoint is None:
                endpoint = kwargs.pop("base_url", None)
            kwargs["azure_endpoint"] = endpoint
            return AzureOpenAI(api_key=api_key, **kwargs)
        return OpenAI(api_key=api_key, **kwargs)

    def _get_bedrock_client(self, **kwargs) -> OpenAI:
        from openai.providers import bedrock

        aws_access_key = kwargs.pop("aws_access_key", None) or kwargs.pop(
            "aws_access_key_id", None
        )
        aws_secret_key = kwargs.pop("aws_secret_key", None) or kwargs.pop(
            "aws_secret_access_key", None
        )
        aws_session_token = kwargs.pop("aws_session_token", None) or kwargs.pop(
            "session_token", None
        )
        aws_region = kwargs.pop("aws_region", None) or kwargs.pop("region", None)
        kwargs.pop("api_key", None)

        if not aws_region:
            import boto3

            aws_region = boto3.Session().region_name
        if not aws_region:
            raise ValueError(
                "provider='bedrock' requires a region "
                "(pass aws_region, set AWS_REGION, or run on EC2 with a region)"
            )

        base_url = kwargs.pop("base_url", None) or kwargs.pop("endpoint", None)
        provider_kwargs = {"base_url": base_url} if base_url else {}

        return OpenAI(
            provider=bedrock(
                region=aws_region,
                access_key_id=aws_access_key,
                secret_access_key=aws_secret_key,
                session_token=aws_session_token,
                **provider_kwargs,
            ),
            timeout=kwargs.pop("timeout", 300.0),
        )

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
            cache_creation_tokens = None
            thinking_tokens = None

            streamed_tools = {}
            streamed_server_tools = {}
            streamed_text_by_index: Dict[int, str] = {}
            citation_url_to_number: Dict[str, int] = {}
            next_citation_number = 1
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
                    cache_creation_tokens = self._extract_cache_write_tokens(
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
                            cache_creation_input_tokens=cache_creation_tokens,
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
                    idx = getattr(chunk, "output_index", 0)
                    if item and getattr(item, "type", None) == "message":
                        next_citation_number, message_text = (
                            self._extract_responses_message_text(
                                item,
                                citation_url_to_number,
                                next_citation_number,
                            )
                        )
                        if message_text:
                            streamed_text_by_index[idx] = message_text

                    elif item and getattr(item, "type", None) == "function_call":
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

                    elif item and self._is_responses_server_tool_call(
                        getattr(item, "type", None)
                    ):
                        server_tool_entry = self._build_responses_server_tool_entry(
                            item=item,
                            fallback_id=f"server_tool_{idx}",
                        )
                        if server_tool_entry is not None:
                            streamed_server_tools[idx] = server_tool_entry
                            tool_call = server_tool_entry["tool_call"]

                            if tool_call.get("id"):
                                data = StreamUtil.create_tool_id_chunk(
                                    idx, tool_call["id"]
                                )
                                smss_stream(data, stream_type="tool")
                                print(prefix + str(data), end="")

                            data = StreamUtil.create_tool_type_chunk(idx, "function")
                            smss_stream(data, stream_type="tool")
                            print(prefix + str(data), end="")

                            if tool_call.get("name"):
                                data = StreamUtil.create_function_name_chunk(
                                    idx, tool_call["name"]
                                )
                                smss_stream(data, stream_type="tool")
                                print(prefix + str(data), end="")

                            arguments_json = json.dumps(
                                tool_call.get("arguments") or {}, ensure_ascii=False
                            )
                            data = StreamUtil.create_function_arguments_chunk(
                                idx, arguments_json
                            )
                            smss_stream(data, stream_type="tool")
                            print(prefix + str(data), end="")

            if streamed_tools:
                data = StreamUtil.create_finish_reason_chunk(finish_reason)
                smss_stream(data, stream_type="tool", interim=False)
                tool_result = []
                tool_calls_by_index = {}
                for idx in sorted(streamed_tools.keys()):
                    tool_call = streamed_tools[idx]
                    try:
                        arguments = tool_call["arguments"]
                        # Return empty dict if arguments is empty string
                        if arguments == "":
                            arguments = {}
                        else:
                            arguments = json.loads(arguments)
                    except json.decoder.JSONDecodeError:
                        arguments = tool_call["arguments"]

                    parsed_tool_call = {
                        "id": tool_call["id"],
                        "type": tool_call["type"],
                        "name": tool_call["name"],
                        "arguments": arguments,
                    }
                    tool_result.append(parsed_tool_call)
                    tool_calls_by_index[idx] = parsed_tool_call

                ordered_parts = self._build_ordered_responses_parts(
                    text_by_index=streamed_text_by_index,
                    server_tool_entries_by_index=streamed_server_tools,
                    client_tool_calls_by_index=tool_calls_by_index,
                )
                final_content = self._join_responses_text_by_index(
                    text_by_index=streamed_text_by_index,
                    fallback_text=aggregated_content,
                )

                if not ordered_parts and final_content:
                    ordered_parts = [{"type": "TEXT", "text": final_content}]

                return AskModelEngineResponse2(
                    response=tool_result,
                    prompt_tokens=input_tokens,
                    response_tokens=response_tokens,
                    cache_read_tokens=cache_read_tokens,
                    cache_creation_tokens=cache_creation_tokens,
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
                            if aggregated_content and not streamed_text_by_index
                            else []
                        )
                        + ordered_parts
                    ),
                )
            else:
                data = StreamUtil.create_finish_reason_chunk(finish_reason)
                smss_stream(data, stream_type="content", interim=False)
                ordered_parts = self._build_ordered_responses_parts(
                    text_by_index=streamed_text_by_index,
                    server_tool_entries_by_index=streamed_server_tools,
                    client_tool_calls_by_index={},
                )
                final_content = self._join_responses_text_by_index(
                    text_by_index=streamed_text_by_index,
                    fallback_text=aggregated_content,
                )

                if not ordered_parts and final_content:
                    ordered_parts = [{"type": "TEXT", "text": final_content}]

                return AskModelEngineResponse2(
                    response=final_content,
                    response_tokens=response_tokens,
                    prompt_tokens=input_tokens,
                    cache_read_tokens=cache_read_tokens,
                    cache_creation_tokens=cache_creation_tokens,
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
                            if aggregated_content and not streamed_text_by_index
                            else []
                        )
                        + ordered_parts
                    ),
                )
        else:
            response_tokens = response.usage.output_tokens
            input_tokens = response.usage.input_tokens
            cache_read_tokens = self._extract_cached_tokens(
                response.usage, details_attr="input_tokens_details"
            )
            cache_creation_tokens = self._extract_cache_write_tokens(
                response.usage, details_attr="input_tokens_details"
            )
            thinking_tokens = self._extract_thinking_tokens(
                response.usage, details_attr="output_tokens_details"
            )
            output_items = getattr(response, "output", [])
            (
                final_content,
                tool_result,
                ordered_parts,
            ) = self._build_non_stream_responses_output_parts(output_items)

            if not final_content:
                final_content = response.output_text

            if not ordered_parts and final_content:
                ordered_parts = [{"type": "TEXT", "text": final_content}]

            reasoning = self._extract_reasoning_summary(response)
            if tool_result:
                return AskModelEngineResponse2(
                    response=tool_result,
                    prompt_tokens=input_tokens,
                    response_tokens=response_tokens,
                    cache_read_tokens=cache_read_tokens,
                    cache_creation_tokens=cache_creation_tokens,
                    thinking_tokens=thinking_tokens,
                    messageType="TOOL",
                    schemaVersion=2,
                    io="OUTPUT",
                    parts=(
                        (
                            [{"type": "THINKING", "thinking": reasoning}]
                            if reasoning
                            else []
                        )
                        + ordered_parts
                    ),
                )

            return AskModelEngineResponse2(
                response=final_content,
                response_tokens=response_tokens,
                prompt_tokens=input_tokens,
                cache_read_tokens=cache_read_tokens,
                cache_creation_tokens=cache_creation_tokens,
                thinking_tokens=thinking_tokens,
                schemaVersion=2,
                io="OUTPUT",
                parts=(
                    ([{"type": "THINKING", "thinking": reasoning}] if reasoning else [])
                    + ordered_parts
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
            cache_creation_tokens = None
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
                    cache_creation_tokens = self._extract_cache_write_tokens(
                        chunk.usage
                    )
                    thinking_tokens = self._extract_thinking_tokens(chunk.usage)

                    smss_stream(
                        StreamUtil.create_usage_chunk(
                            input_tokens=prompt_tokens,
                            output_tokens=response_tokens,
                            cache_read_input_tokens=cache_read_tokens,
                            cache_creation_input_tokens=cache_creation_tokens,
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
                    cache_creation_tokens=cache_creation_tokens,
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
                    cache_creation_tokens=cache_creation_tokens,
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
                cache_creation_tokens = self._extract_cache_write_tokens(response.usage)
                thinking_tokens = self._extract_thinking_tokens(response.usage)
            else:
                response_tokens = 0
                prompt_tokens = 0
                cache_read_tokens = None
                cache_creation_tokens = None
                thinking_tokens = None

            final_content = response.choices[0].message.content
            tool_calls = response.choices[0].message.tool_calls
            if tool_calls:
                return self._parse_tools_call_response(
                    response=response,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                    cache_read_tokens=cache_read_tokens,
                    cache_creation_tokens=cache_creation_tokens,
                    thinking_tokens=thinking_tokens,
                )
            else:
                reasoning = self._extract_reasoning_summary_chat(response)
                return AskModelEngineResponse2(
                    response=final_content,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                    cache_read_tokens=cache_read_tokens,
                    cache_creation_tokens=cache_creation_tokens,
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
        cache_creation_tokens: "int | None" = None,
        thinking_tokens: "int | None" = None,
        server_tool_parts: "list[dict[str, Any]] | None" = None,
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
                if getattr(tool_call, "type", None) != "function_call":
                    continue

                if isinstance(tool_call.arguments, str):
                    try:
                        arguments = json.loads(tool_call.arguments)
                    except json.decoder.JSONDecodeError:
                        arguments = tool_call.arguments
                else:
                    arguments = tool_call.arguments

                tools_result.append(
                    {
                        "id": tool_call.call_id,
                        "type": tool_call.type,
                        "name": tool_call.name,
                        "arguments": arguments,
                    }
                )

        preamble_text = None
        if self.chat_type == "chat-completion":
            preamble_text = response.choices[0].message.content
        elif self.chat_type == "responses":
            preamble_text = getattr(response, "output_text", None)

        text_parts = [{"type": "TEXT", "text": preamble_text}] if preamble_text else []
        if server_tool_parts is None:
            server_tool_parts = []

        return AskModelEngineResponse2(
            response=tools_result,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            cache_read_tokens=cache_read_tokens,
            cache_creation_tokens=cache_creation_tokens,
            thinking_tokens=thinking_tokens,
            messageType="TOOL",
            schemaVersion=2,
            io="OUTPUT",
            parts=text_parts
            + server_tool_parts
            + [{"type": "TOOL_CALL", "tool_call": t} for t in tools_result],
        )

    @staticmethod
    def _extract_openai_citation_url(annotation: Any) -> Optional[str]:
        if annotation is None:
            return None

        if isinstance(annotation, dict):
            nested = annotation.get("url_citation") or {}
            return annotation.get("url") or nested.get("url")

        nested = getattr(annotation, "url_citation", None)
        return getattr(annotation, "url", None) or getattr(nested, "url", None)

    @staticmethod
    def _extract_openai_citation_end_index(annotation: Any) -> Optional[int]:
        if annotation is None:
            return None

        if isinstance(annotation, dict):
            if isinstance(annotation.get("end_index"), int):
                return annotation.get("end_index")
            nested = annotation.get("url_citation") or {}
            if isinstance(nested.get("end_index"), int):
                return nested.get("end_index")
            return None

        end_index = getattr(annotation, "end_index", None)
        if isinstance(end_index, int):
            return end_index

        nested = getattr(annotation, "url_citation", None)
        nested_end_index = getattr(nested, "end_index", None)
        return nested_end_index if isinstance(nested_end_index, int) else None

    def _inject_openai_inline_citations(
        self,
        text: str,
        annotations: Any,
        url_to_number: Dict[str, int],
        next_number: int,
    ) -> tuple[int, str]:
        annotations = annotations or []
        if not annotations:
            return next_number, text

        inserts_by_pos: Dict[int, list[str]] = {}
        for annotation in annotations:
            url = self._extract_openai_citation_url(annotation)
            if not url:
                continue

            if url not in url_to_number:
                url_to_number[url] = next_number
                next_number += 1
            citation_number = url_to_number[url]

            pos = self._extract_openai_citation_end_index(annotation)
            if pos is None:
                pos = len(text)
            pos = max(0, min(len(text), pos))
            inserts_by_pos.setdefault(pos, []).append(
                f"<sup>[{citation_number}]({url})</sup>"
            )

        if not inserts_by_pos:
            return next_number, text

        for pos in sorted(inserts_by_pos.keys(), reverse=True):
            markers = inserts_by_pos[pos]
            marker_str = (
                markers[0] if len(markers) == 1 else "<sup>,</sup>".join(markers)
            )
            text = text[:pos] + marker_str + text[pos:]

        return next_number, text

    def _extract_responses_message_text(
        self,
        item: Any,
        url_to_number: Dict[str, int],
        next_number: int,
    ) -> tuple[int, str]:
        item_dict = self._output_item_to_dict(item)
        if item_dict.get("type") != "message":
            return next_number, ""

        message_parts = []
        for block in item_dict.get("content") or []:
            block_dict = self._output_item_to_dict(block)
            block_type = block_dict.get("type")
            if block_type not in {"output_text", "text"}:
                continue

            text = block_dict.get("text") or ""
            next_number, text = self._inject_openai_inline_citations(
                text=text,
                annotations=block_dict.get("annotations") or [],
                url_to_number=url_to_number,
                next_number=next_number,
            )
            message_parts.append(text)

        return next_number, "".join(message_parts)

    @staticmethod
    def _join_responses_text_by_index(
        text_by_index: Dict[int, str],
        fallback_text: str = "",
    ) -> str:
        if not text_by_index:
            return fallback_text or ""

        return "".join(text_by_index[idx] for idx in sorted(text_by_index.keys()))

    @staticmethod
    def _build_ordered_responses_parts(
        text_by_index: Dict[int, str],
        server_tool_entries_by_index: Dict[int, Dict[str, Dict[str, Any]]],
        client_tool_calls_by_index: Dict[int, Dict[str, Any]],
    ) -> list[Dict[str, Any]]:
        parts = []
        all_indexes = sorted(
            set(text_by_index.keys())
            .union(server_tool_entries_by_index.keys())
            .union(client_tool_calls_by_index.keys())
        )

        for output_index in all_indexes:
            text = text_by_index.get(output_index)
            if text:
                parts.append({"type": "TEXT", "text": text})

            server_tool_entry = server_tool_entries_by_index.get(output_index)
            if server_tool_entry is not None:
                parts.append(
                    {
                        "type": "TOOL_CALL",
                        "tool_call": server_tool_entry["tool_call"],
                    }
                )
                parts.append(
                    {
                        "type": "TOOL_RESULT",
                        "tool_result": server_tool_entry["tool_result"],
                    }
                )

            client_tool_call = client_tool_calls_by_index.get(output_index)
            if client_tool_call is not None:
                parts.append({"type": "TOOL_CALL", "tool_call": client_tool_call})

        return parts

    def _build_non_stream_responses_output_parts(
        self,
        output_items: Any,
    ) -> tuple[str, list[Dict[str, Any]], list[Dict[str, Any]]]:
        tools_result: list[Dict[str, Any]] = []
        text_by_index: Dict[int, str] = {}
        server_tool_entries_by_index: Dict[int, Dict[str, Dict[str, Any]]] = {}
        client_tool_calls_by_index: Dict[int, Dict[str, Any]] = {}

        citation_url_to_number: Dict[str, int] = {}
        next_citation_number = 1

        for idx, item in enumerate(output_items or []):
            item_type = getattr(item, "type", None)
            if item_type is None and isinstance(item, dict):
                item_type = item.get("type")

            if item_type == "message":
                next_citation_number, message_text = (
                    self._extract_responses_message_text(
                        item,
                        citation_url_to_number,
                        next_citation_number,
                    )
                )
                if message_text:
                    text_by_index[idx] = message_text
                continue

            if item_type == "function_call":
                item_dict = self._output_item_to_dict(item)
                arguments = self._parse_json_like_value(item_dict.get("arguments"))

                tool_call = {
                    "id": item_dict.get("call_id") or item_dict.get("id"),
                    "type": item_dict.get("type") or "function_call",
                    "name": item_dict.get("name"),
                    "arguments": arguments,
                }
                tools_result.append(tool_call)
                client_tool_calls_by_index[idx] = tool_call
                continue

            server_tool_entry = self._build_responses_server_tool_entry(
                item=item,
                fallback_id=f"server_tool_{idx}",
            )
            if server_tool_entry is not None:
                server_tool_entries_by_index[idx] = server_tool_entry

        ordered_parts = self._build_ordered_responses_parts(
            text_by_index=text_by_index,
            server_tool_entries_by_index=server_tool_entries_by_index,
            client_tool_calls_by_index=client_tool_calls_by_index,
        )
        final_text = self._join_responses_text_by_index(text_by_index=text_by_index)
        return final_text, tools_result, ordered_parts

    @staticmethod
    def _output_item_to_dict(item: Any) -> Dict[str, Any]:
        if isinstance(item, dict):
            return item

        if hasattr(item, "model_dump"):
            try:
                return item.model_dump(exclude_none=True)
            except TypeError:
                return item.model_dump()

        if hasattr(item, "__dict__"):
            return {
                key: value
                for key, value in vars(item).items()
                if not key.startswith("_")
            }

        return {}

    @staticmethod
    def _is_responses_server_tool_call(item_type: Optional[str]) -> bool:
        if not item_type:
            return False

        lowered_type = item_type.lower()
        if lowered_type in {"function_call", "message", "reasoning"}:
            return False

        return lowered_type.endswith("_call")

    @staticmethod
    def _normalize_server_tool_name(
        item_type: str, explicit_name: Optional[str]
    ) -> str:
        if explicit_name:
            return explicit_name

        if item_type.endswith("_call"):
            return item_type[: -len("_call")]

        return item_type

    @staticmethod
    def _parse_json_like_value(value: Any) -> Any:
        if value == "":
            return {}

        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.decoder.JSONDecodeError:
                return value

        return value

    def _extract_server_tool_arguments(self, item_dict: Dict[str, Any]) -> Any:
        item_type = item_dict.get("type")

        # For OpenAI web_search_call, keep TOOL_CALL arguments focused on the
        # action request payload and exclude provider-returned sources.
        # Sources are preserved in TOOL_RESULT output.
        if item_type == "web_search_call":
            action = item_dict.get("action")
            if isinstance(action, dict):
                action_type = action.get("type")
                sanitized_action = dict(action)
                sanitized_action.pop("sources", None)

                if action_type == "search":
                    sanitized_action = {
                        key: value
                        for key, value in sanitized_action.items()
                        if key in {"type", "query", "queries"} and value is not None
                    }
                elif action_type == "open_page":
                    sanitized_action = {
                        key: value
                        for key, value in sanitized_action.items()
                        if key in {"type", "url"} and value is not None
                    }
                elif action_type == "find_in_page":
                    sanitized_action = {
                        key: value
                        for key, value in sanitized_action.items()
                        if key in {"type", "url", "pattern"} and value is not None
                    }

                return {"action": sanitized_action}

        if "arguments" in item_dict:
            return self._parse_json_like_value(item_dict.get("arguments"))

        if "input" in item_dict:
            return self._parse_json_like_value(item_dict.get("input"))

        if item_dict.get("query") is not None:
            return {"query": item_dict.get("query")}

        excluded_fields = {
            "id",
            "call_id",
            "type",
            "name",
            "status",
            "results",
            "result",
            "content",
        }
        remaining_fields = {
            key: value
            for key, value in item_dict.items()
            if key not in excluded_fields and value is not None
        }
        return remaining_fields or {}

    @staticmethod
    def _extract_server_tool_output_payload(item_dict: Dict[str, Any]) -> Any:
        if item_dict.get("result") is not None:
            return item_dict.get("result")

        if item_dict.get("results") is not None:
            return item_dict.get("results")

        if item_dict.get("content") is not None:
            return item_dict.get("content")

        return item_dict

    def _build_responses_server_tool_entry(
        self,
        item: Any,
        fallback_id: Optional[str] = None,
    ) -> Optional[Dict[str, Dict[str, Any]]]:
        item_type = getattr(item, "type", None)
        if item_type is None and isinstance(item, dict):
            item_type = item.get("type")

        if not self._is_responses_server_tool_call(item_type):
            return None

        item_dict = self._output_item_to_dict(item)
        tool_call_id = (
            item_dict.get("id")
            or item_dict.get("call_id")
            or fallback_id
            or f"server_tool_{item_type}"
        )
        tool_name = self._normalize_server_tool_name(
            item_type=item_type,
            explicit_name=item_dict.get("name"),
        )

        tool_call = {
            "id": tool_call_id,
            "type": "function",
            "name": tool_name,
            "arguments": self._extract_server_tool_arguments(item_dict),
            "server_tool": True,
        }

        output_payload = self._extract_server_tool_output_payload(item_dict)
        try:
            output_json = json.dumps(output_payload, ensure_ascii=False)
        except TypeError:
            output_json = str(output_payload)

        tool_result = {
            "id": tool_call_id,
            "tool_name": tool_name,
            "server_tool": True,
            "output": output_json,
        }
        return {
            "tool_call": tool_call,
            "tool_result": tool_result,
        }

    def _build_responses_server_tool_parts(
        self, output_items: Any
    ) -> list[Dict[str, Any]]:
        parts: list[Dict[str, Any]] = []
        for idx, item in enumerate(output_items or []):
            entry = self._build_responses_server_tool_entry(
                item=item,
                fallback_id=f"server_tool_{idx}",
            )
            if entry is None:
                continue

            parts.append({"type": "TOOL_CALL", "tool_call": entry["tool_call"]})
            parts.append({"type": "TOOL_RESULT", "tool_result": entry["tool_result"]})

        return parts

    @staticmethod
    def _extract_cached_tokens(
        usage: "ResponseUsage | CompletionUsage | None",
        details_attr: str = "prompt_tokens_details",
    ) -> "int | None":
        if usage is None:
            return None
        details = getattr(usage, details_attr, None)
        if details is None:
            return None
        return getattr(details, "cached_tokens", None)

    @staticmethod
    def _extract_cache_write_tokens(
        usage: "ResponseUsage | CompletionUsage | None",
        details_attr: str = "prompt_tokens_details",
    ) -> "int | None":
        if usage is None:
            return None
        details = getattr(usage, details_attr, None)
        if details is None:
            return None
        return getattr(details, "cache_write_tokens", None)

    @staticmethod
    def _extract_thinking_tokens(
        usage: "ResponseUsage | CompletionUsage | None",
        details_attr: str = "completion_tokens_details",
    ) -> "int | None":
        if usage is None:
            return None
        details = getattr(usage, details_attr, None)
        if details is None:
            return None
        return getattr(details, "reasoning_tokens", None)

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

    # ------------------------------------------------------------------
    # Batch API (OpenAI / Azure OpenAI native Batch)
    #
    # Lifecycle: submit -> provider batch id -> poll status -> fetch results.
    # All methods return plain JSON-serializable dicts so they marshal cleanly
    # back to the Java engine over the TCP PayloadStruct protocol.
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_batch_status(status: str | None) -> str:
        s = (status or "").lower()
        mapping = {
            "validating": "VALIDATING",
            "in_progress": "IN_PROGRESS",
            "finalizing": "FINALIZING",
            "completed": "COMPLETED",
            "failed": "FAILED",
            "expired": "EXPIRED",
            "cancelling": "CANCELING",
            "cancelled": "CANCELED",
            "canceled": "CANCELED",
        }
        return mapping.get(s, s.upper() or "UNKNOWN")

    def _normalize_request_for_batch(
        self, req, idx: int, endpoint: str = "/v1/chat/completions"
    ):
        """Convert simplified {command, context} format to the correct wire format for endpoint."""
        if not isinstance(req, dict):
            return req

        if req.get("message_json"):
            return self._build_batch_body_from_history(req, idx)
        if "command" not in req:
            return req
        custom_id = req.get("custom_id") or f"req-{idx}"
        skip = {"command", "context", "custom_id"}
        extra = {k: v for k, v in req.items() if k not in skip}
        if endpoint == "/v1/responses":
            body = {"input": [{"role": "user", "content": req["command"]}]}
            if req.get("context"):
                body["instructions"] = req["context"]
            # Responses API uses max_output_tokens, not max_completion_tokens
            if "max_completion_tokens" in extra:
                extra = dict(extra)
                extra["max_output_tokens"] = extra.pop("max_completion_tokens")
        else:
            messages = []
            if req.get("context"):
                messages.append({"role": "system", "content": req["context"]})
            messages.append({"role": "user", "content": req["command"]})
            body = {"messages": messages}
        body.update(extra)
        return {"custom_id": custom_id, "body": body}

    def _build_batch_body_from_history(self, req: dict, idx: int):
        """Build a per-request batch body from a full SEMOSS message_json + tools,
        reusing the same message builder the synchronous ask path uses."""
        custom_id = req.get("custom_id") or f"req-{idx}"
        skip = {"command", "context", "custom_id", "message_json"}
        kwargs = {k: v for k, v in req.items() if k not in skip}
        message_json = req.get("message_json")
        if not message_json:
            raise ValueError(
                f"Request {custom_id} is missing 'message_json' for batch processing"
            )
        semoss_messages = self.build_semoss_messages(
            model_settings=self.model_settings,
            message_json=message_json,
            **kwargs,
        )
        body = self.message_builder.build_request(semoss_messages)
        body.pop("stream", None)  # no streaming on batch requests
        return {"custom_id": custom_id, "body": body}

    def _chat_completion_to_content_blocks(self, body: dict):
        """Normalize a chat completion response body to SEMOSS content blocks."""
        if not isinstance(body, dict):
            return None
        msg = (body.get("choices") or [{}])[0].get("message") or {}
        blocks = []
        text = msg.get("content")
        if text:
            blocks.append({"type": "text", "text": text})
        for tc in msg.get("tool_calls") or []:
            func = tc.get("function") or {}
            try:
                input_data = json.loads(func.get("arguments") or "{}")
            except Exception:
                input_data = {"raw": func.get("arguments")}
            blocks.append(
                {
                    "type": "tool_use",
                    "id": tc.get("id"),
                    "name": func.get("name"),
                    "input": input_data,
                }
            )
        return {"role": "assistant", "content": blocks} if blocks else None

    def _responses_api_to_content_blocks(self, body: dict):
        """Normalize a Responses API response body to SEMOSS content blocks."""
        if not isinstance(body, dict):
            return None
        blocks = []
        for item in body.get("output") or []:
            itype = item.get("type")
            if itype == "message":
                for block in item.get("content") or []:
                    btype = block.get("type")
                    if btype in ("output_text", "text"):
                        blocks.append({"type": "text", "text": block.get("text")})
                    elif btype == "refusal":
                        blocks.append({"type": "text", "text": block.get("refusal")})
            elif itype == "function_call":
                try:
                    input_data = json.loads(item.get("arguments") or "{}")
                except Exception:
                    input_data = {"raw": item.get("arguments")}
                blocks.append(
                    {
                        "type": "tool_use",
                        "id": item.get("call_id"),
                        "name": item.get("name"),
                        "input": input_data,
                    }
                )
            elif itype == "reasoning":
                for block in item.get("summary") or []:
                    if block.get("type") == "summary_text":
                        blocks.append(
                            {"type": "thinking", "thinking": block.get("text", "")}
                        )
        return {"role": "assistant", "content": blocks} if blocks else None

    def submit_batch(
        self,
        requests,
        endpoint: Optional[str] = None,
        **kwargs,
    ):
        # completion_window and metadata are read from kwargs since not all
        # providers honor them; OpenAI only allows a 24h completion window.
        metadata = kwargs.get("metadata")
        import io

        if endpoint is None:
            endpoint = (
                "/v1/responses"
                if self.chat_type == "responses"
                else "/v1/chat/completions"
            )
        if isinstance(requests, str):
            requests = json.loads(requests)
        requests = [
            self._normalize_request_for_batch(r, i, endpoint)
            for i, r in enumerate(requests or [])
        ]

        model = self.model_settings.model_name
        lines = []
        for req in requests or []:
            custom_id = str(req.get("custom_id"))
            body = dict(req.get("body") or {})
            body.setdefault("model", model)
            lines.append(
                json.dumps(
                    {
                        "custom_id": custom_id,
                        "method": "POST",
                        "url": endpoint,
                        "body": body,
                    },
                    ensure_ascii=False,
                )
            )
        if not lines:
            raise ValueError("submit_batch requires at least one request")

        jsonl = "\n".join(lines)
        file_obj = io.BytesIO(jsonl.encode("utf-8"))
        file_obj.name = "batch_input.jsonl"
        uploaded = self.client.files.create(file=file_obj, purpose="batch")
        completion_window = "24h"  # completion window must be '24h' right now

        batch = self.client.batches.create(
            input_file_id=uploaded.id,
            endpoint=endpoint,
            completion_window=completion_window,
            metadata=metadata if metadata else None,
        )
        return {
            "provider_batch_id": batch.id,
            "status": self._normalize_batch_status(batch.status),
            "request_count": len(lines),
            "endpoint": endpoint,
            "input_file_id": uploaded.id,
            "raw": batch.model_dump(),
        }

    def get_batch_status(self, provider_batch_id: str, **kwargs):
        batch: Batch = self.client.batches.retrieve(provider_batch_id)
        rc: BatchRequestCounts | None = batch.request_counts
        counts = {}
        if rc is not None:
            counts = {
                "total": rc.total,
                "completed": rc.completed,
                "failed": rc.failed,
            }
        return {
            "provider_batch_id": batch.id,
            "status": self._normalize_batch_status(batch.status),
            "counts": counts,
            "output_ref": batch.output_file_id,
            "error_ref": batch.error_file_id,
            "raw": batch.model_dump(),
        }

    def get_batch_results(self, provider_batch_id: str, **kwargs):
        batch: Batch = self.client.batches.retrieve(provider_batch_id)
        batch_endpoint = batch.endpoint
        output_file_id = batch.output_file_id
        error_file_id = batch.error_file_id
        items = []
        raw_lines = []

        def _consume(file_id: Optional[str]):
            if not file_id:
                return
            content = self.client.files.content(file_id)
            if hasattr(content, "text"):
                text = content.text
            else:
                text = content.read().decode("utf-8")
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                raw_lines.append(line)
                obj = json.loads(line)
                resp = obj.get("response") or {}
                err = obj.get("error")
                status_code = (
                    resp.get("status_code") if isinstance(resp, dict) else None
                )
                body = resp.get("body") if isinstance(resp, dict) else None
                usage = body.get("usage") if isinstance(body, dict) else None
                ok = err is None and (status_code is None or 200 <= status_code < 300)
                if not ok and err is None and isinstance(body, dict):
                    err = body.get("error") or body
                if ok:
                    if batch_endpoint == "/v1/responses":
                        message = self._responses_api_to_content_blocks(body)
                        input_tokens = (usage or {}).get("input_tokens")
                        output_tokens = (usage or {}).get("output_tokens")
                    else:
                        message = self._chat_completion_to_content_blocks(body)
                        input_tokens = (usage or {}).get("prompt_tokens")
                        output_tokens = (usage or {}).get("completion_tokens")
                else:
                    message = None
                    input_tokens = None
                    output_tokens = None
                items.append(
                    {
                        "custom_id": obj.get("custom_id"),
                        "ok": ok,
                        "status": "succeeded" if err is None else "errored",
                        "message": message,
                        "error": err,
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                    }
                )

        _consume(output_file_id)
        _consume(error_file_id)
        return {
            "provider_batch_id": provider_batch_id,
            "status": self._normalize_batch_status(batch.status),
            "count": len(items),
            "results": items,
            "raw_jsonl": "\n".join(raw_lines),
        }

    def list_batches(self, limit: int = 20, after: str | None = None, **kwargs):
        if not after:
            after = kwargs.get("after")
        resp = self.client.batches.list(
            limit=limit,
            after=after if after is not None else omit,
        )
        batches = [
            {
                "provider_batch_id": b.id,
                "status": self._normalize_batch_status(b.status),
                "request_count": (
                    b.request_counts.total if b.request_counts is not None else None
                ),
                "created_at": b.created_at,
            }
            for b in resp.data
        ]
        return {
            "batches": batches,
            "has_more": resp.has_more,
            "next_cursor": resp.data[-1].id if resp.data and resp.has_more else None,
        }

    def cancel_batch(self, provider_batch_id: str, **kwargs):
        batch: Batch = self.client.batches.cancel(provider_batch_id)
        return {
            "provider_batch_id": batch.id,
            "status": self._normalize_batch_status(batch.status),
            "raw": batch.model_dump(),
        }
