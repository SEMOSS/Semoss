from typing import Optional, Dict, Any, Union, TYPE_CHECKING, List
import json

if TYPE_CHECKING:
    # injected into globals in handle_python of gaas_tcp_server_handler.py
    def smss_stream(
        data: Any, stream_type: str = "content", interim: bool = True
    ) -> None: ...


from smss_thread_local import get_smss_stream
from pydantic import BaseModel
from ...clients.google_clients import (
    GoogleClient,
    GoogleClientConfig,
    GoogleClientType,
)
from ...message_builders.anthropic.anthropic_models import AnthropicRequestConfig
from ...constants import (
    AskModelEngineResponse2,
    TEMPLATE,
    TEMPLATE_NAME,
)
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...message_builders.anthropic.anthropic_message_builder import (
    AnthropicMessageBuilder,
)
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil
from anthropic import Anthropic, AnthropicBedrock, AnthropicFoundry
from ..model_engine_exception import (
    ModelEngineException,
    AnthropicRefusalError,
)
from ...utils import string_to_bool


class ToolCall(BaseModel):
    name: str
    description: Optional[str] = None
    input_schema: Optional[Dict[str, Any]] = None


class Usage(BaseModel):
    input_tokens: int
    output_tokens: int


class AnthropicTextClient(AbstractTextGenerationClient):

    def __init__(
        self,
        provider: str,
        use_beta_header: Optional[Union[str, bool]] = False,
        prompt_caching: Optional[Union[str, bool]] = True,
        **kwargs,
    ):
        super().__init__(
            template=kwargs.pop(TEMPLATE, None),
            template_name=kwargs.pop(TEMPLATE_NAME, None),
            **kwargs,
        )

        self.provider = provider.lower()
        self.use_beta_header = (
            use_beta_header.lower() in ["true", "1", "yes", "on"]
            if isinstance(use_beta_header, str)
            else use_beta_header
        )
        self.prompt_caching = (
            prompt_caching.lower() in ["true", "1", "yes", "on"]
            if isinstance(prompt_caching, str)
            else prompt_caching
        )
        self.beta_feature_name = kwargs.pop("beta_feature_name", None)
        if self.use_beta_header and not self.beta_feature_name:
            raise ValueError(
                "beta_feature_name is required when use_beta_header is enabled."
            )

        self.client = self._get_client(**kwargs)
        self.thinking_signature = None

    def _get_client(self, **kwargs):
        if self.provider == "google":
            self.client_config = GoogleClientConfig(
                type=GoogleClientType.ANTHROPIC,
                service_account_credentials=kwargs.pop(
                    "service_account_credentials", None
                ),
                service_account_key_file=kwargs.pop("service_account_key_file", None),
                region=kwargs.pop("region", None),
                project=kwargs.pop("project", None),
                api_key=kwargs.pop("api_key", None),
            )
            return GoogleClient(config=self.client_config).client
        elif self.provider == "bedrock":
            return AnthropicBedrock(
                aws_region=kwargs.pop("aws_region", None),
                aws_access_key=kwargs.pop("aws_access_key", None),
                aws_secret_key=kwargs.pop("aws_secret_key", None),
            )
        elif self.provider == "azure":
            return AnthropicFoundry(
                base_url=kwargs.pop("endpoint", None),
                api_key=kwargs.pop("api_key", None),
            )
        elif self.provider == "anthropic":
            return Anthropic(
                api_key=kwargs.pop("api_key", None),
            )
        else:
            raise ValueError(
                f"Provider '{self.provider}' is not supported for Anthropic Text Client."
            )

    @staticmethod
    def _apply_cache_to_tools(request_config: "AnthropicRequestConfig") -> None:
        """
        Add cache_control to the last tool definition. Tools are evaluated
        first in Anthropic's cache breakpoint order (tools → system → messages),
        so caching them saves tokens whenever the tool list is large and static.
        """
        tools = request_config.tools
        if not tools:
            return
        tools[-1]["cache_control"] = {"type": "ephemeral"}

    @staticmethod
    def _apply_cache_to_system(request_config: "AnthropicRequestConfig") -> None:
        """
        Convert the system prompt to list form and attach cache_control to its
        last text block. This caches the system prompt on the first call so
        subsequent turns pay only the cache-read price for those tokens.

        Only called for providers (Bedrock, Vertex) that don't support the
        top-level automatic cache_control field.
        """
        system = request_config.system
        if not system:
            return
        if isinstance(system, str):
            request_config.system = [
                {"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}
            ]
        elif isinstance(system, list):
            # Already a list — attach to the last text block.
            for block in reversed(system):
                if isinstance(block, dict) and block.get("type") == "text":
                    block["cache_control"] = {"type": "ephemeral"}
                    break

    # Block types that Anthropic supports cache_control on.
    _CACHEABLE_BLOCK_TYPES = {"text", "tool_result", "image", "document"}

    @staticmethod
    def _apply_cache_to_last_block(messages: List[Dict[str, Any]]) -> None:
        """
        Add cache_control to the last cacheable block of the last message. This
        replicates Anthropic's automatic caching behaviour for providers
        (Bedrock, Vertex) that only support block-level cache_control.

        On each turn the last message is the newest one, so the marker
        naturally moves forward through the conversation as history grows.

        Supports text, tool_result, image, and document blocks — not just text —
        so that tool execution turns (whose last message contains tool_result
        blocks) are also cached correctly.
        """
        if not messages:
            return
        last_msg = messages[-1]
        content = last_msg.get("content")
        if isinstance(content, str):
            last_msg["content"] = [
                {
                    "type": "text",
                    "text": content,
                    "cache_control": {"type": "ephemeral"},
                }
            ]
        elif isinstance(content, list):
            for block in reversed(content):
                if (
                    isinstance(block, dict)
                    and block.get("type") in AnthropicTextClient._CACHEABLE_BLOCK_TYPES
                ):
                    block["cache_control"] = {"type": "ephemeral"}
                    break

    def ask_call(
        self,
        prefix="",
        **kwargs,
    ):
        if self.client is None:
            raise ValueError("Anthropic client is not initialized.")
        try:
            if (
                hasattr(self.model_settings, "global_param_override")
                and self.model_settings.global_param_override
            ):
                kwargs.update(self.model_settings.global_param_override)

            built_in_tools = kwargs.get("built_in_tools", []) or []
            web_search_enabled = any(
                isinstance(tool, str) and tool.lower() == "web_search"
                for tool in built_in_tools
            )
            inline_citations = kwargs.get("inline_citations", None)
            if inline_citations is None:
                inline_citations_enabled = True
            else:
                try:
                    inline_citations_enabled = string_to_bool(inline_citations)
                except ValueError:
                    inline_citations_enabled = True

            semoss_messages = self.build_semoss_messages(
                model_settings=self.model_settings, **kwargs
            )

            try:
                msg_builder_response = AnthropicMessageBuilder().build_messages(
                    semoss_messages,
                    self.model_settings,
                    self.model_limits,
                    self.model_name,
                    self.use_beta_header,
                    self.beta_feature_name,
                    thinking_signature=self.thinking_signature,
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to build messages in Anthropic format from SEMOSS format: {e}"
                )

            request_config = msg_builder_response.request_config
            streaming = msg_builder_response.streaming
            self.has_schema = msg_builder_response.has_structured_input

            if self.prompt_caching:
                if self.provider in ("anthropic", "azure"):
                    request_config.cache_control = {"type": "ephemeral"}
                elif self.provider in ("bedrock", "google"):
                    self._apply_cache_to_tools(request_config)
                    self._apply_cache_to_system(request_config)
                    self._apply_cache_to_last_block(request_config.messages)

            if streaming:
                return self._handle_streaming(
                    request_config,
                    prefix=prefix,
                    web_search_enabled=web_search_enabled,
                    inline_citations_enabled=inline_citations_enabled,
                )

            if self.use_beta_header:
                response = self.client.beta.messages.create(
                    **request_config.model_dump(exclude_none=True),
                )
            else:
                response = self.client.messages.create(
                    **request_config.model_dump(exclude_none=True),
                )

            if response.stop_reason == "refusal":
                raise AnthropicRefusalError(
                    "The model refused to complete the request."
                )

            if response.stop_reason == "tool_use":
                _cache_read = (
                    getattr(response.usage, "cache_read_input_tokens", None) or 0
                )
                _cache_creation = (
                    getattr(response.usage, "cache_creation_input_tokens", None) or 0
                )
                return self._parse_tools_call_response(
                    response,
                    prompt_tokens=response.usage.input_tokens
                    + _cache_read
                    + _cache_creation,
                    response_tokens=response.usage.output_tokens,
                    cache_read_tokens=_cache_read,
                    cache_creation_tokens=_cache_creation,
                )

            thinking_text = ""
            response_text = ""
            for content in response.content:
                if hasattr(content, "type") and content.type == "thinking":
                    thinking_text += content.thinking
                elif hasattr(content, "type") and content.type == "text":
                    response_text += content.text

            if web_search_enabled and inline_citations_enabled:
                response_text = self._add_inline_citations(response) or response_text

            usage = Usage(
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
            )
            cache_read_tokens = (
                getattr(response.usage, "cache_read_input_tokens", None) or None
            )
            cache_creation_tokens = (
                getattr(response.usage, "cache_creation_input_tokens", None) or None
            )
            # Normalize Anthropic input_tokens (new-only) to total billed, matching OpenAI/Gemini
            total_input_tokens = (
                usage.input_tokens
                + (cache_read_tokens or 0)
                + (cache_creation_tokens or 0)
            )

            if self.prompt_caching and (cache_read_tokens or cache_creation_tokens):
                print(
                    f"[prompt_caching] cache_read_tokens={cache_read_tokens} "
                    f"cache_creation_tokens={cache_creation_tokens}",
                    flush=True,
                )

            parts = []
            if response_text:
                parts.append({"type": "TEXT", "text": response_text})
            if thinking_text:
                parts.append({"type": "THINKING", "thinking": thinking_text})

            return AskModelEngineResponse2(
                response=response_text,
                response_tokens=usage.output_tokens,
                prompt_tokens=total_input_tokens,
                cache_read_tokens=cache_read_tokens,
                cache_creation_tokens=cache_creation_tokens,
                schemaVersion=2,
                io="OUTPUT",
                parts=parts,
                messageType="CHAT",
            )
        except Exception as e:
            return ModelEngineException(
                error=e, client="anthropic", model=self.model_name
            ).parse_error()

    def _parse_tools_call_response(
        self,
        response,
        prompt_tokens: int = 0,
        response_tokens: int = 0,
        cache_read_tokens: Optional[int] = None,
        cache_creation_tokens: Optional[int] = None,
    ) -> AskModelEngineResponse2:
        tools_result = []
        for content in response.content:
            if content.type == "tool_use":
                tool_use = {
                    "id": content.id,
                    "name": content.name,
                    "arguments": content.input,
                    "type": "function",
                }
                tools_result.append(tool_use)

        if self.has_schema:
            is_schema, json_str = self._flatten_schema_tool(tools_result, "return_json")
            if is_schema:
                parts = [{"type": "TEXT", "text": json_str}] if json_str else []
                return AskModelEngineResponse2(
                    response=json_str,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                    cache_read_tokens=cache_read_tokens,
                    cache_creation_tokens=cache_creation_tokens,
                    schemaVersion=2,
                    io="OUTPUT",
                    parts=parts,
                    messageType="CHAT",
                )

        return AskModelEngineResponse2(
            response=tools_result,
            response_tokens=response_tokens,
            prompt_tokens=prompt_tokens,
            cache_read_tokens=cache_read_tokens,
            cache_creation_tokens=cache_creation_tokens,
            schemaVersion=2,
            io="OUTPUT",
            parts=[{"type": "TOOL_CALL", "tool_call": t} for t in tools_result],
            messageType="TOOL",
        )

    def _handle_streaming(
        self,
        request_config: AnthropicRequestConfig,
        prefix: str = "",
        web_search_enabled: bool = False,
        inline_citations_enabled: bool = True,
    ) -> AskModelEngineResponse2:
        smss_stream = get_smss_stream()

        input_tokens = 0
        output_tokens = 0
        cache_read_tokens: Optional[int] = None
        cache_creation_tokens: Optional[int] = None
        stop_reason: Optional[str] = None

        content_array = []
        this_content_block: Dict[str, Any] = {}
        this_content_block_type = ""

        tool_result = []
        # Maps server-tool_use id -> the underlying tool name (e.g. "web_search").
        # Populated when a server_tool_use block closes, read when its result block
        # arrives so the persisted TOOL_RESULT carries the real tool name instead
        # of an Anthropic-specific block-type string.
        server_tool_use_names: Dict[str, str] = {}

        use_beta_stream = self.use_beta_header and hasattr(
            self.client.beta.messages, "stream"
        )
        stream_method = (
            self.client.beta.messages.stream
            if use_beta_stream
            else self.client.messages.stream
        )

        stream_kwargs = request_config.model_dump(exclude_none=True)
        if self.use_beta_header and not use_beta_stream:
            # Bedrock: beta.messages has no .stream; pass beta via extra_headers so
            # the Bedrock SDK converts anthropic-beta header → anthropic_beta body field
            stream_kwargs.pop("betas", None)
            stream_kwargs["extra_headers"] = {"anthropic-beta": self.beta_feature_name}

        with stream_method(**stream_kwargs) as stream:
            final_message = None
            for event in stream:
                if event.type == "message_start":
                    input_tokens = event.message.usage.input_tokens
                    cache_read_tokens = (
                        getattr(event.message.usage, "cache_read_input_tokens", None)
                        or None
                    )
                    cache_creation_tokens = (
                        getattr(
                            event.message.usage, "cache_creation_input_tokens", None
                        )
                        or None
                    )
                    # Normalize to total input billed (see non-streaming path).
                    input_tokens = (
                        input_tokens
                        + (cache_read_tokens or 0)
                        + (cache_creation_tokens or 0)
                    )

                    smss_stream(
                        StreamUtil.create_usage_chunk(
                            input_tokens=input_tokens,
                            cache_read_input_tokens=cache_read_tokens,
                            cache_creation_input_tokens=cache_creation_tokens,
                        ),
                        stream_type="usage",
                    )

                elif event.type == "content_block_start":
                    this_content_block_type = event.content_block.type
                    this_content_block["type"] = this_content_block_type

                    if this_content_block_type == "text":
                        text_chunk = event.content_block.text
                        this_content_block["final_response"] = text_chunk

                        data = StreamUtil.create_content_chunk(text_chunk)
                        smss_stream(data, stream_type="content")
                        print(prefix + text_chunk, end="", flush=True)

                    elif this_content_block_type == "thinking":
                        text_chunk = event.content_block.thinking
                        this_content_block["final_response"] = text_chunk

                        data = StreamUtil.create_thinking_chunk(text_chunk)
                        smss_stream(data, stream_type="thinking")
                        print(prefix + text_chunk, end="", flush=True)

                    elif (
                        this_content_block_type == "tool_use"
                        or this_content_block_type == "server_tool_use"
                    ):
                        this_content_block.update(
                            {
                                "id": None,
                                "type": "function",
                                "function": {"name": None, "arguments": ""},
                                "server_tool": (
                                    this_content_block_type == "server_tool_use"
                                ),
                            }
                        )
                        this_content_block["id"] = event.content_block.id
                        this_content_block["function"][
                            "name"
                        ] = event.content_block.name

                        data = StreamUtil.create_tool_id_chunk(
                            index=len(tool_result), tool_id=event.content_block.id
                        )
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                        data = StreamUtil.create_tool_type_chunk(index=len(tool_result))
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                        data = StreamUtil.create_function_name_chunk(
                            index=len(tool_result),
                            function_name=event.content_block.name,
                        )
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                    elif this_content_block_type == "web_search_tool_result":
                        tool_use_id = event.content_block.tool_use_id
                        this_content_block.update(
                            {
                                "tool_use_id": tool_use_id,
                                "type": "tool_result",
                                "content": [],
                                # Resolve to the underlying tool name (e.g. "web_search")
                                # captured when the matching server_tool_use block closed.
                                "name": server_tool_use_names.get(
                                    tool_use_id, "web_search"
                                ),
                                "server_tool": True,
                            }
                        )
                        for item in event.content_block.content:
                            this_content_block["content"].append(
                                {
                                    "type": item.type,
                                    "url": item.url,
                                    "title": item.title,
                                    "encrypted_content": item.encrypted_content,
                                    "page_age": item.page_age,
                                }
                            )

                elif event.type == "content_block_delta":
                    if this_content_block_type == "text":
                        if hasattr(event.delta, "text"):
                            text_chunk = event.delta.text
                            this_content_block["final_response"] += text_chunk

                            data = StreamUtil.create_content_chunk(text_chunk)
                            smss_stream(data, stream_type="content")
                            print(prefix + text_chunk, end="", flush=True)

                    elif this_content_block_type == "thinking":
                        if event.delta.type == "signature_delta":
                            this_content_block["signature"] = event.delta.signature
                            continue

                        text_chunk = event.delta.thinking
                        this_content_block["final_response"] += text_chunk

                        data = StreamUtil.create_thinking_chunk(text_chunk)
                        smss_stream(data, stream_type="thinking")
                        print(prefix + text_chunk, end="", flush=True)

                    elif (
                        this_content_block_type == "tool_use"
                        or this_content_block_type == "server_tool_use"
                    ):
                        this_content_block["function"][
                            "arguments"
                        ] += event.delta.partial_json

                        data = StreamUtil.create_function_arguments_chunk(
                            index=len(tool_result),
                            arguments_chunk=event.delta.partial_json,
                        )
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                elif event.type == "content_block_stop":
                    if (
                        this_content_block_type == "tool_use"
                        or this_content_block_type == "server_tool_use"
                    ):
                        try:
                            arguments = json.loads(
                                this_content_block["function"]["arguments"]
                            )
                        except json.decoder.JSONDecodeError:
                            arguments = this_content_block["function"]["arguments"]

                        if this_content_block["server_tool"]:
                            # Remember the real tool name so the paired result block
                            # can replay with `name="web_search"` rather than the
                            # Anthropic block-type string.
                            server_tool_use_names[this_content_block["id"]] = (
                                this_content_block["function"]["name"]
                            )
                        else:
                            tool_result.append(
                                {
                                    "id": this_content_block["id"],
                                    "type": this_content_block["type"],
                                    "name": this_content_block["function"]["name"],
                                    "arguments": arguments,
                                }
                            )

                    elif this_content_block_type == "text":
                        if event.content_block.citations:
                            this_content_block["citations"] = []
                            for item in event.content_block.citations:
                                this_content_block["citations"].append(
                                    {
                                        "type": item.type,
                                        "url": item.url,
                                        "title": item.title,
                                        "encrypted_index": item.encrypted_index,
                                        "cited_text": item.cited_text,
                                    }
                                )

                    content_array.append(this_content_block)
                    this_content_block = {}
                    this_content_block_type = ""

                elif event.type == "message_delta":
                    output_tokens = event.usage.output_tokens
                    if getattr(event, "delta", None) and getattr(
                        event.delta, "stop_reason", None
                    ):
                        stop_reason = event.delta.stop_reason

                    smss_stream(
                        StreamUtil.create_usage_chunk(output_tokens=output_tokens),
                        stream_type="usage",
                    )

            if stop_reason is None:
                try:
                    final_message = stream.get_final_message()
                    stop_reason = final_message.stop_reason
                except Exception:
                    stop_reason = None
                    final_message = None
            else:
                try:
                    final_message = stream.get_final_message()
                except Exception:
                    final_message = None

        if stop_reason == "refusal":
            data = StreamUtil.create_finish_reason_chunk("refusal")
            smss_stream(data, stream_type="content", interim=False)
            raise AnthropicRefusalError("The model refused to complete the request.")

        if tool_result:
            data = StreamUtil.create_finish_reason_chunk("tool_use")
            smss_stream(data, stream_type="tool", interim=False)
        else:
            data = StreamUtil.create_finish_reason_chunk("stop")
            smss_stream(data, stream_type="content", interim=False)

        citation_index = 1  # start numbering at 1
        final_response = ""
        thinking_response = ""
        thinking_signature = ""
        for content in content_array:
            if content.get("final_response", None):
                if content.get("type", None) == "thinking":
                    thinking_response += content.get("final_response")
                    if content.get("signature"):
                        thinking_signature = content.get("signature")
                else:
                    final_response += content.get("final_response")
                    # if there are citations, we will append <sup>[{number}]({url})</sup>
                    # at the end of each final_response
                    for item in content.get("citations", []):
                        url = item.get("url", None)
                        if url:
                            final_response += f"<sup>[{citation_index}]({url})</sup>"
                            citation_index += 1

        if thinking_signature and self.thinking_signature is None:
            self.thinking_signature = thinking_signature

        citation_index = 1  # start numbering at 1
        parts = []
        current_text_block = None  # Track consecutive text blocks to merge them
        for content in content_array:
            content_type = content.get("type")

            # flush accumulated text if we hit a non-text block
            if content_type != "text" and current_text_block is not None:
                parts.append(current_text_block)
                current_text_block = None

            if content_type == "thinking":
                parts.append(
                    {"type": "THINKING", "thinking": content.get("final_response", "")}
                )

            elif content_type == "text":
                text_content = content.get("final_response", "")
                # Append citation markers to the text content
                for citation in content.get("citations", []):
                    url = citation.get("url", None)
                    if url:
                        text_content += f"<sup>[{citation_index}]({url})</sup>"
                        citation_index += 1  # Increment for next citation

                # If we have a current text block, append to it
                if current_text_block is not None:
                    current_text_block["text"] += text_content
                else:
                    # Start a new text block
                    current_text_block = {
                        "type": "TEXT",
                        "text": text_content,
                    }

            elif content_type == "function":
                # Parse the function arguments JSON
                try:
                    arguments = content.get("function", {}).get("arguments")
                    # Return empty dict if no arguments
                    if arguments == "":
                        arguments = {}
                    else:
                        arguments = json.loads(arguments)
                except json.decoder.JSONDecodeError:
                    arguments = content.get("function", {}).get("arguments")

                tool_call = {
                    "id": content.get("id"),
                    "name": content.get("function", {}).get("name"),
                    "arguments": arguments,
                    "type": "function",
                    "server_tool": content.get("server_tool", False),
                }
                parts.append({"type": "TOOL_CALL", "tool_call": tool_call})

            elif content_type == "tool_result":
                tool_use_id = content.get("tool_use_id")
                tool_name = content.get("name", "unknown_tool")
                tool_content = content.get("content", [])
                parts.append(
                    {
                        "type": "TOOL_RESULT",
                        "tool_result": {
                            "id": tool_use_id,
                            "tool_name": tool_name,
                            "server_tool": content.get("server_tool", False),
                            "output": json.dumps(tool_content, ensure_ascii=False),
                        },
                    }
                )

        # Don't forget to flush any remaining text at the end
        if current_text_block is not None:
            parts.append(current_text_block)

        if self.prompt_caching and (cache_read_tokens or cache_creation_tokens):
            print(
                f"[prompt_caching] cache_read_tokens={cache_read_tokens} "
                f"cache_creation_tokens={cache_creation_tokens}",
                flush=True,
            )

        # input_tokens was already normalized to include cache at message_start
        total_input_tokens = input_tokens

        if tool_result:
            if self.has_schema:
                # TODO: come back to this method and have it properly mantain and update the existing parts instead of making a new one
                is_schema, json_str = self._flatten_schema_tool(
                    tool_result, "return_json"
                )
                if is_schema:
                    parts = [{"type": "TEXT", "text": json_str}] if json_str else []
                    if thinking_response:
                        parts.append(
                            {"type": "THINKING", "thinking": thinking_response}
                        )
                    return AskModelEngineResponse2(
                        response=json_str,
                        response_tokens=output_tokens,
                        prompt_tokens=total_input_tokens,
                        cache_read_tokens=cache_read_tokens,
                        cache_creation_tokens=cache_creation_tokens,
                        schemaVersion=2,
                        io="OUTPUT",
                        parts=parts,
                        messageType="CHAT",
                    )

            return AskModelEngineResponse2(
                response=tool_result,
                response_tokens=output_tokens,
                prompt_tokens=total_input_tokens,
                cache_read_tokens=cache_read_tokens,
                cache_creation_tokens=cache_creation_tokens,
                schemaVersion=2,
                io="OUTPUT",
                parts=parts,
                messageType="TOOL",
            )

        return AskModelEngineResponse2(
            response=final_response,
            prompt_tokens=total_input_tokens,
            response_tokens=output_tokens,
            cache_read_tokens=cache_read_tokens,
            cache_creation_tokens=cache_creation_tokens,
            schemaVersion=2,
            io="OUTPUT",
            parts=parts,
            messageType="CHAT",
        )

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

    def _extract_citation_url(self, citation: Any) -> Optional[str]:
        if citation is None:
            return None
        if isinstance(citation, dict):
            return citation.get("url") or (citation.get("source") or {}).get("url")
        return getattr(citation, "url", None) or getattr(
            getattr(citation, "source", None), "url", None
        )

    def _extract_citation_end_index(self, citation: Any) -> Optional[int]:
        if citation is None:
            return None
        if isinstance(citation, dict):
            end = citation.get("end_index")
        else:
            end = getattr(citation, "end_index", None)
        return end if isinstance(end, int) else None

    def _add_inline_citations(self, response: Any) -> str:
        """
        Anthropic text blocks may include `citations` with (start_index/end_index,url).
        This injects `<sup>[n](url)</sup>` markers into the text at each citation end index.
        """
        content_blocks = getattr(response, "content", None) or []

        url_to_number: Dict[str, int] = {}
        next_number = 1
        out = []

        for block in content_blocks:
            block_type = (
                block.get("type")
                if isinstance(block, dict)
                else getattr(block, "type", None)
            )
            if block_type != "text":
                continue

            text = (
                block.get("text", "")
                if isinstance(block, dict)
                else (getattr(block, "text", "") or "")
            )
            citations = (
                block.get("citations")
                if isinstance(block, dict)
                else getattr(block, "citations", None)
            )
            citations = citations or []

            inserts_by_pos: Dict[int, List[str]] = {}
            for citation in citations:
                url = self._extract_citation_url(citation)
                if not url:
                    continue
                if url not in url_to_number:
                    url_to_number[url] = next_number
                    next_number += 1
                number = url_to_number[url]

                pos = self._extract_citation_end_index(citation)
                if pos is None:
                    pos = len(text)
                pos = max(0, min(len(text), pos))
                inserts_by_pos.setdefault(pos, []).append(
                    f"<sup>[{number}]({url})</sup>"
                )

            if inserts_by_pos:
                for pos in sorted(inserts_by_pos.keys(), reverse=True):
                    markers = inserts_by_pos[pos]
                    marker_str = (
                        markers[0]
                        if len(markers) == 1
                        else "<sup>,</sup>".join(markers)
                    )
                    text = text[:pos] + marker_str + text[pos:]

            out.append(text)

        return "".join(out)
