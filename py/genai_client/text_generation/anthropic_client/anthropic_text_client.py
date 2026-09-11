from typing import Optional, Dict, Any, Union, TYPE_CHECKING, List
import json

if TYPE_CHECKING:
    # injected into globals in handle_python of gaas_tcp_server_handler.py
    def smss_stream(
        data: Any, stream_type: str = "content", interim: bool = True
    ) -> None: ...

    from anthropic import Anthropic, AnthropicFoundry
    from anthropic.lib.bedrock import AnthropicBedrock
    from anthropic.lib.vertex import AnthropicVertex


from smss_thread_local import get_smss_stream
from pydantic import BaseModel
from ...message_builders.anthropic.anthropic_models import (
    AnthropicCacheTTL,
    AnthropicRequestConfig,
)
from ...constants import (
    AskModelEngineResponse2,
    TEMPLATE,
    TEMPLATE_NAME,
)
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...message_builders.anthropic.anthropic_message_builder import (
    AnthropicMessageBuilder,
)
from ...message_builders.semoss_base.builtin_tools import has_built_in_tool
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil
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
        cache_ttl: Optional[str] = None,
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
        self.cache_ttl = self._normalize_cache_ttl(cache_ttl)
        self.beta_feature_name = kwargs.pop("beta_feature_name", None)
        if self.use_beta_header and not self.beta_feature_name:
            raise ValueError(
                "beta_feature_name is required when use_beta_header is enabled."
            )

        self.client = self._get_client(**kwargs)
        self.thinking_signature = None

    def _get_client(self, **kwargs) -> Union[
        "Anthropic",
        "AnthropicBedrock",
        "AnthropicFoundry",
        "AnthropicVertex",
    ]:
        if self.provider == "google":
            from ...clients.google_clients import (
                GoogleClient,
                GoogleClientConfig,
                GoogleClientType,
            )

            self.client_config = GoogleClientConfig(
                type=GoogleClientType.ANTHROPIC,
                service_account_credentials=kwargs.pop(
                    "service_account_credentials", None
                ),
                service_account_key_file=kwargs.pop("service_account_key_file", None),
                region=kwargs.pop("region", None),
                project=kwargs.pop("project", None),
                api_key=kwargs.pop("api_key", None),
                # Accept either .smss property name -- BATCH_REGION and
                # GCP_BATCH_REGION have both been used across engines.
                batch_region=kwargs.pop("batch_region", None) or kwargs.pop("gcp_batch_region", None),
            )
            return GoogleClient(config=self.client_config).anthropic_client
        elif self.provider == "bedrock":
            from anthropic.lib.bedrock import AnthropicBedrock

            bedrock_kwargs = {
                "aws_region": kwargs.pop("aws_region", None),
                "aws_access_key": kwargs.pop("aws_access_key", None),
                "aws_secret_key": kwargs.pop("aws_secret_key", None),
                "default_headers": self._get_bedrock_guardrail_headers(
                    kwargs.pop("guardrail_identifier", None),
                    kwargs.pop("guardrail_version", None),
                    trace=kwargs.pop("guardrail_trace", True),
                ),
            }
            try:
                return AnthropicBedrock(**bedrock_kwargs)
            except ValueError:
                if bedrock_kwargs["aws_region"] is not None:
                    raise
                bedrock_kwargs["aws_region"] = "us-east-1"
                return AnthropicBedrock(**bedrock_kwargs)
        elif self.provider == "azure":
            from anthropic import AnthropicFoundry

            return AnthropicFoundry(
                base_url=kwargs.pop("endpoint", None),
                api_key=kwargs.pop("api_key", None),
            )
        elif self.provider == "anthropic":
            from anthropic import Anthropic

            return Anthropic(
                api_key=kwargs.pop("api_key", None),
            )
        else:
            raise ValueError(
                f"Provider '{self.provider}' is not supported for Anthropic Text Client."
            )

    @staticmethod
    def _normalize_cache_ttl(cache_ttl: Optional[str]) -> Optional[str]:
        """
        Resolve the cache_ttl the engine was initialized with to one of the
        values Anthropic accepts on cache_control.

        Returns None for an unset value, which leaves the ttl field off the
        request entirely and gets Anthropic's 5 minute default.
        """
        if cache_ttl is None:
            return None
        ttl = str(cache_ttl).strip().lower()
        if not ttl:
            return None
        if ttl not in AnthropicCacheTTL.values():
            raise ValueError(
                f"cache_ttl '{cache_ttl}' is not supported. Valid values are: "
                + ", ".join(AnthropicCacheTTL.values())
            )
        return ttl

    def _cache_control(self) -> Dict[str, Any]:
        """
        Build the cache_control payload applied at every breakpoint. The ttl key
        is only included when one was explicitly requested so requests that rely
        on the default lifetime keep their existing shape.
        """
        cache_control: Dict[str, Any] = {"type": "ephemeral"}
        if self.cache_ttl:
            cache_control["ttl"] = self.cache_ttl
        return cache_control

    @staticmethod
    def _get_bedrock_guardrail_headers(
        guardrail_identifier: Optional[str],
        guardrail_version: Optional[str],
        trace: Union[str, bool] = True,
    ) -> Optional[Dict[str, str]]:
        """
        Bedrock's InvokeModel API takes guardrails as request headers rather
        than the guardrailConfig body field used by the Converse API.
        """
        if not guardrail_identifier and not guardrail_version:
            return None
        if not (guardrail_identifier and guardrail_version):
            raise ValueError(
                "Both guardrail_identifier and guardrail_version are required to apply a Bedrock guardrail."
            )
        headers = {
            "X-Amzn-Bedrock-GuardrailIdentifier": guardrail_identifier,
            "X-Amzn-Bedrock-GuardrailVersion": guardrail_version,
        }
        if string_to_bool(trace) if isinstance(trace, str) else trace:
            headers["X-Amzn-Bedrock-Trace"] = "ENABLED"
        return headers

    @staticmethod
    def _process_bedrock_guardrail_trace(obj: Any) -> Optional[Dict[str, Any]]:
        """
        With the trace header enabled, Bedrock attaches the guardrail trace as
        extra JSON fields (amazon-bedrock-trace / amazon-bedrock-guardrailAction)
        on the InvokeModel response body -- the final message when non-streaming,
        the message_stop event when streaming. The Anthropic SDK preserves
        unknown fields in model_extra.

        Logs the raw trace and, when the guardrail intervened, returns a
        GUARDRAIL part listing each policy rule that matched so it can be
        included in the response body.
        """
        extra = getattr(obj, "model_extra", None) or {}
        action = extra.get("amazon-bedrock-guardrailAction")
        trace = extra.get("amazon-bedrock-trace")
        if not action and not trace:
            return None

        violations = []
        guardrail_trace = trace.get("guardrail", {}) if isinstance(trace, dict) else {}
        assessments = []
        input_assessments = guardrail_trace.get("input")
        if isinstance(input_assessments, dict):
            assessments.append(("INPUT", input_assessments))
        for output_assessment in guardrail_trace.get("outputs") or []:
            if isinstance(output_assessment, dict):
                assessments.append(("OUTPUT", output_assessment))

        for source, by_guardrail_id in assessments:
            for assessment in by_guardrail_id.values():
                if not isinstance(assessment, dict):
                    continue
                for policy_name, policy in assessment.items():
                    if not isinstance(policy, dict):
                        continue
                    for rules in policy.values():
                        if not isinstance(rules, list):
                            continue
                        for rule in rules:
                            if not isinstance(rule, dict):
                                continue
                            rule_action = rule.get("action")
                            if rule_action and rule_action != "NONE":
                                violations.append(
                                    {
                                        "source": source,
                                        "policy": policy_name,
                                        "rule": rule.get("name")
                                        or rule.get("type")
                                        or rule.get("match", ""),
                                        "action": rule_action,
                                    }
                                )

        if action == "INTERVENED" or violations:
            return {
                "type": "GUARDRAIL",
                "action": action,
                "violations": violations,
            }
        return None

    def _apply_cache_to_tools(self, request_config: "AnthropicRequestConfig") -> None:
        """
        Add cache_control to the last tool definition. Tools are evaluated
        first in Anthropic's cache breakpoint order (tools -> system -> messages),
        so caching them saves tokens whenever the tool list is large and static.
        """
        tools = request_config.tools
        if not tools:
            return
        tools[-1]["cache_control"] = self._cache_control()

    def _apply_cache_to_system(self, request_config: "AnthropicRequestConfig") -> None:
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
                {
                    "type": "text",
                    "text": system,
                    "cache_control": self._cache_control(),
                }
            ]
        elif isinstance(system, list):
            # Already a list - attach to the last text block.
            for block in reversed(system):
                if isinstance(block, dict) and block.get("type") == "text":
                    block["cache_control"] = self._cache_control()
                    break

    # Block types that Anthropic supports cache_control on.
    _CACHEABLE_BLOCK_TYPES = {"text", "tool_result", "image", "document"}

    def _apply_cache_to_last_block(self, messages: List[Dict[str, Any]]) -> None:
        """
        Add cache_control to the last cacheable block of the last message. This
        replicates Anthropic's automatic caching behaviour for providers
        (Bedrock, Vertex) that only support block-level cache_control.

        On each turn the last message is the newest one, so the marker
        naturally moves forward through the conversation as history grows.

        Supports text, tool_result, image, and document blocks - not just text -
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
                    "cache_control": self._cache_control(),
                }
            ]
        elif isinstance(content, list):
            for block in reversed(content):
                if (
                    isinstance(block, dict)
                    and block.get("type") in AnthropicTextClient._CACHEABLE_BLOCK_TYPES
                ):
                    block["cache_control"] = self._cache_control()
                    break

    def ask_call(
        self,
        prefix="",
        **kwargs,
    ):
        if self.client is None:
            raise ValueError("Anthropic client is not initialized.")
        try:
            if self.model_settings.global_param_override:
                kwargs.update(self.model_settings.global_param_override)

            web_search_enabled = has_built_in_tool(
                kwargs.get("built_in_tools"), "web_search"
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
                    self.model_name,
                    self.use_beta_header,
                    self.beta_feature_name,
                    self.thinking_signature,
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
                    request_config.cache_control = self._cache_control()
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
                    f"[prompt_caching] ttl={self.cache_ttl or '5m'} "
                    f"cache_read_tokens={cache_read_tokens} "
                    f"cache_creation_tokens={cache_creation_tokens}",
                    flush=True,
                )

            parts = []
            if response_text:
                parts.append({"type": "TEXT", "text": response_text})
            if thinking_text:
                parts.append({"type": "THINKING", "thinking": thinking_text})

            metadata = {}
            if self.provider == "bedrock":
                metadata["guardrail_response"] = self._process_bedrock_guardrail_trace(
                    response
                )

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
                metadata=metadata,
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
        # preamble text blocks the model emitted alongside the tool_use blocks
        preamble_text = ""
        for content in response.content:
            if content.type == "tool_use":
                tool_use = {
                    "id": content.id,
                    "name": content.name,
                    "arguments": content.input,
                    "type": "function",
                }
                tools_result.append(tool_use)
            elif content.type == "text":
                preamble_text += getattr(content, "text", "") or ""

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

        text_parts = [{"type": "TEXT", "text": preamble_text}] if preamble_text else []

        return AskModelEngineResponse2(
            response=tools_result,
            response_tokens=response_tokens,
            prompt_tokens=prompt_tokens,
            cache_read_tokens=cache_read_tokens,
            cache_creation_tokens=cache_creation_tokens,
            schemaVersion=2,
            io="OUTPUT",
            parts=text_parts
            + [{"type": "TOOL_CALL", "tool_call": t} for t in tools_result],
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
        server_tool_use_names: Dict[str, str] = {}
        metadata = {}

        use_beta_stream = self.use_beta_header and hasattr(
            self.client.beta.messages, "stream"
        )
        stream_method = (
            self.client.beta.messages.create
            if use_beta_stream
            else self.client.messages.create
        )

        stream_kwargs = request_config.model_dump(exclude_none=True)
        stream_kwargs["stream"] = True
        if self.use_beta_header and not use_beta_stream:
            stream_kwargs.pop("betas", None)
            stream_kwargs["extra_headers"] = {"anthropic-beta": self.beta_feature_name}

        with stream_method(**stream_kwargs) as stream:
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
                        if getattr(event.delta, "type", None) == "citations_delta":
                            item = event.delta.citation
                            this_content_block.setdefault("citations", []).append(
                                {
                                    "type": getattr(item, "type", None),
                                    "url": getattr(item, "url", None),
                                    "title": getattr(item, "title", None),
                                    "encrypted_index": getattr(
                                        item, "encrypted_index", None
                                    ),
                                    "cited_text": getattr(item, "cited_text", None),
                                }
                            )
                        elif hasattr(event.delta, "text"):
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

                    content_array.append(this_content_block)
                    this_content_block = {}
                    this_content_block_type = ""

                elif event.type == "message_stop" and self.provider == "bedrock":
                    metadata["guardrail_response"] = (
                        self._process_bedrock_guardrail_trace(event)
                    )

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

        citation_index = 1
        parts = []
        current_text_block = None
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
                    current_text_block = {
                        "type": "TEXT",
                        "text": text_content,
                    }

            elif content_type == "function":
                try:
                    arguments = content.get("function", {}).get("arguments")
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

        if current_text_block is not None:
            parts.append(current_text_block)

        if self.prompt_caching and (cache_read_tokens or cache_creation_tokens):
            print(
                f"[prompt_caching] ttl={self.cache_ttl or '5m'} "
                f"cache_read_tokens={cache_read_tokens} "
                f"cache_creation_tokens={cache_creation_tokens}",
                flush=True,
            )

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
                        metadata=metadata,
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
                metadata=metadata,
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
            metadata=metadata,
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

    # ------------------------------------------------------------------
    # Batch API (Anthropic native Message Batches)
    #
    # Lifecycle: submit -> provider batch id -> poll status -> fetch results.
    # All methods return plain JSON-serializable dicts so they marshal cleanly
    # back to the Java engine over the TCP PayloadStruct protocol.
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_batch_status(status):
        s = (status or "").lower()
        mapping = {
            "in_progress": "IN_PROGRESS",
            "canceling": "CANCELING",
            "ended": "COMPLETED",
        }
        return mapping.get(s, s.upper() or "UNKNOWN")

    def _ensure_native_batch_supported(self):
        """Batch is supported for direct Anthropic (native Message Batches API)
        and Anthropic-on-Vertex (GCS-mediated Vertex batch prediction jobs, see
        the _*_vertex methods below); raise for any other provider (bedrock,
        azure)."""
        if self.provider not in ("anthropic", "google"):
            raise ValueError(
                f"Batch is not supported for Anthropic provider '{self.provider}'."
            )

    # Vertex batch prediction jobs are a regional resource -- they cannot be
    # created (or looked up) against the "global" endpoint this engine uses
    # for regular, non-batch inference. Fall back to a real region only for
    # batch calls; every batch call site must agree on the same fallback
    # since a job's resource name is region-qualified.
    def _vertex_genai_client(self):
        """A google.genai.Client configured for Vertex AI, for .batches.*.
        Reuses this engine's own Vertex project/credentials (already loaded
        for non-batch Anthropic-on-Vertex inference) -- unrelated to whatever
        GCS storage engine's credentials a batch call was given.

        Unlike regular inference, a Vertex AI batch prediction job for
        Anthropic models is a regional resource and rejects region="global"
        outright -- raise rather than silently substituting a region the
        caller never configured, since a wrong guess would create the job
        somewhere the caller doesn't expect (and later status/results calls
        must agree on the same region to find it again)."""
        from ...clients.google_clients import (
            GoogleClient,
            GoogleClientConfig,
            GoogleClientType,
        )

        region = self.client_config.batch_region or self.client_config.region
        if not region or region.strip().lower() == "global":
            raise ValueError(
                "Vertex AI batch prediction requires a real (non-global) region for "
                "Anthropic-on-Vertex models. This engine's REGION is "
                f"{self.client_config.region!r}; set BATCH_REGION on the engine's "
                ".smss to a supported Anthropic-on-Vertex region (e.g. 'us-east5')."
            )

        config = GoogleClientConfig(
            type=GoogleClientType.GOOGLE,
            service_account_credentials=self.client_config.service_account_credentials,
            service_account_key_file=self.client_config.service_account_key_file,
            region=region,
            project=self.client_config.project,
        )
        return GoogleClient(config=config).genai_client

    def _submit_batch_vertex(self, requests, **kwargs) -> Dict[str, Any]:
        """Anthropic-on-Vertex has no hosted batch API; a Vertex AI batch
        prediction job is used instead, staged through Cloud Storage. This
        Python client never touches that bucket directly -- Java drives the
        upload through the SEMOSS storage engine that holds its credentials
        (see ModelBatchManager.submitVertexBatch), calling this method twice:

        Phase 1 (no inputUri kwarg): build the input JSONL content only, no
        GCS/Vertex touched -- Java uploads what comes back in raw.jsonl_content.
        Phase 2 (inputUri/outputUriPrefix kwargs present, requests empty):
        Java has already uploaded the input file; just create the job.
        """
        input_uri = kwargs.pop("inputUri", None)
        output_uri_prefix = kwargs.pop("outputUriPrefix", None)
        if input_uri and output_uri_prefix:
            from google.genai.types import CreateBatchJobConfig

            from ...clients.vertex_batch_client import normalize_job_state

            genai_client = self._vertex_genai_client()
            job = genai_client.batches.create(
                model=f"publishers/anthropic/models/{self.model_settings.model_name}",
                src=input_uri,
                config=CreateBatchJobConfig(dest=output_uri_prefix),
            )
            return {
                "provider_batch_id": job.name,
                "status": normalize_job_state(job.state),
                "raw": {"name": job.name, "state": str(job.state)},
            }

        if isinstance(requests, str):
            requests = json.loads(requests)
        requests = [
            self._normalize_request_for_batch(r, i)
            for i, r in enumerate(requests or [])
        ]
        default_max_tokens = getattr(self.model_settings, "max_tokens", None) or 1024
        lines = []
        for req in requests or []:
            params = dict(req.get("body") or req.get("params") or {})
            # model is set at the job level, not per line
            params.pop("model", None)
            params.setdefault("max_tokens", default_max_tokens)
            params.setdefault("anthropic_version", "vertex-2023-10-16")
            lines.append({"custom_id": str(req.get("custom_id")), "request": params})
        if not lines:
            raise ValueError("submit_batch requires at least one request")

        jsonl_content = "\n".join(json.dumps(line, ensure_ascii=False) for line in lines)
        return {
            "status": "BUILT",
            "request_count": len(lines),
            "raw": {"jsonl_content": jsonl_content},
        }

    def _get_batch_status_vertex(self, provider_batch_id: str, **kwargs) -> Dict[str, Any]:
        from ...clients.vertex_batch_client import job_output_uri, normalize_job_state

        job_name = provider_batch_id.split(".", 1)[-1]
        genai_client = self._vertex_genai_client()
        job = genai_client.batches.get(name=job_name)
        return {
            "provider_batch_id": provider_batch_id,
            "status": normalize_job_state(job.state),
            "counts": None,
            "raw": {
                "name": job.name,
                "state": str(job.state),
                "output_uri": job_output_uri(job),
            },
        }

    def _get_batch_results_vertex(self, provider_batch_id: str, **kwargs) -> Dict[str, Any]:
        """rawBlobs here is the raw text content of every output shard Java
        already downloaded through the storage engine -- this method only
        normalizes lines into the SEMOSS shape; it never touches GCS itself."""
        from ...clients.vertex_batch_client import normalize_job_state, normalize_result_line

        job_name = provider_batch_id.split(".", 1)[-1]
        raw_blobs = kwargs.pop("rawBlobs", None) or []
        if isinstance(raw_blobs, str):
            raw_blobs = json.loads(raw_blobs)

        items = []
        for blob_text in raw_blobs:
            for raw_line in blob_text.splitlines():
                raw_line = raw_line.strip()
                if raw_line:
                    items.append(normalize_result_line(json.loads(raw_line)))

        genai_client = self._vertex_genai_client()
        job = genai_client.batches.get(name=job_name)
        return {
            "provider_batch_id": provider_batch_id,
            "status": normalize_job_state(job.state),
            "count": len(items),
            "results": items,
        }

    def _list_batches_vertex(self, limit: int = 20, **kwargs) -> Dict[str, Any]:
        """Lists Vertex AI batch jobs for this project/region. Each returned
        provider_batch_id is the bare Vertex job name (no storageEngineId
        prefix) since a project-wide listing has no single storage engine to
        attach -- results for one of these ids would need "storage" supplied
        again by the caller some other way; not a well-supported path today."""
        from ...clients.vertex_batch_client import normalize_job_state

        genai_client = self._vertex_genai_client()
        resp = genai_client.batches.list(config={"page_size": limit})
        batches = []
        for job in resp:
            batches.append(
                {
                    "provider_batch_id": job.name,
                    "status": normalize_job_state(job.state),
                    "request_count": None,
                    "created_at": str(getattr(job, "create_time", None)),
                }
            )
        return {"batches": batches}

    def _cancel_batch_vertex(self, provider_batch_id: str, **kwargs) -> Dict[str, Any]:
        from ...clients.vertex_batch_client import normalize_job_state

        job_name = provider_batch_id.split(".", 1)[-1]
        genai_client = self._vertex_genai_client()
        job = genai_client.batches.cancel(name=job_name)
        if job is None:
            job = genai_client.batches.get(name=job_name)
        return {
            "provider_batch_id": provider_batch_id,
            "status": normalize_job_state(getattr(job, "state", None)),
            "raw": {"name": job_name},
        }

    def _normalize_request_for_batch(self, req: Any, idx: int) -> Dict[str, Any]:
        """Convert simplified {command, context} format to Anthropic batch wire format."""
        if not isinstance(req, dict):
            return req
        if req.get("message_json"):
            return self._build_batch_params_from_history(req, idx)
        if "command" not in req:
            return req
        custom_id = req.get("custom_id") or f"req-{idx}"
        params = {"messages": [{"role": "user", "content": req["command"]}]}
        if req.get("context"):
            params["system"] = req["context"]
        skip = {"command", "context", "custom_id"}
        for k, v in req.items():
            if k not in skip:
                params[k] = v
        return {"custom_id": custom_id, "params": params}

    def _build_batch_params_from_history(
        self, req: Dict[str, Any], idx: int
    ) -> Dict[str, Any]:
        """Build per-request Anthropic batch params from a full SEMOSS message_json +
        tools, reusing the same message builder the synchronous ask path uses."""
        custom_id = req.get("custom_id") or f"req-{idx}"
        skip = {"command", "context", "custom_id", "message_json"}
        kwargs = {k: v for k, v in req.items() if k not in skip}
        semoss_messages = self.build_semoss_messages(
            model_settings=self.model_settings,
            message_json=req["message_json"],
            **kwargs,
        )
        msg_builder_response = AnthropicMessageBuilder().build_messages(
            semoss_messages,
            self.model_settings,
            self.model_name,
            self.use_beta_header if self.use_beta_header else False,
            self.beta_feature_name,
            thinking_signature=self.thinking_signature,
        )
        params = msg_builder_response.request_config.model_dump(exclude_none=True)

        params.pop("stream", None)  # no streaming for batch
        params.pop("betas", None)
        extra_body = params.pop("extra_body", None)
        if extra_body:
            params.update(extra_body)
        return {"custom_id": custom_id, "params": params}

    def submit_batch(self, requests, **kwargs) -> Dict[str, Any]:
        """Submit a batch of requests to the Anthropic API."""
        from anthropic.types.message_create_params import (
            MessageCreateParamsNonStreaming,
        )
        from anthropic.types.messages.batch_create_params import Request
        from anthropic.types.messages import MessageBatch

        self._ensure_native_batch_supported()
        if self.provider == "google":
            return self._submit_batch_vertex(requests, **kwargs)
        if isinstance(requests, str):
            requests = json.loads(requests)
        requests = [
            self._normalize_request_for_batch(r, i)
            for i, r in enumerate(requests or [])
        ]
        default_max_tokens = getattr(self.model_settings, "max_tokens", None) or 1024
        batch_requests = []
        for req in requests or []:
            params = dict(req.get("body") or req.get("params") or {})
            model = params.pop("model", None) or self.model_settings.model_name
            params.setdefault("max_tokens", default_max_tokens)
            request = Request(
                custom_id=str(req.get("custom_id")),
                params=MessageCreateParamsNonStreaming(
                    **params,
                    model=model,
                ),
            )
            batch_requests.append(request)
        if not batch_requests:
            raise ValueError("submit_batch requires at least one request")

        batch: MessageBatch = self.client.messages.batches.create(
            requests=batch_requests
        )
        return {
            "provider_batch_id": batch.id,
            "status": self._normalize_batch_status(batch.processing_status),
            "request_count": len(batch_requests),
            "endpoint": "/v1/messages/batches",
            "results_url": batch.results_url,
            "raw": batch.model_dump(),
        }

    def get_batch_status(self, provider_batch_id: str, **kwargs) -> Dict[str, Any]:
        """Get the status of a previously submitted batch."""
        self._ensure_native_batch_supported()
        if self.provider == "google":
            return self._get_batch_status_vertex(provider_batch_id, **kwargs)
        batch = self.client.messages.batches.retrieve(provider_batch_id)
        rc = batch.request_counts
        counts = {
            "total": rc.processing
            + rc.succeeded
            + rc.errored
            + rc.canceled
            + rc.expired,
            "completed": rc.succeeded,
            "failed": rc.errored + rc.canceled + rc.expired,
            "in_progress": rc.processing,
        }
        return {
            "provider_batch_id": batch.id,
            "status": self._normalize_batch_status(batch.processing_status),
            "counts": counts,
            "output_ref": None,
            "error_ref": None,
            "results_url": batch.results_url,
            "raw": batch.model_dump(),
        }

    def get_batch_results(self, provider_batch_id: str, **kwargs) -> Dict[str, Any]:
        """Get the results of a previously submitted batch"""
        self._ensure_native_batch_supported()
        if self.provider == "google":
            return self._get_batch_results_vertex(provider_batch_id, **kwargs)
        items = []
        raw_lines = []
        for entry in self.client.messages.batches.results(provider_batch_id):
            result = entry.result
            rtype = result.type
            ok = rtype == "succeeded"

            batch = {
                "custom_id": entry.custom_id,
                "ok": ok,
                "status": rtype,
                "message": None,
                "error": None,
                "error_type": None,
                "error_message": None,
                "input_tokens": None,
                "output_tokens": None,
            }

            if result.type == "succeeded":
                msg = result.message
                batch["message"] = {
                    "role": msg.role,
                    "content": msg.content,
                }
                batch["input_tokens"] = msg.usage.input_tokens
                batch["output_tokens"] = msg.usage.output_tokens
            elif result.type == "errored":
                batch["error"] = result.error.model_dump()
                batch["error_type"] = result.error.error.type
                batch["error_message"] = result.error.error.message

            items.append(batch)
            raw_lines.append(
                json.dumps(entry.model_dump(), ensure_ascii=False, default=str)
            )
        return {
            "provider_batch_id": provider_batch_id,
            "count": len(items),
            "results": items,
            "raw_jsonl": "\n".join(raw_lines),
        }

    def list_batches(self, limit: int = 20, **kwargs) -> Dict[str, Any]:
        """List previously submitted batches."""
        self._ensure_native_batch_supported()
        if self.provider == "google":
            return self._list_batches_vertex(limit, **kwargs)
        list_kwargs: Dict[str, Any] = {"limit": limit}
        after = kwargs.get("after")
        if not after:
            after = kwargs.get("after_id")
        if after is not None:
            list_kwargs["after_id"] = after
        before = kwargs.get("before")
        if not before:
            before = kwargs.get("before_id")
        if before is not None:
            list_kwargs["before_id"] = before
        batches_client = getattr(self.client.messages, "batches", None)
        resp = (
            batches_client.list(**list_kwargs) if batches_client is not None else None
        )

        data = getattr(resp, "data", []) or []
        batches = []
        for b in data:
            batches.append(
                {
                    "provider_batch_id": b.id,
                    "status": self._normalize_batch_status(
                        getattr(b, "processing_status", None)
                    ),
                    "request_count": None,
                    "created_at": getattr(b, "created_at", None),
                }
            )
        return {"batches": batches}

    def cancel_batch(self, provider_batch_id: str, **kwargs) -> Dict[str, Any]:
        """Cancel a previously submitted batch."""
        self._ensure_native_batch_supported()
        if self.provider == "google":
            return self._cancel_batch_vertex(provider_batch_id, **kwargs)
        batch = self.client.messages.batches.cancel(provider_batch_id)
        return {
            "provider_batch_id": batch.id,
            "status": batch.processing_status,
            "raw": batch.model_dump(),
        }
