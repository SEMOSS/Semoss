from typing import List, Optional, Dict, Any, Union, TYPE_CHECKING

if TYPE_CHECKING:
    # injected into globals in handle_python of gaas_tcp_server_handler.py
    def smss_stream(
        data: Any, stream_type: str = "content", interim: bool = True
    ) -> None: ...


from smss_thread_local import get_smss_stream
import json
from pydantic import BaseModel
from ...clients.google_clients import (
    GoogleClient,
    GoogleClientConfig,
    GoogleClientType,
)
from ...message_builders.anthropic.anthropic_models import AnthropicRequestConfig
from ...constants import AskModelEngineResponse, TEMPLATE, TEMPLATE_NAME
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...message_builders.anthropic.anthropic_message_builder import (
    AnthropicMessageBuilder,
)
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil
from anthropic import AnthropicBedrock


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
        self.beta_feature_name = kwargs.pop("beta_feature_name", None)
        if self.use_beta_header and not self.beta_feature_name:
            raise ValueError(
                "beta_feature_name is required when use_beta_header is enabled."
            )

        self.client = self._get_client(**kwargs)

    def _get_client(self, **kwargs):
        # TODO: Implement support for Anthropic API directly
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
        else:
            raise ValueError(
                f"Provider '{self.provider}' is not supported for Anthropic Text Client."
            )

    def ask_call(
        self,
        prefix="",
        **kwargs,
    ):
        if self.client is None:
            raise ValueError("Anthropic client is not initialized.")

        self.semoss_messages = self.build_semoss_messages(
            model_settings=self.model_settings, **kwargs
        )

        try:
            msg_builder_response = AnthropicMessageBuilder().build_messages(
                self.semoss_messages,
                self.model_limits,
                self.model_name,
                self.use_beta_header,
                self.beta_feature_name,
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to build messages in Anthropic format from SEMOSS format: {e}"
            )

        request_config = msg_builder_response.request_config
        streaming = msg_builder_response.streaming
        self.has_schema = msg_builder_response.has_structured_input

        if streaming:
            return self._handle_streaming(request_config, prefix=prefix)
        else:
            if self.use_beta_header:
                response = self.client.beta.messages.create(
                    **request_config.model_dump(exclude_none=True),
                )
            else:
                response = self.client.messages.create(
                    **request_config.model_dump(exclude_none=True),
                )

            if response.stop_reason == "tool_use":
                return self._parse_tools_call_response(
                    response,
                    prompt_tokens=response.usage.input_tokens,
                    response_tokens=response.usage.output_tokens,
                )

            thinking_text = ""
            response_text = ""
            for content in response.content:
                if hasattr(content, "type") and content.type == "thinking":
                    thinking_text += content.thinking
                elif hasattr(content, "type") and content.type == "text":
                    response_text += content.text

            usage = Usage(
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
            )

        return AskModelEngineResponse(
            response=response_text,
            response_tokens=usage.output_tokens,
            prompt_tokens=usage.input_tokens,
            messageType="CHAT",
            thinking=thinking_text,
        )

    def _parse_tools_call_response(
        self, response, prompt_tokens: int = None, response_tokens: int = None
    ) -> AskModelEngineResponse:
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
                return AskModelEngineResponse(
                    response=json_str,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                    messageType="CHAT",
                )

        return AskModelEngineResponse(
            response=tools_result,
            response_tokens=response_tokens,
            prompt_tokens=prompt_tokens,
            messageType="TOOL",
        )

    def _handle_streaming(
        self, request_config: AnthropicRequestConfig, prefix: str = ""
    ) -> AskModelEngineResponse:
        # Get the stream function for the current thread
        smss_stream = get_smss_stream()

        input_tokens = 0
        output_tokens = 0

        content_array = []
        this_content_block = {}
        this_content_block_type = ""

        # since we can have text and tools
        # we will declare this a tool response
        # if any tools come back
        tool_result = []
        try:
            stream_method = (
                self.client.beta.messages.stream
                if self.use_beta_header
                else self.client.messages.stream
            )

            with stream_method(
                **request_config.model_dump(exclude_none=True)
            ) as stream:
                # Handle different types of streaming events
                for event in stream:
                    if event.type == "message_start":
                        input_tokens = event.message.usage.input_tokens
                    elif event.type == "content_block_start":
                        this_content_block_type = event.content_block.type
                        this_content_block["type"] = this_content_block_type
                        # start context block
                        if this_content_block_type == "text":
                            text_chunk = event.content_block.text
                            this_content_block["final_response"] = text_chunk

                            data = StreamUtil.create_content_chunk(text_chunk)
                            smss_stream(data, stream_type="content")
                            print(prefix + text_chunk, end="", flush=True)

                        # start thinking block
                        elif this_content_block_type == "thinking":
                            text_chunk = event.content_block.thinking
                            this_content_block["final_response"] = text_chunk

                            data = StreamUtil.create_content_chunk(text_chunk)
                            smss_stream(data, stream_type="content")
                            print(prefix + text_chunk, end="", flush=True)

                        # start tool use block
                        elif this_content_block_type == "tool_use":
                            this_content_block.update(
                                {
                                    "id": None,
                                    "type": "function",
                                    "function": {"name": None, "arguments": ""},
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

                            data = StreamUtil.create_tool_type_chunk(
                                index=len(tool_result)
                            )
                            smss_stream(data, stream_type="tool")
                            print(prefix + str(data), end="")

                            data = StreamUtil.create_function_name_chunk(
                                index=len(tool_result),
                                function_name=event.content_block.name,
                            )
                            smss_stream(data, stream_type="tool")
                            print(prefix + str(data), end="")

                    elif event.type == "content_block_delta":
                        # text delta
                        if this_content_block_type == "text":
                            text_chunk = event.delta.text
                            this_content_block["final_response"] += text_chunk

                            data = StreamUtil.create_content_chunk(text_chunk)
                            smss_stream(data, stream_type="content")
                            print(prefix + text_chunk, end="", flush=True)

                        # thinking delta
                        elif this_content_block_type == "thinking":
                            # we can ignore the thinking signature...
                            if event.delta.type == "signature_delta":
                                continue

                            text_chunk = event.delta.thinking
                            this_content_block["final_response"] += text_chunk

                            data = StreamUtil.create_content_chunk(text_chunk)
                            smss_stream(data, stream_type="content")
                            print(prefix + text_chunk, end="", flush=True)

                        # tool delta
                        elif this_content_block_type == "tool_use":
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
                        if this_content_block_type == "tool_use":
                            # append the tool result as a anthropic tool
                            try:
                                arguments = json.loads(
                                    this_content_block["function"]["arguments"]
                                )
                            except json.decoder.JSONDecodeError:
                                arguments = this_content_block["function"]["arguments"]

                            tool_result.append(
                                {
                                    "id": this_content_block["id"],
                                    "type": this_content_block["type"],
                                    "name": this_content_block["function"]["name"],
                                    "arguments": arguments,
                                }
                            )
                        # append this content block
                        # and create a new block
                        content_array.append(this_content_block)
                        this_content_block = {}
                        this_content_block_type = ""

                    elif event.type == "message_delta":
                        output_tokens = event.usage.output_tokens

            # we are done iterating
            # do we have tools that we need to do a tool response?
            if tool_result:
                data = StreamUtil.create_finish_reason_chunk("tool_use")
                smss_stream(data, stream_type="tool", interim=False)
            else:
                data = StreamUtil.create_finish_reason_chunk("stop")
                smss_stream(data, stream_type="content", interim=False)

            # aggregate text blocks
            final_response = ""
            thinking_response = ""
            for content in content_array:
                if content.get("final_response", None):
                    if content.get("type", None) == "thinking":
                        thinking_response += content.get("final_response")
                    else:
                        final_response += content.get("final_response")

            if tool_result:
                if self.has_schema:
                    is_schema, json_str = self._flatten_schema_tool(
                        tool_result, "return_json"
                    )
                    if is_schema:
                        return AskModelEngineResponse(
                            response=json_str,
                            response_tokens=output_tokens,
                            prompt_tokens=input_tokens,
                            messageType="CHAT",
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
