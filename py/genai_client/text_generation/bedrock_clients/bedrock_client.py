from typing import Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:

    def smss_stream(
        data: Any, stream_type: str = "content", interim: bool = True
    ) -> None: ...


import json, boto3, re
import botocore.exceptions
from smss_thread_local import get_smss_stream
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil
from ...message_builders.bedrock.bedrock_message_builder import BedrockMessageBuilder
from ...constants import AskModelEngineResponse


class BedrockClient(AbstractTextGenerationClient):
    def __init__(
        self,
        modelId: str,
        region: str,
        service_name: str = "bedrock-runtime",
        access_key: str = None,
        secret_key: str = None,
        template: str = None,
        template_name: str = None,
        guardrail_identifier: str = None,
        guardrail_version: str = None,
        **kwargs,
    ):
        init_params = {
            "model_name": modelId,
            **kwargs,
        }
        super().__init__(template=template, template_name=template_name, **init_params)
        self.kwargs = kwargs
        self.model_id = modelId

        self.client = self._create_client(region, access_key, secret_key, service_name)
        self.guardrail_config = self._create_guardrail_config(
            guardrail_identifier, guardrail_version
        )

    def _create_client(
        self,
        region: str,
        access_key: str = None,
        secret_key: str = None,
        service_name: str = None,
    ) -> boto3.client:
        """Create a boto3 client for Bedrock with appropriate authentication."""
        if access_key and secret_key:
            return boto3.client(
                service_name=service_name,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
            )
        else:
            # We assume keys are provided by environment or IAM role
            return boto3.client(
                service_name=service_name,
                region_name=region,
            )

    def _create_guardrail_config(
        self, guardrail_identifier: str, guardrail_version: str
    ) -> Dict[str, Any]:
        """Create guardrail configuration if enabled."""
        if guardrail_identifier and guardrail_version:
            return {
                "guardrailIdentifier": guardrail_identifier,
                "guardrailVersion": guardrail_version,
                "trace": "enabled",
            }
        return None

    def ask_call(
        self,
        prefix: str = "",
        **kwargs,
    ) -> AskModelEngineResponse:
        """Entry point for making Bedrock ask calls."""
        if self.client is None:
            raise RuntimeError("Bedrock client is not initialized.")

        semoss_messages = self.build_semoss_messages(
            model_settings=self.model_settings, **kwargs
        )

        try:
            bedrock_request = BedrockMessageBuilder().build_messages(semoss_messages)

            stream = bedrock_request.pop("stream", True)
            self.has_schema = bedrock_request.pop("has_schema", False)
            bedrock_request["guardrailConfig"] = self.guardrail_config

            bedrock_request = {
                k: v for k, v in bedrock_request.items() if v is not None
            }

        except Exception as e:
            raise ValueError(f"Error building Bedrock messages: {str(e)}") from e

        if stream:
            return self._handle_streaming(bedrock_request, prefix=prefix)
        else:
            return self._handle_non_streaming(bedrock_request)

    def _handle_streaming(
        self, request: Dict[str, Any], prefix: str = ""
    ) -> AskModelEngineResponse:
        """Handle streaming responses from Bedrock."""
        smss_stream = get_smss_stream()

        try:
            stream_response = self.client.converse_stream(
                modelId=self.model_id, **request
            )

            stop_reason = ""
            final_response = ""
            input_tokens = 0
            output_tokens = 0

            content_array = []
            this_content_block = {}
            this_content_block_type = ""

            # since we can have text and tools
            # we will declare this a tool response
            # if any tools come back
            tool_result = []

            for event in stream_response.get("stream", []):
                if "messageStart" in event:
                    pass

                # only tools get a contentBlockStart for some reason...
                elif "contentBlockStart" in event:
                    start_this_content = event["contentBlockStart"]["start"]
                    tool_use_content = start_this_content.get("toolUse", None)
                    if tool_use_content is not None:
                        this_content_block_type = "tool_use"
                        this_content_block.update(
                            {
                                "id": None,
                                "type": "function",
                                "function": {"name": None, "arguments": ""},
                            }
                        )
                        this_content_block["id"] = tool_use_content["toolUseId"]
                        this_content_block["function"]["name"] = tool_use_content[
                            "name"
                        ]
                        data = StreamUtil.create_tool_id_chunk(
                            index=len(tool_result),
                            tool_id=tool_use_content["toolUseId"],
                        )
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                        data = StreamUtil.create_tool_type_chunk(index=len(tool_result))
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                        data = StreamUtil.create_function_name_chunk(
                            index=len(tool_result),
                            function_name=tool_use_content["name"],
                        )
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                # this is for normal text and tool arguments
                elif "contentBlockDelta" in event:
                    this_content_delta = event["contentBlockDelta"]["delta"]

                    if "text" in this_content_delta:
                        text_chunk = this_content_delta["text"]
                        if text_chunk is not None:
                            if "final_response" in this_content_block:
                                this_content_block["final_response"] += text_chunk
                            else:
                                this_content_block["final_response"] = text_chunk

                            data = StreamUtil.create_content_chunk(text_chunk)
                            smss_stream(data, stream_type="content")
                            print(prefix + text_chunk, end="", flush=True)

                    elif "toolUse" in this_content_delta:
                        this_content_block["function"][
                            "arguments"
                        ] += this_content_delta["toolUse"]["input"]

                        data = StreamUtil.create_function_arguments_chunk(
                            index=len(tool_result),
                            arguments_chunk=this_content_delta["toolUse"]["input"],
                        )
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                elif "contentBlockStop" in event:
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

                    content_array.append(this_content_block)
                    this_content_block = {}
                    this_content_block_type = ""

                elif "messageStop" in event:
                    stop_reason = event["messageStop"]["stopReason"]

                if "metadata" in event:
                    metadata = event["metadata"]
                    if "usage" in metadata:
                        prompt_tokens = metadata["usage"]["inputTokens"]
                        output_tokens = metadata["usage"]["outputTokens"]

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
            for content in content_array:
                if content.get("final_response", None):
                    final_response += content.get("final_response")

            if self.has_schema:
                final_response = re.search(r"\{.*\}", final_response, re.DOTALL).group(
                    0
                )

            if tool_result:
                return AskModelEngineResponse(
                    response=tool_result,
                    response_tokens=output_tokens,
                    prompt_tokens=input_tokens,
                    messageType="TOOL",
                    schemaVersion=2,
                    io="OUTPUT",
                    parts=[{"type": "TOOL_CALL", "toolCalls": tool_result}],
                )
            else:
                return AskModelEngineResponse(
                    response=final_response,
                    response_tokens=output_tokens,
                    prompt_tokens=input_tokens,
                    messageType="CHAT",
                    schemaVersion=2,
                    io="OUTPUT",
                    parts=[{"type": "TEXT", "text": final_response}] if final_response else [],
                )

        except botocore.exceptions.BotoCoreError as e:
            raise RuntimeError(f"Failed to stream response from Bedrock: {e}")

    def _handle_non_streaming(self, request: Dict[str, Any]) -> AskModelEngineResponse:
        """Handle non-streaming responses from Bedrock"""
        try:
            response = self.client.converse(modelId=self.model_id, **request)

            output = response.get("output", {})
            message = output.get("message", {})
            content_list = message.get("content", [])

            tool_uses = []
            texts = []

            for content in content_list:
                if "toolUse" in content:
                    tool_use_block = content["toolUse"]
                    tool_uses.append(
                        {
                            "id": tool_use_block.get("toolUseId"),
                            "type": "function",
                            "name": tool_use_block.get("name"),
                            "arguments": tool_use_block.get("input", {}),
                        }
                    )
                elif "text" in content:
                    texts.append(content["text"])

            if tool_uses:
                final_response = tool_uses
                message_type = "TOOL"
            else:
                final_response = "\n".join(texts) if texts else ""
                message_type = "CHAT"

            if self.has_schema:
                final_response = re.search(r"\{.*\}", final_response, re.DOTALL).group(
                    0
                )

            return AskModelEngineResponse(
                response=final_response,
                prompt_tokens=response["usage"]["inputTokens"],
                response_tokens=response["usage"]["outputTokens"],
                messageType=message_type,
                schemaVersion=2,
                io="OUTPUT",
                parts=(
                    [{"type": "TOOL_CALL", "toolCalls": final_response}]
                    if message_type == "TOOL"
                    else ([{"type": "TEXT", "text": final_response}] if final_response else [])
                ),
            )

        except botocore.exceptions.BotoCoreError as e:
            raise RuntimeError(f"Failed to get response from Bedrock: {e}")
