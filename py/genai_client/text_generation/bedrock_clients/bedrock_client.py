from typing import Dict, Any, TYPE_CHECKING, Optional, Tuple

if TYPE_CHECKING:

    from mypy_boto3_bedrock_runtime import BedrockRuntimeClient

    def smss_stream(
        data: Any, stream_type: str = "content", interim: bool = True
    ) -> None: ...


import json, boto3, re
from smss_thread_local import get_smss_stream
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil
from ...message_builders.bedrock.bedrock_message_builder import BedrockMessageBuilder
from ...constants import AskModelEngineResponse2
from ..model_engine_exception import ModelEngineException, ErrorDetails
from ...usage_ceiling import (
    PromptCount,
    count_prompt_tokens,
    fit_thinking_budget,
    note_output_stop,
)
from botocore.config import Config
from botocore.exceptions import ClientError


class BedrockClient(AbstractTextGenerationClient):
    client: "BedrockRuntimeClient"

    # Claude's smallest extended thinking budget on Bedrock
    MIN_THINKING_BUDGET = 1024

    # CountTokens errors that mean it does not work for this model or account
    _COUNT_TOKENS_UNSUPPORTED_CODES = {
        "ValidationException",
        "AccessDeniedException",
        "ResourceNotFoundException",
        "UnknownOperationException",
    }

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
        retry_config: Dict[str, Any] = None,
        **kwargs,
    ):
        init_params = {
            "model_name": modelId,
            **kwargs,
        }
        super().__init__(template=template, template_name=template_name, **init_params)
        self.kwargs = kwargs
        self.model_id = modelId

        self.client = self._create_client(
            region,
            access_key,
            secret_key,
            service_name,
            retry_config=Config(retries=retry_config),
        )

        self.guardrail_config = self._create_guardrail_config(
            guardrail_identifier, guardrail_version
        )

    def _create_client(
        self,
        region: str,
        access_key: str = None,
        secret_key: str = None,
        service_name: str = None,
        retry_config: Config = None,
    ):
        """Create a boto3 client for Bedrock with appropriate authentication."""
        if access_key and secret_key:
            return boto3.client(
                service_name=service_name,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
                config=retry_config,
            )
        else:
            return boto3.client(
                service_name=service_name,
                region_name=region,
                config=retry_config,
            )

    def _create_guardrail_config(
        self, guardrail_identifier: str, guardrail_version: str
    ) -> Dict[str, Any] | None:
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
    ) -> AskModelEngineResponse2 | ErrorDetails:
        """Entry point for making Bedrock ask calls."""
        if self.client is None:
            raise RuntimeError("Bedrock client is not initialized.")
        try:
            semoss_messages = self.build_semoss_messages(
                model_settings=self.model_settings, **kwargs
            )

            try:
                bedrock_request = BedrockMessageBuilder().build_messages(
                    semoss_messages
                )

                if (
                    hasattr(self.model_settings, "global_param_override")
                    and self.model_settings.global_param_override
                ):
                    bedrock_request.update(self.model_settings.global_param_override)

                stream = bedrock_request.pop("stream", True)
                self.has_schema = bedrock_request.pop("has_schema", False)
                bedrock_request["guardrailConfig"] = self.guardrail_config

                bedrock_request = {
                    k: v for k, v in bedrock_request.items() if v is not None
                }
            except Exception as e:
                raise ValueError(f"Error building Bedrock messages: {str(e)}") from e

            # fitted after the global parameter override, so the maximum fitted is
            # the one sent; a refusal becomes the USAGE_LIMIT_DOES_NOT_FIT error
            # response
            self._apply_usage_ceiling(bedrock_request)

            if stream:
                return self._handle_streaming(bedrock_request, prefix=prefix)
            return self._handle_non_streaming(bedrock_request)
        except Exception as e:
            return ModelEngineException(
                error=e, client="bedrock", model=self.model_id
            ).parse_error()

    def _apply_usage_ceiling(self, request: Dict[str, Any]) -> None:
        """Fit the Converse request to the usage ceiling of the call in progress.

        inferenceConfig.maxTokens is lowered to what the prompt leaves, never
        raised. A reasoning budget is lowered below it, leaving part of it for
        the answer, and reasoning is turned off when even 1,024 tokens do not
        fit.
        """
        # copies are changed, since these dicts may be the client's own
        # global_param_override values, which later calls reuse
        inference = dict(request.get("inferenceConfig") or {})
        fit = self.fit_output_to_usage_ceiling(
            inference.get("maxTokens"), lambda: self._count_prompt_tokens(request)
        )
        if fit is None or not fit.lowered:
            return
        inference["maxTokens"] = fit.max_tokens
        request["inferenceConfig"] = inference

        fields = request.get("additionalModelRequestFields")
        if not isinstance(fields, dict):
            return
        # the reasoning field's name differs by model; thinking counts inside
        # maxTokens, so its budget follows the lowered maximum
        for key in ("reasoning_config", "thinking"):
            reasoning = fields.get(key)
            if not isinstance(reasoning, dict) or reasoning.get("type") != "enabled":
                continue
            budget = fit_thinking_budget(
                int(reasoning.get("budget_tokens") or self.MIN_THINKING_BUDGET),
                fit.max_tokens,
                self.usage_ceiling_min_output_tokens(),
                self.MIN_THINKING_BUDGET,
            )
            fields = dict(fields)
            # too small a budget runs the call without reasoning
            if budget is None:
                fields.pop(key)
            else:
                fields[key] = {**reasoning, "budget_tokens": budget}
            request["additionalModelRequestFields"] = fields

    def _count_prompt_tokens(self, request: Dict[str, Any]) -> PromptCount:
        """Count the prompt with the Bedrock CountTokens API where the model
        supports it, and estimate it otherwise or when the count fails."""
        if not getattr(self, "_count_tokens_unavailable", False):
            converse = {
                key: request[key]
                for key in (
                    "messages",
                    "system",
                    "toolConfig",
                    "additionalModelRequestFields",
                )
                if request.get(key)
            }
            try:
                result = self.client.count_tokens(
                    modelId=self.model_id, input={"converse": converse}
                )
                return PromptCount(tokens=int(result["inputTokens"]), estimated=False)
            except ClientError as e:
                # remembered for the client, so a model without CountTokens is
                # not asked again on every call
                code = e.response.get("Error", {}).get("Code")
                if code in self._COUNT_TOKENS_UNSUPPORTED_CODES:
                    self._count_tokens_unavailable = True
            except AttributeError:
                # a boto3 release without CountTokens
                self._count_tokens_unavailable = True
            except Exception:
                # a passing failure: this call is estimated, the next one counts
                pass

        messages = request.get("messages") or []
        return count_prompt_tokens(
            [request.get("system"), messages],
            [request.get("toolConfig")],
            message_count=len(messages),
        )

    @staticmethod
    def _normalize_usage(
        usage: Optional[Dict[str, Any]],
    ) -> Tuple[int, int, Optional[int], Optional[int], Optional[int]]:
        """Converse usage as (prompt, response, cache read, cache write,
        thinking) tokens, counted like the other clients.

        Converse reports inputTokens without the tokens read from or written
        to the prompt cache (totalTokens is inputTokens + outputTokens +
        cacheReadInputTokens + cacheWriteInputTokens), so the prompt count adds
        them back. outputTokens already includes reasoning tokens.
        """
        usage = usage or {}
        cache_read_tokens = usage.get("cacheReadInputTokens") or None
        cache_creation_tokens = usage.get("cacheWriteInputTokens") or None
        prompt_tokens = (
            (usage.get("inputTokens") or 0)
            + (cache_read_tokens or 0)
            + (cache_creation_tokens or 0)
        )
        return (
            prompt_tokens,
            usage.get("outputTokens") or 0,
            cache_read_tokens,
            cache_creation_tokens,
            usage.get("thinkingTokens") or None,
        )

    def _handle_streaming(
        self, request: Dict[str, Any], prefix: str = ""
    ) -> AskModelEngineResponse2:
        """Handle streaming responses from Bedrock."""
        smss_stream = get_smss_stream()
        stream_response = self.client.converse_stream(modelId=self.model_id, **request)

        prompt_tokens = 0
        output_tokens = 0
        # Bedrock outputTokens is total billed (thinking already included). thinkingTokens is observational.
        # prompt_tokens includes cache reads and writes; see _normalize_usage.
        cache_read_tokens: Optional[int] = None
        cache_creation_tokens: Optional[int] = None
        thinking_tokens: Optional[int] = None

        content_array = []
        this_content_block: Dict[str, Any] = {}
        this_content_block_type = ""

        tool_result = []

        for event in stream_response.get("stream", []):
            if "messageStart" in event:
                continue

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
                    this_content_block["function"]["name"] = tool_use_content["name"]

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

            elif "contentBlockDelta" in event:
                this_content_delta = event["contentBlockDelta"]["delta"]

                if "text" in this_content_delta:
                    text_chunk = this_content_delta["text"]
                    if text_chunk is not None:
                        this_content_block["type"] = "text"
                        this_content_block["final_response"] = (
                            this_content_block.get("final_response", "") + text_chunk
                        )

                        data = StreamUtil.create_content_chunk(text_chunk)
                        smss_stream(data, stream_type="content")
                        print(prefix + text_chunk, end="", flush=True)

                elif "toolUse" in this_content_delta:
                    this_content_block["function"]["arguments"] += this_content_delta[
                        "toolUse"
                    ]["input"]

                    data = StreamUtil.create_function_arguments_chunk(
                        index=len(tool_result),
                        arguments_chunk=this_content_delta["toolUse"]["input"],
                    )
                    smss_stream(data, stream_type="tool")
                    print(prefix + str(data), end="")

            elif "contentBlockStop" in event:
                if this_content_block_type == "tool_use":
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

            if "messageStop" in event:
                note_output_stop(event["messageStop"].get("stopReason") == "max_tokens")

            if "metadata" in event:
                metadata = event["metadata"]
                if "usage" in metadata:
                    (
                        prompt_tokens,
                        output_tokens,
                        cache_read_tokens,
                        cache_creation_tokens,
                        thinking_tokens,
                    ) = self._normalize_usage(metadata["usage"])

        if tool_result:
            data = StreamUtil.create_finish_reason_chunk("tool_use")
            smss_stream(data, stream_type="tool", interim=False)
        else:
            data = StreamUtil.create_finish_reason_chunk("stop")
            smss_stream(data, stream_type="content", interim=False)

        final_response = ""
        for content in content_array:
            if content.get("final_response", None):
                final_response += content.get("final_response")

        if self.has_schema and isinstance(final_response, str):
            try:
                final_response = re.search(r"\{.*\}", final_response, re.DOTALL).group(
                    0
                )
            except Exception:
                pass

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
                            "output": json.dumps(tool_content, ensure_ascii=False),
                        },
                    }
                )

        # Don't forget to flush any remaining text at the end
        if current_text_block is not None:
            parts.append(current_text_block)

        if tool_result:
            return AskModelEngineResponse2(
                response=tool_result,
                response_tokens=output_tokens,
                prompt_tokens=prompt_tokens,
                cache_read_tokens=cache_read_tokens,
                cache_creation_tokens=cache_creation_tokens,
                thinking_tokens=thinking_tokens,
                schemaVersion=2,
                io="OUTPUT",
                parts=parts,
                messageType="TOOL",
            )

        return AskModelEngineResponse2(
            response=final_response,
            response_tokens=output_tokens,
            prompt_tokens=prompt_tokens,
            cache_read_tokens=cache_read_tokens,
            cache_creation_tokens=cache_creation_tokens,
            thinking_tokens=thinking_tokens,
            schemaVersion=2,
            io="OUTPUT",
            parts=parts,
            messageType="CHAT",
        )

    def _handle_non_streaming(self, request: Dict[str, Any]) -> AskModelEngineResponse2:
        """Handle non-streaming responses from Bedrock"""
        response = self.client.converse(modelId=self.model_id, **request)
        note_output_stop(response.get("stopReason") == "max_tokens")

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
            final_response: Any = tool_uses
            message_type = "TOOL"
        else:
            final_response = "\n".join(texts) if texts else ""
            message_type = "CHAT"

        if self.has_schema and isinstance(final_response, str):
            try:
                final_response = re.search(r"\{.*\}", final_response, re.DOTALL).group(
                    0
                )
            except Exception:
                pass

        parts = []
        text_combined = "\n".join(texts) if texts else ""
        if text_combined:
            parts.append({"type": "TEXT", "text": text_combined})
        if tool_uses:
            for t in tool_uses:
                parts.append({"type": "TOOL_CALL", "tool_call": t})
        (
            prompt_tokens,
            response_tokens,
            cache_read_tokens,
            cache_creation_tokens,
            thinking_tokens,
        ) = self._normalize_usage(response.get("usage"))
        return AskModelEngineResponse2(
            response=final_response,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            cache_read_tokens=cache_read_tokens,
            cache_creation_tokens=cache_creation_tokens,
            thinking_tokens=thinking_tokens,
            messageType=message_type,
            schemaVersion=2,
            io="OUTPUT",
            parts=parts,
        )
