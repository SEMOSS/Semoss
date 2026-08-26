from typing import List, Dict, Any, Tuple, Union
import base64
import json
from ...utils import (
    get_image_extension,
    fetch_and_encode_image,
)
from ..semoss_base.builtin_tools import built_in_tool_names
from ..semoss_base.reasoning import normalize_reasoning
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSMessagePartType,
    SEMOSSMediaContent,
    SEMOSSMediaInputType,
    parse_multimodal_tool_response,
)
from .bedrock_models import (
    BedrockMessage,
    BedrockImageBlock,
    BedrockImageSource,
    BedrockDocumentBlock,
    BedrockDocumentSource,
    BedrockInferenceConfig,
    BedrockSystemBlock,
    BedrockTextContentBlock,
    BedrockImageContentBlock,
    BedrockDocumentContentBlock,
    BedrockToolUseContentBlock,
    BedrockToolResultContentBlock,
)


class BedrockMessageBuilder:
    def build_messages(self, semoss_messages: List[SEMOSSMessage]) -> Dict[str, Any]:
        """Convert SEMOSS messages to Bedrock format with enhanced tool support."""
        bedrock_messages = []
        param_map = {}
        tools = None
        stream = True
        has_schema = False
        system_block = None
        inference_config = None

        for i, message in enumerate(semoss_messages):
            is_last = i == len(semoss_messages) - 1
            content_blocks = []

            if message.parts:
                is_assistant = message.io != "INPUT"
                assistant_media_parts = []

                for p in message.parts:
                    if p.type == SEMOSSMessagePartType.TEXT:
                        content_blocks.append(self._build_text_content_block(p.text))

                    elif p.type == SEMOSSMessagePartType.MEDIA:
                        if is_assistant:
                            # Bedrock does not allow image blocks in assistant turns;
                            # add a text placeholder and queue the image for a synthetic user message.
                            file_name = getattr(p.media_info, "file_name", None) or "image"
                            content_blocks.append(
                                self._build_text_content_block(
                                    f"[Generated image: {file_name}]"
                                )
                            )
                            media_content = self._build_media_content_single_part(
                                p.media_info
                            )
                            assistant_media_parts.append(media_content)
                        else:
                            media_content = self._build_media_content_single_part(
                                p.media_info
                            )
                            content_blocks.append(media_content)

                    elif p.type == SEMOSSMessagePartType.TOOL_CALL:
                        tool_use_data = {
                            "toolUseId": p.tool_call.id or "",
                            "name": p.tool_call.function.name,
                            "input": p.tool_call.function.parameters,
                        }
                        tool_use_part = BedrockToolUseContentBlock(
                            toolUse=tool_use_data
                        )
                        content_blocks.append(tool_use_part)

                    elif p.type == SEMOSSMessagePartType.TOOL_RESULT:
                        output = p.tool_result.output or "Tool executed successfully."
                        blocks = parse_multimodal_tool_response(output)
                        tool_result_data = {
                            "toolUseId": p.tool_result.id,
                            "content": self._build_bedrock_tool_content(output, blocks),
                        }
                        tool_result_part = BedrockToolResultContentBlock(
                            toolResult=tool_result_data
                        )
                        content_blocks.append(tool_result_part)

                    elif p.type == SEMOSSMessagePartType.THINKING:
                        thinking_dict = {
                            "type": "thinking",
                            "thinking": p.thinking,
                        }
                        if self.thinking_signature:
                            thinking_dict["signature"] = self.thinking_signature
                        content_blocks.append(thinking_dict)

                bedrock_messages.append(
                    BedrockMessage(
                        role=("user" if message.io == "INPUT" else "assistant"),
                        content=content_blocks,
                    )
                )

                # Inject a synthetic user message with the images so the model can reference them
                if assistant_media_parts:
                    synthetic_content = [
                        self._build_text_content_block("Here is the generated image:")
                    ] + assistant_media_parts
                    bedrock_messages.append(
                        BedrockMessage(
                            role="user",
                            content=synthetic_content,
                        )
                    )

                # handle parameters update based on last message same as w/o parts
                if is_last:
                    system_prompt = message.param_map.pop("system_prompt", None)
                    if system_prompt:
                        system_block = self.build_system_block(system_prompt)
                    elif system_prompt is None and "instructions" in message.param_map:
                        instructions = message.param_map.pop("instructions")
                        system_block = self.build_system_block(instructions)
                    else:
                        system_block = None

                    inference_config, param_map = self._build_request_parameters(
                        message.param_map
                    )

                    # Formatting the structured json input
                    has_schema = param_map.get("schema", False)
                    if has_schema:
                        content = self._get_structured_parameters_format(**param_map)

                        bedrock_messages.append(
                            BedrockMessage(
                                role=role,
                                content=content,
                            )
                        )

                    last_message_tools = message.param_map.get("tools")
                    last_message_built_in_tools = message.param_map.pop("built_in_tools", None)
                    tool_choice = message.param_map.pop("tool_choice", None)
                    if last_message_tools:
                        mcp_tools = self._convert_mcp_to_bedrock_tools(
                            last_message_tools
                        )
                        if last_message_built_in_tools:
                            built_in = self._build_built_in_tools(last_message_built_in_tools)
                            mcp_tools["tools"].extend(built_in)
                        tools = self._build_tool_config_for_bedrock(
                            mcp_tools, tool_choice
                        )
                    elif last_message_built_in_tools:
                        built_in = self._build_built_in_tools(last_message_built_in_tools)
                        tools = self._build_tool_config_for_bedrock(
                            {"tools": built_in}, tool_choice
                        )

                    stream = message.param_map.get("stream", True)

                    param_map = self._apply_reasoning(inference_config, param_map)

            else:
                role = self._message_type_to_role(message.type)
                is_assistant = role == "assistant"
                assistant_media_blocks = []

                if (
                    message.content
                    and message.type != SEMOSSMessageType.INPUT_TOOL_EXEC
                ):
                    content_blocks.append(
                        self._build_text_content_block(message.content)
                    )

                if message.media_content:
                    if is_assistant:
                        # Bedrock does not allow image blocks in assistant turns;
                        # add a text placeholder and queue the images for a synthetic user message.
                        assistant_media_blocks = self._build_media_blocks(message.media_content)
                        for media in message.media_content:
                            file_name = getattr(media, "file_name", None) or "image"
                            content_blocks.append(
                                self._build_text_content_block(
                                    f"[Generated image: {file_name}]"
                                )
                            )
                    else:
                        media_blocks = self._build_media_blocks(message.media_content)
                        content_blocks.extend(media_blocks)

                # Handle tool calls (RESPONSE_TOOL messages)
                if (
                    message.type == SEMOSSMessageType.RESPONSE_TOOL
                    and message.tool_calls
                ):
                    for tool_call in message.tool_calls:
                        tool_use_block = self._build_tool_use_block(tool_call)
                        content_blocks.append(tool_use_block)

                    if content_blocks:
                        bedrock_messages.append(
                            BedrockMessage(
                                role=role,
                                content=content_blocks,
                            )
                        )

                    tool_call_ids = [tc.get("id") for tc in message.tool_calls]
                    tool_results, next_i = self._collect_tool_results(
                        semoss_messages, i + 1, tool_call_ids
                    )

                    if tool_results:
                        bedrock_messages.append(
                            BedrockMessage(
                                role="user",
                                content=tool_results,
                            )
                        )

                    i = next_i
                    continue

                elif message.type != SEMOSSMessageType.INPUT_TOOL_EXEC:
                    if content_blocks:
                        bedrock_messages.append(
                            BedrockMessage(
                                role=role,
                                content=content_blocks,
                            )
                        )

                    # Inject a synthetic user message with the images so the model can reference them
                    if is_assistant and message.media_content and assistant_media_blocks:
                        synthetic_content = [
                            self._build_text_content_block("Here is the generated image:")
                        ] + assistant_media_blocks
                        bedrock_messages.append(
                            BedrockMessage(
                                role="user",
                                content=synthetic_content,
                            )
                        )

                if is_last:
                    system_prompt = message.param_map.pop("system_prompt", None)
                    if system_prompt:
                        system_block = self.build_system_block(system_prompt)
                    elif system_prompt is None and "instructions" in message.param_map:
                        instructions = message.param_map.pop("instructions")
                        system_block = self.build_system_block(instructions)
                    else:
                        system_block = None

                    inference_config, param_map = self._build_request_parameters(
                        message.param_map
                    )

                    # Formatting the structured json input
                    has_schema = param_map.get("schema", False)
                    if has_schema:
                        content = self._get_structured_parameters_format(**param_map)

                        bedrock_messages.append(
                            BedrockMessage(
                                role=role,
                                content=content,
                            )
                        )

                    last_message_tools = message.param_map.get("tools")
                    last_message_built_in_tools = message.param_map.pop("built_in_tools", None)
                    tool_choice = message.param_map.pop("tool_choice", None)
                    if last_message_tools:
                        mcp_tools = self._convert_mcp_to_bedrock_tools(
                            last_message_tools
                        )
                        if last_message_built_in_tools:
                            built_in = self._build_built_in_tools(last_message_built_in_tools)
                            mcp_tools["tools"].extend(built_in)
                        tools = self._build_tool_config_for_bedrock(
                            mcp_tools, tool_choice
                        )
                    elif last_message_built_in_tools:
                        built_in = self._build_built_in_tools(last_message_built_in_tools)
                        tools = self._build_tool_config_for_bedrock(
                            {"tools": built_in}, tool_choice
                        )

                    stream = message.param_map.get("stream", True)

                    param_map = self._apply_reasoning(inference_config, param_map)

        messages_dict = [msg.model_dump(exclude_none=True) for msg in bedrock_messages]
        system_dict = (
            [block.model_dump(exclude_none=True) for block in system_block]
            if system_block
            else None
        )
        inference_config_dict = (
            inference_config.model_dump(exclude_none=True) if inference_config else None
        )

        return {
            "messages": messages_dict,
            "system": system_dict,
            "inferenceConfig": inference_config_dict,
            "toolConfig": tools,
            "additionalModelRequestFields": param_map,
            "stream": stream,
            "has_schema": has_schema,
        }

    def _get_structured_parameters_format(self, **param_map) -> Tuple[str, int, str]:
        """
        1. Validate the schema
        2. Create the structured json format
        """
        schema = param_map.pop("schema")
        # Validating the schema
        schema = self._validate_structured_input(schema)
        # Formatting as the user content form
        content = [self._build_text_content_block(schema)]

        return content

    def _validate_structured_input(self, schema) -> Tuple[str, Any]:
        """
        Validate the input schema for structured output.
        Returns the schema instance.
        Convert to Dict if JSON..
        """
        if isinstance(schema, str):
            # Attempting to parse as JSON
            try:
                return json.loads(schema)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON string provided for schema.")
        elif isinstance(schema, dict):
            # Validating that dict can be serialized to JSON
            try:
                return json.dumps(schema, ensure_ascii=False)
            except TypeError:
                raise ValueError("Schema dict contains non-serializable values.")
        else:
            raise ValueError("Schema must be a JSON string, dict.")

    def _group_tool_calls_and_results(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Dict[int, Dict]:
        """Pre-process messages to group tool calls with their results."""
        tool_groups = {}

        for i, message in enumerate(semoss_messages):
            if message.type == SEMOSSMessageType.RESPONSE_TOOL and message.tool_calls:
                tool_call_ids = [tc.get("id") for tc in message.tool_calls]
                tool_groups[i] = {
                    "tool_calls": message.tool_calls,
                    "tool_call_ids": tool_call_ids,
                    "results": [],
                }

                for j in range(i + 1, len(semoss_messages)):
                    result_msg = semoss_messages[j]
                    if result_msg.type == SEMOSSMessageType.INPUT_TOOL_EXEC:
                        if result_msg.tool_call_id in tool_call_ids:
                            tool_groups[i]["results"].append((j, result_msg))
                    elif result_msg.type != SEMOSSMessageType.INPUT_TOOL_EXEC:
                        break

        return tool_groups

    def _collect_tool_results(
        self,
        semoss_messages: List[SEMOSSMessage],
        start_index: int,
        expected_tool_call_ids: List[str],
    ) -> Tuple[List[BedrockToolResultContentBlock], int]:
        """Collect all tool results for the given tool call IDs."""
        tool_results = []
        current_index = start_index
        found_tool_call_ids = set()

        while current_index < len(semoss_messages):
            message = semoss_messages[current_index]

            if message.type == SEMOSSMessageType.INPUT_TOOL_EXEC:
                tool_call_id = message.tool_call_id

                if tool_call_id in expected_tool_call_ids:
                    tool_name = self._find_tool_name_by_id(
                        semoss_messages, tool_call_id
                    )

                    tool_result_block = self._build_tool_result_block(
                        tool_call_id,
                        tool_name or "unknown_tool",
                        message.content or "",
                    )
                    tool_results.append(tool_result_block)
                    found_tool_call_ids.add(tool_call_id)

                    if len(found_tool_call_ids) == len(expected_tool_call_ids):
                        break

                current_index += 1
            else:
                break

        return tool_results, current_index

    def _find_tool_name_by_id(
        self, semoss_messages: List[SEMOSSMessage], tool_call_id: str
    ) -> str:
        """Find the tool name for a given tool call ID."""
        for message in semoss_messages:
            if message.type == SEMOSSMessageType.RESPONSE_TOOL and message.tool_calls:
                for tool_call in message.tool_calls:
                    if str(tool_call.get("id")) == str(tool_call_id):
                        return tool_call["function"]["name"]
        return None

    def _extract_tools_from_tool_calls(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Dict[str, Any]:
        """Extract tool definitions from tool calls in the messages."""
        tools_dict = {}

        for message in semoss_messages:
            if message.type == SEMOSSMessageType.RESPONSE_TOOL and message.tool_calls:
                for tool_call in message.tool_calls:
                    tool_name = tool_call["function"]["name"]
                    if tool_name not in tools_dict:

                        arguments = tool_call["function"].get("arguments", {})
                        properties = {}
                        required = []

                        for key, value in arguments.items():
                            if isinstance(value, bool):
                                prop_type = "boolean"
                            elif isinstance(value, int):
                                prop_type = "integer"
                            elif isinstance(value, float):
                                prop_type = "number"
                            elif isinstance(value, list):
                                prop_type = "array"
                            elif isinstance(value, dict):
                                prop_type = "object"
                            else:
                                prop_type = "string"

                            properties[key] = {
                                "type": prop_type,
                                "description": f"Parameter {key}",
                            }
                            required.append(key)

                        tools_dict[tool_name] = {
                            "name": tool_name,
                            "description": f"Tool function {tool_name}",
                            "inputSchema": {
                                "type": "object",
                                "properties": properties,
                                "required": required,
                            },
                        }

        if tools_dict:
            return self._convert_mcp_to_bedrock_tools(list(tools_dict.values()))

        return None

    def _build_built_in_tools(self, built_in_tools: Any) -> List[Dict[str, Any]]:
        """Convert built-in tool selections to Bedrock systemTool format.
        Converse systemTools carry only a name - the catalog's Bedrock-hosted
        OpenAI web search (which does take params) runs through the OpenAI
        Responses client instead, so params are intentionally unused here."""
        return [
            {"systemTool": {"name": tool}}
            for tool in built_in_tool_names(built_in_tools)
        ]

    def _convert_mcp_to_bedrock_tools(self, mcp_tools: List[Dict]) -> Dict[str, Any]:
        """Convert MCP-formatted tools to Bedrock tool configuration."""
        tools_list = []

        for tool in mcp_tools:
            bedrock_tool = {
                "toolSpec": {
                    "name": tool["name"],
                    "description": tool["description"],
                    "inputSchema": {"json": tool["inputSchema"]},
                }
            }
            tools_list.append(bedrock_tool)

        return {
            "tools": tools_list,
            "toolChoice": {"auto": {}},
        }

    def _build_tool_use_block(
        self, tool_call: Dict[str, Any]
    ) -> BedrockToolUseContentBlock:
        """Build a tool use content block from a SEMOSS tool call."""
        tool_use_data = {
            "toolUseId": tool_call.get("id", ""),
            "name": tool_call["function"]["name"],
            "input": tool_call["function"]["arguments"],
        }

        return BedrockToolUseContentBlock(toolUse=tool_use_data)

    def _build_bedrock_tool_content(self, output: str, blocks) -> list:
        """Convert SEMOSS multimodal blocks to Bedrock tool result content array."""
        if blocks is None:
            return [{"text": output or "Tool executed successfully."}]
        result = []
        for b in blocks:
            if b.type == "text":
                result.append({"text": b.text})
            elif not b.data:
                continue  # unresolved file ref - Java should have inlined this
            else:
                try:
                    data_bytes = base64.b64decode(b.data)
                except (ValueError, TypeError):
                    continue  # malformed base64 - skip this block
                if b.type == "image":
                    fmt = (b.mime_type or "image/png").split("/")[-1]
                    if fmt == "jpg":
                        fmt = "jpeg"
                    result.append({"image": {"format": fmt,
                                             "source": {"bytes": data_bytes}}})
                else:
                    fmt = (b.mime_type or "application/pdf").split("/")[-1]
                    result.append({"document": {"format": fmt, "name": "document",
                                                "source": {"bytes": data_bytes}}})
        return result or [{"text": output or "Tool executed successfully."}]

    def _build_tool_result_block(
        self, tool_use_id: str, tool_name: str, result_content: str
    ) -> BedrockToolResultContentBlock:
        """Build a tool result content block."""
        blocks = parse_multimodal_tool_response(result_content)
        tool_result_data = {
            "toolUseId": tool_use_id,
            "content": self._build_bedrock_tool_content(result_content, blocks),
        }

        return BedrockToolResultContentBlock(toolResult=tool_result_data)

    def clean_param_map(self, param_map: Dict[str, Any]) -> Dict[str, Any]:
        """Remove parameters that shouldn't be passed to Bedrock."""
        # CODEX SPECIFIC HANDLING
        instructions = param_map.pop("instructions", None)
        include = param_map.pop("include", None)
        parallel_tool_calls = param_map.pop("parallel_tool_calls", None)
        store = param_map.pop("store", None)
        prompt_cache_key = param_map.pop("prompt_cache_key", None)
        # END CODEX SPECIFIC HANDLING

        param_map.pop("max_completion_tokens", None)
        param_map.pop("max_tokens", None)
        param_map.pop("max_new_tokens", None)
        param_map.pop("model_name", None)
        param_map.pop("history", None)
        param_map.pop("use_history", None)
        param_map.pop("context", None)
        param_map.pop("image_url", None)
        param_map.pop("image_encoded", None)
        param_map.pop("tools", None)
        param_map.pop("built_in_tools", None)
        param_map.pop("stream", None)
        param_map.pop("streaming", None)
        param_map.pop("schema", None)
        # reasoning keys are handled via reasoning_config; never pass them raw
        param_map.pop("thinking", None)
        param_map.pop("thinking_budget", None)
        param_map.pop("effort", None)
        return param_map

    def _message_type_to_role(self, message_type: SEMOSSMessageType) -> str:
        """Convert SEMOSS message type to Bedrock role."""
        user_message_types = [
            SEMOSSMessageType.INPUT_TEXT,
            SEMOSSMessageType.INPUT_MEDIA,
            SEMOSSMessageType.INPUT_TOOL_EXEC,
        ]
        assistant_message_types = [
            SEMOSSMessageType.RESPONSE_TEXT,
            SEMOSSMessageType.RESPONSE_MEDIA,
            SEMOSSMessageType.RESPONSE_TOOL,
        ]
        if message_type in user_message_types:
            return "user"
        elif message_type in assistant_message_types:
            return "assistant"
        else:
            raise ValueError(f"Unknown SEMOSS message type: {message_type}")

    def _build_text_content_block(self, content: str) -> BedrockTextContentBlock:
        """Build a text content block."""
        return BedrockTextContentBlock(text=content)

    def _build_media_blocks(
        self, media_content: List[SEMOSSMediaContent] = []
    ) -> List[Union[BedrockImageContentBlock, BedrockDocumentContentBlock]]:
        """Build media content blocks from SEMOSS media content."""
        bedrock_content_blocks = []
        for media in media_content:
            bedrock_content_blocks.append(self._build_media_content_single_part(media))

        return bedrock_content_blocks

    def _build_media_content_single_part(
        self, media: SEMOSSMediaContent = []
    ) -> Union[BedrockImageContentBlock, BedrockDocumentContentBlock]:
        """Build media content block from SEMOSS media content."""
        if media.type == SEMOSSMediaInputType.URL:
            return self._build_url_media_content(media)
        elif media.type == SEMOSSMediaInputType.BASE64:
            return self._build_base64_media_content(media)
        else:
            raise ValueError(f"Unsupported SEMOSS media type: {media.type}")

    def _build_url_media_content(
        self, media_content: SEMOSSMediaContent
    ) -> BedrockImageContentBlock:
        """Build a Bedrock media block from a URL."""

        # TODO: this utility methods needs to be expanded for non-images
        image_bytes, media_type = fetch_and_encode_image(media_content.url)
        if media_type == "image/jpg":
            media_type = "image/jpeg"

        media_type = media_type.split("/")[-1].lower()

        try:
            decoded_bytes = base64.b64decode(image_bytes)
        except Exception as e:
            raise ValueError(f"Could not decode base64 image data: {e}")

        if media_type.startswith("image"):
            media_source = BedrockImageSource(bytes=decoded_bytes)
            block = BedrockImageBlock(source=media_source, format=media_type)
            return BedrockImageContentBlock(image=block)
        else:
            media_source = BedrockDocumentSource(bytes=decoded_bytes)
            # TODO: should add into fetch_and_encode to predict filename from url
            block = BedrockDocumentBlock(
                source=media_source, format=media_type, name=media_content.file_name
            )
            return BedrockDocumentContentBlock(document=block)

    def _build_base64_media_content(
        self, media_content: SEMOSSMediaContent
    ) -> Union[BedrockImageContentBlock, BedrockDocumentContentBlock]:
        """Build a Bedrock media block from base64 data."""
        if not media_content.data:
            raise ValueError("Base64 media content requires 'data' field.")

        if not media_content.mime_type:
            media_content.mime_type = get_image_extension(media_content.data)

        if media_content.mime_type == "image/jpg":
            media_content.mime_type = "image/jpeg"
        media_type = media_content.mime_type.split("/")[-1].lower()

        if media_content.data.startswith("data:"):
            base64_data = media_content.data.split(",")[1]
        else:
            base64_data = media_content.data

        try:
            decoded_bytes = base64.b64decode(base64_data)
        except Exception as e:
            raise ValueError(f"Could not decode base64 media data: {e}")

        if media_content.mime_type.startswith("image"):
            media_source = BedrockImageSource(bytes=decoded_bytes)
            block = BedrockImageBlock(source=media_source, format=media_type)
            return BedrockImageContentBlock(image=block)
        else:
            print(f"Original filename: {repr(media_content.file_name)}")

            media_source = BedrockDocumentSource(bytes=decoded_bytes)
            block = BedrockDocumentBlock(
                source=media_source, format=media_type, name=media_content.file_name
            )

            # Debug after validation
            print(f"Cleaned filename: {repr(block.name)}")

            return BedrockDocumentContentBlock(document=block)

    def _build_request_parameters(
        self, param_map: Dict[str, Any]
    ) -> Tuple[BedrockInferenceConfig, Dict[str, Any]]:
        """Build inference configuration and remaining parameters."""
        max_tokens = (
            param_map.pop("max_tokens", None)
            or param_map.pop("max_completion_tokens", None)
            or param_map.pop("max_new_tokens", None)
        )
        stop_sequences = param_map.pop("stop_sequences", None) or param_map.pop(
            "stop_sequence",
            None,
        )
        temperature = param_map.pop("temperature", None)
        top_p = param_map.pop("top_p", None) or param_map.pop(
            "topP",
            None,
        )

        return (
            BedrockInferenceConfig(
                maxTokens=max_tokens,
                stopSequences=stop_sequences,
                temperature=temperature,
                topP=top_p,
            ),
            param_map,
        )

    def _resolve_reasoning_config(
        self, param_map: Dict[str, Any]
    ) -> Dict[str, Any] | None:
        """Bedrock Converse reasoning (Anthropic Claude on Bedrock). Converse is
        budget-based — it has no effort knob — so the canonical effort collapses
        to a token budget. The builder is stateless (no model_settings), so this
        resolves from the param map only. normalize_reasoning pops the
        thinking/effort/thinking_budget keys so they don't leak into
        additionalModelRequestFields.
        """
        resolved = normalize_reasoning(param_map)
        if resolved is None:
            return None
        return {"type": "enabled", "budget_tokens": resolved.budget}

    def _apply_reasoning(
        self, inference_config: "BedrockInferenceConfig", param_map: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Resolve reasoning, clean the param map, then re-attach reasoning_config
        and reconcile the inference config (maxTokens must exceed the budget;
        sampling params are incompatible with reasoning)."""
        reasoning_config = self._resolve_reasoning_config(param_map)
        param_map = self.clean_param_map(param_map)
        if reasoning_config:
            param_map["reasoning_config"] = reasoning_config
            budget = reasoning_config.get("budget_tokens", 0)
            if inference_config.maxTokens is None or inference_config.maxTokens <= budget:
                inference_config.maxTokens = budget + 4096
            inference_config.temperature = None
            inference_config.topP = None
        return param_map

    def build_system_block(
        self, system_prompt: str = None
    ) -> Union[List[BedrockSystemBlock], None]:
        """Build a system content block."""
        if system_prompt:
            return [BedrockSystemBlock(text=system_prompt)]
        else:
            return None

    def _build_tool_config_for_bedrock(
        self, mcp_tools: List[Dict], tool_choice: Dict[str, str] | None
    ) -> Dict[str, Any] | None:
        """
        Map SEMOSS tool_choice -> Bedrock toolConfig.
        SEMOSS: [auto, required, forced, none]
        Bedrock: toolChoice is a UNION of [auto, any, tool]; omit toolConfig for 'none'.
        But tool is only supported on Anthropic Claude 3 / Amazon Nova so I'm not honoring it here.
        """
        # defaulting to auto
        choice = (tool_choice or {}).get("type", "auto").lower()

        # If none don't return toolConfig
        if choice == "none":
            return None

        tools_list = mcp_tools.get("tools", []) if isinstance(mcp_tools, dict) else []

        if choice == "auto":
            tool_choice_obj = {"auto": {}}
        elif choice == "required" or choice == "forced":
            tool_choice_obj = {"any": {}}
        else:
            tool_choice_obj = {"auto": {}}

        return {"tools": tools_list, "toolChoice": tool_choice_obj}
