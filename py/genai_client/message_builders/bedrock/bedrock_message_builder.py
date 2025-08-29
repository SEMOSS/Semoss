from typing import List, Dict, Any, Tuple, Union
import base64
from ...utils import (
    get_image_extension,
    fetch_and_encode_image,
)
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSImageContent,
    SEMOSSImageType,
)
from .bedrock_models import (
    BedrockMessage,
    BedrockImageBlock,
    BedrockImageSource,
    BedrockInferenceConfig,
    BedrockRequest,
    BedrockSystemBlock,
    BedrockTextContentBlock,
    BedrockImageContentBlock,
    BedrockToolUseContentBlock,
    BedrockToolResultContentBlock,
)


class BedrockMessageBuilder:
    def build_messages(
        self, semoss_messages: List[SEMOSSMessage], system_prompt: str = None
    ) -> Dict[str, Any]:
        """Convert SEMOSS messages to Bedrock format with enhanced tool support."""
        bedrock_messages = []
        param_map = {}
        tools = None
        stream = False
        system_block = None
        inference_config = None

        has_tool_content = any(
            (message.type == SEMOSSMessageType.RESPONSE_TOOL and message.tool_calls)
            or message.type == SEMOSSMessageType.INPUT_TOOL_EXEC
            for message in semoss_messages
        )

        if has_tool_content:
            for message in semoss_messages:
                if message.param_map.get("tools"):
                    tools = self._convert_mcp_to_bedrock_tools(
                        message.param_map["tools"]
                    )
                    break

            if not tools:
                tools = self._extract_tools_from_tool_calls(semoss_messages)

        i = 0
        while i < len(semoss_messages):
            message = semoss_messages[i]
            is_last = i == len(semoss_messages) - 1
            role = self._message_type_to_role(message.type)

            content_blocks = []

            if message.content and message.type != SEMOSSMessageType.INPUT_TOOL_EXEC:
                content_blocks.append(self._build_text_content_block(message.content))

            if message.image_content:
                image_blocks = self._build_image_blocks(message.image_content)
                content_blocks.extend(image_blocks)

            # Handle tool calls (RESPONSE_TOOL messages)
            if message.type == SEMOSSMessageType.RESPONSE_TOOL and message.tool_calls:
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

            if is_last:
                inference_config, param_map = self._build_request_parameters(
                    message.param_map
                )

                # Formatting the structured json input
                has_schema = param_map.pop("schema", False)
                if has_schema:
                    content = [self._build_text_content_block(str(has_schema))]

                    bedrock_messages.append(
                        BedrockMessage(
                            role=role,
                            content=content,
                        )
                    )

                last_message_tools = message.param_map.get("tools")
                if last_message_tools:
                    tools = self._convert_mcp_to_bedrock_tools(last_message_tools)

                stream = message.param_map.get("stream", False)
                system_block = self.build_system_block(system_prompt)
                param_map = self.clean_param_map(param_map)

            i += 1

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
        }

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

    def _build_tool_result_block(
        self, tool_use_id: str, tool_name: str, result_content: str
    ) -> BedrockToolResultContentBlock:
        """Build a tool result content block."""
        tool_result_data = {
            "toolUseId": tool_use_id,
            "content": [{"text": result_content}],
        }

        return BedrockToolResultContentBlock(toolResult=tool_result_data)

    def clean_param_map(self, param_map: Dict[str, Any]) -> Dict[str, Any]:
        """Remove parameters that shouldn't be passed to Bedrock."""
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
        param_map.pop("stream", None)
        param_map.pop("streaming", None)
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

    def _build_image_blocks(
        self, image_content: List[SEMOSSImageContent] = []
    ) -> List[BedrockImageContentBlock]:
        """Build image content blocks from SEMOSS image content."""
        bedrock_content_blocks = []
        for image in image_content:
            if image.type == SEMOSSImageType.URL:
                image_block = self._build_url_image_content(image)
                content_block = BedrockImageContentBlock(image=image_block)
                bedrock_content_blocks.append(content_block)
            elif image.type == SEMOSSImageType.BASE64:
                image_block = self._build_base64_image_content(image)
                content_block = BedrockImageContentBlock(image=image_block)
                bedrock_content_blocks.append(content_block)
            else:
                raise ValueError(f"Unsupported SEMOSS image type: {image.type}")

        return bedrock_content_blocks

    def _build_url_image_content(
        self, image_content: SEMOSSImageContent
    ) -> BedrockImageBlock:
        """Build a Bedrock image block from a URL."""
        img_bytes, media_type = fetch_and_encode_image(image_content.url)
        if media_type == "image/jpg":
            media_type = "image/jpeg"

        media_type = media_type.split("/")[-1].lower()

        try:
            img_bytes = base64.b64decode(img_bytes)
        except Exception as e:
            raise ValueError(f"Could not decode base64 image data: {e}")

        image_source = BedrockImageSource(bytes=img_bytes)
        return BedrockImageBlock(source=image_source, format=media_type)

    def _build_base64_image_content(
        self, image_content: SEMOSSImageContent
    ) -> BedrockImageBlock:
        """Build a Bedrock image block from base64 data."""
        if not image_content.data:
            raise ValueError("Base64 image content requires 'data' field.")

        if not image_content.mime_type:
            image_content.mime_type = get_image_extension(image_content.data)

        if image_content.mime_type == "image/jpg":
            image_content.mime_type = "image/jpeg"
        media_type = image_content.mime_type.split("/")[-1].lower()

        if image_content.data.startswith("data:"):
            base64_data = image_content.data.split(",")[1]
        else:
            base64_data = image_content.data

        try:
            image_bytes = base64.b64decode(base64_data)
        except Exception as e:
            raise ValueError(f"Could not decode base64 image data: {e}")

        image_source = BedrockImageSource(bytes=image_bytes)
        return BedrockImageBlock(source=image_source, format=media_type)

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

    def build_system_block(
        self, system_prompt: str = None
    ) -> Union[List[BedrockSystemBlock], None]:
        """Build a system content block."""
        if system_prompt:
            return [BedrockSystemBlock(text=system_prompt)]
        else:
            return None
