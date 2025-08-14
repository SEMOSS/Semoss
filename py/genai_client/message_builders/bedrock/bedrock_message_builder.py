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

        # Track tool execution state more carefully
        pending_tool_results = []
        expected_tool_count = 0

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

        for i, message in enumerate(semoss_messages):
            is_last = i == len(semoss_messages) - 1
            role = self._message_type_to_role(message.type)

            content_blocks = []

            if message.content:
                content_blocks.append(self._build_text_content_block(message.content))

            if message.image_content:
                image_blocks = self._build_image_blocks(message.image_content)
                content_blocks.extend(image_blocks)

            # Handle tool calls (RESPONSE_TOOL messages)
            if message.type == SEMOSSMessageType.RESPONSE_TOOL and message.tool_calls:
                expected_tool_count = len(message.tool_calls)

                for tool_call in message.tool_calls:
                    tool_use_block = self._build_tool_use_block(tool_call)
                    content_blocks.append(tool_use_block)

            # Handle tool execution results (INPUT_TOOL_EXEC messages)
            elif message.type == SEMOSSMessageType.INPUT_TOOL_EXEC:
                if expected_tool_count > 0:
                    tool_call_info = self._find_tool_call_info(
                        semoss_messages, i, message.tool_call_id
                    )

                    if tool_call_info:
                        tool_result_block = self._build_tool_result_block(
                            tool_call_info["id"],
                            tool_call_info["name"],
                            message.content,
                        )
                        pending_tool_results.append(tool_result_block)

                        if len(pending_tool_results) == expected_tool_count:
                            bedrock_messages.append(
                                BedrockMessage(
                                    role="user",
                                    content=pending_tool_results,
                                )
                            )
                            pending_tool_results = []
                            expected_tool_count = 0
                            continue

            if content_blocks:
                bedrock_messages.append(
                    BedrockMessage(
                        role=role,
                        content=content_blocks,
                    )
                )

            # Extract parameters from the last message
            if is_last:
                inference_config, param_map = self._build_request_parameters(
                    message.param_map
                )

                last_message_tools = message.param_map.get("tools")
                if last_message_tools:
                    tools = self._convert_mcp_to_bedrock_tools(last_message_tools)

                stream = message.param_map.get("stream", False)

                system_block = self.build_system_block(system_prompt)

                param_map = self.clean_param_map(param_map)

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

                        # Build properties from the arguments
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

    def _find_tools_from_messages(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Dict[str, Any]:
        """Find tools configuration from earlier messages if not present in the last message."""
        for message in reversed(semoss_messages):
            tools = message.param_map.get("tools")
            if tools:
                return self._convert_mcp_to_bedrock_tools(tools)

        return None

    def convert_mcp_to_bedrock_tools(self, mcp_tools: List[Dict]) -> Dict[str, Any]:
        """
        Convert MCP-formatted tools to Bedrock tool configuration format.
        Args:
            mcp_tools: List of tools in MCP format
        Returns:
            Bedrock tool configuration
        """
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

    def _find_tool_call_info(
        self,
        semoss_messages: List[SEMOSSMessage],
        current_index: int,
        tool_call_id: str,
    ) -> Dict[str, str]:
        """Find the tool call information by looking backwards through messages."""
        for j in range(current_index - 1, -1, -1):
            prev_msg = semoss_messages[j]
            if prev_msg.type == SEMOSSMessageType.RESPONSE_TOOL and prev_msg.tool_calls:
                for tool_call in prev_msg.tool_calls:
                    if str(tool_call.get("id")) == str(tool_call_id):
                        return {
                            "id": tool_call.get("id", ""),
                            "name": tool_call["function"]["name"],
                        }
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
        param_map.pop("tools", None)  # Remove tools from additional fields
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
