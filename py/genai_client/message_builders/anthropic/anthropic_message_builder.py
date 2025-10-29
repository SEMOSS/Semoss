from typing import List, Dict, Any, Tuple, Union
import json
from ...utils import (
    get_image_extension,
    fetch_and_encode_image,
)
from .anthropic_models import (
    AnthropicRoles,
    AnthropicMessage,
    AnthropicImageSourceBase64,
    AnthropicImageContentPart,
    AnthropicTextContentPart,
    AnthropicToolUseContentPart,
    AnthropicToolResultContentPart,
    AnthropicRequestConfig,
    AnthropicMessageBuilderResponse,
)
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSImageContent,
    SEMOSSImageType,
)
from ...text_generation.abstract_text_generation_client import ModelLimits


class AnthropicMessageBuilder:

    def build_messages(
        self,
        semoss_messages: List[SEMOSSMessage],
        model_limits: ModelLimits,
        model_name: str,
        use_beta_header: bool = False,
        beta_feature_name: str = "extended_thinking",
    ) -> AnthropicMessageBuilderResponse:
        """Convert SEMOSS messages to Anthropic messages and return the param map from the latest message"""
        self.model_limits = model_limits
        self.model_name = model_name
        self.use_beta_header = use_beta_header
        self.beta_feature_name = beta_feature_name
        anthropic_messages = []
        param_map = {}

        pending_tool_calls = []
        pending_tool_results = []

        streaming = True
        has_schema = False

        for i, message in enumerate(semoss_messages):
            is_last = i == len(semoss_messages) - 1
            content_parts = []

            if (
                message.type == SEMOSSMessageType.INPUT_TEXT
                or message.type == SEMOSSMessageType.INPUT_MEDIA
            ):
                if message.content:
                    content_parts.append(self._build_text_content_part(message.content))

                if message.image_content:
                    image_contents_parts = self._build_image_content_part(
                        message.image_content
                    )
                    content_parts.extend(image_contents_parts)

                anthropic_messages.append(
                    AnthropicMessage(
                        role=AnthropicRoles.USER,
                        content=content_parts,
                    )
                )

            elif message.type == SEMOSSMessageType.RESPONSE_TOOL:
                # Handle assistant tool calls
                if message.tool_calls:
                    for tool_call in message.tool_calls:
                        tool_use_part = AnthropicToolUseContentPart(
                            id=tool_call["id"],
                            name=tool_call["function"]["name"],
                            input=tool_call["function"]["arguments"],
                        )
                        content_parts.append(tool_use_part)
                        # Track this tool call as pending
                        pending_tool_calls.append(tool_call["id"])

                    anthropic_messages.append(
                        AnthropicMessage(
                            role=AnthropicRoles.ASSISTANT,
                            content=content_parts,
                        )
                    )

            elif message.type == SEMOSSMessageType.INPUT_TOOL_EXEC:
                # Handle tool execution results
                if message.tool_call_id:
                    tool_result = AnthropicToolResultContentPart(
                        tool_use_id=message.tool_call_id,
                        content=message.content,
                    )
                    pending_tool_results.append(tool_result)

                    if message.tool_call_id in pending_tool_calls:
                        pending_tool_calls.remove(message.tool_call_id)

                # Check if we have all tool results for pending tool calls
                # or if this is the last message or next message is not INPUT_TOOL_EXEC
                should_flush = (
                    len(pending_tool_calls) == 0  # All tool calls have results
                    or is_last  # This is the last message
                    or (
                        i + 1 < len(semoss_messages)
                        and semoss_messages[i + 1].type
                        != SEMOSSMessageType.INPUT_TOOL_EXEC
                    )  # Next message is not tool exec
                )

                if should_flush and pending_tool_results:
                    anthropic_messages.append(
                        AnthropicMessage(
                            role=AnthropicRoles.USER,
                            content=pending_tool_results.copy(),
                        )
                    )
                    pending_tool_results.clear()
                    pending_tool_calls.clear()

            elif message.type == SEMOSSMessageType.RESPONSE_TEXT:
                if message.content:
                    content_parts.append(self._build_text_content_part(message.content))

                anthropic_messages.append(
                    AnthropicMessage(
                        role=AnthropicRoles.ASSISTANT,
                        content=content_parts,
                    )
                )

            if is_last:
                param_map = message.param_map

                # Formatting the structured json input
                schema = param_map.pop("schema", False)
                if schema:
                    schema_tool = self._get_structured_parameters_format(schema)
                    has_schema = True

                    if "tools" in param_map:
                        param_map["tools"].append(schema_tool)
                    else:
                        param_map["tools"] = [schema_tool]

                if "tools" in param_map:
                    param_map["tools"] = self._convert_mcp_to_anthropic_tools(
                        param_map["tools"]
                    )
                if "tool_choice" in param_map:
                    param_map["tool_choice"] = self._build_tool_choice(
                        param_map["tool_choice"]
                    )

        if "streaming" in param_map:
            streaming = param_map.pop("streaming", None)
        if streaming is None and "stream" in param_map:
            streaming = param_map.pop("stream", None)
        if streaming is None:
            streaming = True

        request_config = self._convert_args_to_provider_config(
            history=anthropic_messages, **param_map
        )

        return AnthropicMessageBuilderResponse(
            request_config=request_config,
            streaming=streaming,
            has_structured_input=has_schema,
        )

    def _build_tool_choice(
        self, tool_choice: Dict[str, str]
    ) -> Union[Dict[str, str], None]:
        """
        Build the tool choice dictionary for Anthropic
        SEMOSS tool_type options [auto, required, forced, none]
        Anthropic type options [auto, any, tool, none]
        Anthropic types of any and tool are not available with extended thinking
        """
        tool_type = tool_choice.get("type", "auto").lower()
        tool_name = tool_choice.get("name", None)
        if tool_type == "auto":
            return {"type": "auto"}
        elif tool_type == "required":
            return {"type": "any"}
        elif tool_type == "forced" and tool_name:
            return {"type": "tool", "name": tool_name}
        elif tool_type == "none":
            return {"type": "none"}
        else:
            return None

    def _get_structured_parameters_format(self, schema) -> Tuple[str, int, str]:
        """
        1. Validate the schema
        2. Create the structured json format
        """
        # Validating the schema
        schema = self._validate_structured_input(schema)
        # Formatting as the user content form
        tool = self._schema_to_anthropic_tool(
            schema,
            name="return_json",
            description="Return JSON matching the requested schema.",
        )

        return tool

    def _schema_to_anthropic_tool(
        self, schema, name: str, description: str
    ) -> Dict[str, Any]:
        """
        Wrap a JSON schema as an Anthropic tool for structured output.
        Accepts schema as dict or JSON string.
        """
        # Normalize to dict
        if isinstance(schema, str):
            try:
                schema_dict = json.loads(schema)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON string provided for schema.")
        elif isinstance(schema, dict):
            schema_dict = schema
        else:
            raise ValueError("Schema must be a JSON string or dict.")

        # Minimal validation
        if schema_dict.get("type") != "object":
            raise ValueError("Top-level schema must be an object.")

        return {
            "name": name,
            "description": description,
            "inputSchema": schema_dict,  # Anthropic expects pure JSON Schema here
        }

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

    def _message_type_to_role(self, message_type: SEMOSSMessageType) -> AnthropicRoles:
        """Convert SEMOSS message type to Anthropic role."""
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
            return AnthropicRoles.USER
        elif message_type in assistant_message_types:
            return AnthropicRoles.ASSISTANT
        else:
            raise ValueError(f"Unknown message type: {message_type}")

    def _build_text_content_part(self, content: str) -> AnthropicTextContentPart:
        """Build Anthropic text content part"""
        return AnthropicTextContentPart(text=content)

    def _build_image_content_part(
        self, image_content: List[SEMOSSImageContent] = []
    ) -> List[AnthropicImageContentPart]:
        """Build Anthropic image content parts from SEMOSS image content."""

        anthropic_image_parts = []
        for image in image_content:
            if image.type == SEMOSSImageType.URL:
                anthropic_image_parts.append(self._build_url_image_content(image))
            elif image.type == SEMOSSImageType.BASE64:
                anthropic_image_parts.append(self._build_base64_image_content(image))
            else:
                raise ValueError(f"Unknown image type: {image.type}")

        return anthropic_image_parts

    def _build_url_image_content(
        self, image_content: SEMOSSImageContent
    ) -> AnthropicImageContentPart:
        """Build Anthropic image content part from URL as base64"""
        if not image_content.url:
            raise ValueError(
                "The image type was specified as URL but no URL was provided.."
            )
        image_data, media_type = fetch_and_encode_image(image_content.url)
        if media_type == "image/jpg":
            media_type = "image/jpeg"

        image_source = AnthropicImageSourceBase64(
            media_type=media_type,
            data=image_data,
        )

        return AnthropicImageContentPart(source=image_source)

    def _build_base64_image_content(
        self, image_content: SEMOSSImageContent
    ) -> AnthropicImageContentPart:
        """Build Anthropic image content part from base64"""
        if not image_content.data:
            raise ValueError(
                "The image type was specified as base64 but no data was provided."
            )

        if not image_content.mime_type:
            image_content.mime_type = get_image_extension(image_content.data)

        if image_content.mime_type == "image/jpg":
            image_content.mime_type = "image/jpeg"

        image_source = AnthropicImageSourceBase64(
            media_type=image_content.mime_type,
            data=image_content.data,
        )

        return AnthropicImageContentPart(source=image_source)

    def _convert_mcp_to_anthropic_tools(self, mcp_tools: List[Dict]) -> List[Dict]:
        """
        Convert MCP-formatted tools to Anthropic tool format.
        """
        anthropic_tools = []

        for tool in mcp_tools:
            anthropic_tool = {
                "name": tool["name"],
                "description": tool["description"],
                "input_schema": {
                    "type": tool["inputSchema"]["type"],
                    "properties": {},
                    "required": tool["inputSchema"].get("required", []),
                },
            }

            for prop_name, prop_def in tool["inputSchema"]["properties"].items():
                anthropic_tool["input_schema"]["properties"][prop_name] = {
                    k: v for k, v in prop_def.items() if k != "title"
                }

            anthropic_tools.append(anthropic_tool)

        return anthropic_tools

    def _convert_args_to_provider_config(
        self, history: List[AnthropicMessage] = None, **kwargs
    ) -> AnthropicRequestConfig:
        """
        Converts the arguments to a provider-specific configuration.
        """

        system_prompt = kwargs.pop("system_prompt", None)

        max_tokens = (
            kwargs.pop("max_tokens", None)
            or kwargs.pop("max_completion_tokens", None)
            or self.model_limits.max_completion_tokens
        )

        tools = kwargs.pop("tools", None)

        return AnthropicRequestConfig(
            model=self.model_name,
            system=system_prompt,
            messages=[message.model_dump(mode="json") for message in history],
            betas=[self.beta_feature_name] if self.use_beta_header else None,
            tools=tools,
            tool_choice=kwargs.pop("tool_choice", None),
            max_tokens=max_tokens,
            temperature=kwargs.pop("temperature", None),
            top_k=kwargs.pop("top_k", None),
            top_p=kwargs.pop("top_p", None),
            container=kwargs.pop("container", None),
            stop_sequences=kwargs.pop("stop_sequences", None),
        )
