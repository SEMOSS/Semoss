from typing import List, Dict, Any, Tuple, Union, Optional
import json
from ...utils import (
    get_image_extension,
    fetch_and_encode_image,
)
from .anthropic_models import (
    AnthropicRoles,
    AnthropicMessage,
    AnthropicMediaSourceBase64,
    AnthropicImageContentPart,
    AnthropicDocumentContentPart,
    AnthropicTextContentPart,
    AnthropicToolUseContentPart,
    AnthropicToolResultContentPart,
    AnthropicRequestConfig,
    AnthropicMessageBuilderResponse,
)
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSMediaContent,
    SEMOSSMediaInputType,
    ModelSettings,
)
from ...text_generation.abstract_text_generation_client import ModelLimits
from ...utils import string_to_bool


class AnthropicMessageBuilder:

    def build_messages(
        self,
        semoss_messages: List[SEMOSSMessage],
        model_settings: ModelSettings,
        model_limits: ModelLimits,
        model_name: str,
        use_beta_header: bool = False,
        beta_feature_name: str = "extended_thinking",
        thinking_signature: Optional[str] = None,
    ) -> AnthropicMessageBuilderResponse:
        """Convert SEMOSS messages to Anthropic messages and return the param map from the latest message"""
        self.model_limits = model_limits
        self.model_name = model_name
        self.model_settings = model_settings
        self.use_beta_header = use_beta_header
        self.beta_feature_name = beta_feature_name
        self.thinking_signature = thinking_signature
        anthropic_messages = []
        param_map = {}

        pending_tool_calls = []
        pending_tool_results = []

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

                if message.media_content:
                    media_contents_parts = self._build_media_content_part(
                        message.media_content
                    )
                    content_parts.extend(media_contents_parts)

                anthropic_messages.append(
                    AnthropicMessage(
                        role=AnthropicRoles.USER,
                        content=content_parts,
                    )
                )

            elif message.type == SEMOSSMessageType.RESPONSE_TOOL:
                # Handle assistant tool calls
                if message.tool_calls:
                    # When thinking is enabled and we have thinking content, add it FIRST as raw dict
                    if self.model_settings.thinking and message.param_map.get(
                        "thinking"
                    ):
                        thinking_dict = {
                            "type": "thinking",
                            "thinking": message.param_map.get("thinking"),
                        }
                        if self.thinking_signature:
                            thinking_dict["signature"] = self.thinking_signature

                        content_parts.append(thinking_dict)

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
                if "built_in_tools" in param_map:
                    built_in_tools = self._build_built_in_tools(
                        param_map["built_in_tools"]
                    )
                    if "tools" in param_map:
                        param_map["tools"].extend(built_in_tools)
                if "tool_choice" in param_map:
                    param_map["tool_choice"] = self._build_tool_choice(
                        param_map["tool_choice"]
                    )

        streaming = param_map.pop("streaming", None)
        if streaming is None:
            streaming = param_map.pop("stream", None)
        if streaming is None:
            streaming = True

        if streaming is not None and isinstance(streaming, str):
            try:
                streaming = string_to_bool(streaming)
            except ValueError:
                streaming = False

        request_config = self._convert_args_to_provider_config(
            model_settings=self.model_settings,
            history=anthropic_messages,
            **param_map,
        )

        return AnthropicMessageBuilderResponse(
            request_config=request_config,
            streaming=streaming,
            has_structured_input=has_schema,
        )

    def _build_built_in_tools(self, built_in_tools: List[str]) -> List[Dict[str, Any]]:
        anthropic_built_in_tools: List[Dict[str, Any]] = []
        for tool in built_in_tools:
            if tool.lower() == "web_search":
                anthropic_built_in_tools.append(
                    {"type": "web_search_20250305", "name": "web_search", "max_uses": 5}
                )
            elif tool.lower() == "code_execution":
                anthropic_built_in_tools.append(
                    {"type": "code_execution_20250825", "name": "code_execution"}
                )
        return anthropic_built_in_tools

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

        # When thinking is enabled, only auto and none are supported
        if self.model_settings.thinking:
            if tool_type in ["required", "forced"]:
                return {"type": "auto"}

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

    def _build_media_content_part(
        self, media_content: List[SEMOSSMediaContent] = []
    ) -> List[Union[AnthropicImageContentPart, AnthropicDocumentContentPart]]:
        """Build Anthropic media content parts from SEMOSS media content."""

        anthropic_media_parts = []
        for media in media_content:
            if media.type == SEMOSSMediaInputType.URL:
                anthropic_media_parts.append(self._build_url_media_content(media))
            elif media.type == SEMOSSMediaInputType.BASE64:
                anthropic_media_parts.append(self._build_base64_media_content(media))
            else:
                raise ValueError(f"Unknown media type: {media.type}")

        return anthropic_media_parts

    def _build_url_media_content(
        self, media_content: SEMOSSMediaContent
    ) -> Union[AnthropicImageContentPart, AnthropicDocumentContentPart]:
        """Build Anthropic media content part from URL as base64"""
        if not media_content.url:
            raise ValueError(
                "The media type was specified as URL but no URL was provided.."
            )

        # TODO: this utility methods needs to be expanded for non-images
        media_data, media_type = fetch_and_encode_image(media_content.url)
        if media_type == "image/jpg":
            media_type = "image/jpeg"

        media_source = AnthropicMediaSourceBase64(
            media_type=media_type,
            data=media_data,
        )
        if media_type.startswith("image"):
            return AnthropicImageContentPart(source=media_source)
        else:
            return AnthropicDocumentContentPart(source=media_source)

    def _build_base64_media_content(
        self, media_content: SEMOSSMediaContent
    ) -> Union[AnthropicImageContentPart, AnthropicDocumentContentPart]:
        """Build Anthropic media content part from base64"""
        if not media_content.data:
            raise ValueError(
                "The media type was specified as base64 but no data was provided."
            )

        if not media_content.mime_type:
            media_content.mime_type = get_image_extension(media_content.data)

        if media_content.mime_type == "image/jpg":
            media_content.mime_type = "image/jpeg"

        media_source = AnthropicMediaSourceBase64(
            media_type=media_content.mime_type,
            data=media_content.data,
        )
        if media_content.mime_type.startswith("image"):
            return AnthropicImageContentPart(source=media_source)
        else:
            return AnthropicDocumentContentPart(source=media_source)

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

    def _resolve_extended_thinking(
        self,
        thinking: Optional[bool] = None,
        thinking_budget: Optional[int] = None,
        param_map: Optional[Dict[str, Any]] = {},
    ) -> Dict[str, Any] | None:
        """
        Honor the thinking keys passed in the param map first and then use anything passed from the SMSS.
        """
        if "thinking" in param_map:
            try:
                thinking = string_to_bool(param_map["thinking"])
            except ValueError:
                thinking = False
        if "thinking_budget" in param_map:
            thinking_budget = int(param_map["thinking_budget"])

        if thinking is None:
            thinking = False

        if thinking:
            if thinking_budget is None:
                thinking_budget = 10000

            return {"type": "enabled", "budget_tokens": thinking_budget}

        return None

    def _convert_args_to_provider_config(
        self,
        model_settings: ModelSettings,
        history: List[AnthropicMessage] = None,
        **kwargs,
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

        thinking_map = self._resolve_extended_thinking(
            thinking=model_settings.thinking,
            thinking_budget=model_settings.thinking_budget,
            param_map=kwargs,
        )

        temperature = kwargs.pop("temperature", None)
        top_p = kwargs.pop("top_p", None)
        if thinking_map:
            # top_p between 0.95 to 1 when thinking
            if top_p is not None:
                if top_p < 0.95:
                    top_p = 0.95
                elif top_p > 1:
                    top_p = 1
            # temperature can only be 1 when thinking
            if temperature is not None:
                temperature = 1

        return AnthropicRequestConfig(
            model=self.model_name,
            system=system_prompt,
            messages=[message.model_dump(mode="json") for message in history],
            betas=[self.beta_feature_name] if self.use_beta_header else None,
            tools=tools,
            tool_choice=kwargs.pop("tool_choice", None),
            max_tokens=max_tokens,
            temperature=temperature,
            top_k=kwargs.pop("top_k", None),
            top_p=top_p,
            container=kwargs.pop("container", None),
            stop_sequences=kwargs.pop("stop_sequences", None),
            thinking=thinking_map,
        )
