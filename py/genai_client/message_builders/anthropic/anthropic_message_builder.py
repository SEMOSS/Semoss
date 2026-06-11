from typing import List, Dict, Any, Tuple, Union, Optional
import json, re
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
    AnthropicServerToolResultContentPart,
    AnthropicRequestConfig,
    AnthropicMessageBuilderResponse,
)
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSMessagePartType,
    SEMOSSMediaContent,
    SEMOSSMediaInputType,
    ModelSettings,
)
from ...text_generation.abstract_text_generation_client import ModelLimits
from ...utils import string_to_bool

MODEL_MAX_OUTPUT_TOKENS = {
    # Claude 4.5 family
    ("opus", "4", "5"): 64_000,
    ("sonnet", "4", "5"): 64_000,
    ("haiku", "4", "5"): 64_000,
    # Claude 4.x family
    ("opus", "4", "1"): 64_000,
    ("opus", "4", None): 64_000,
    ("sonnet", "4", None): 64_000,
    # Claude 3.7
    ("sonnet", "3", "7"): 128_000,
    # Claude 3.5
    ("sonnet", "3", "5"): 8_192,
    ("haiku", "3", "5"): 8_192,
    # Claude 3
    ("opus", "3", None): 4_096,
    ("sonnet", "3", None): 4_096,
    ("haiku", "3", None): 4_096,
}

VERSION_FALLBACKS = {
    ("4", "5"): 64_000,
    ("4", None): 64_000,
    ("3", "7"): 128_000,
    ("3", "5"): 8_192,
    ("3", None): 4_096,
}


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

        has_schema = False

        # Define noise strings that should be treated as empty
        IGNORED_CONTENT = ["(no content)", ""]

        for i, message in enumerate(semoss_messages):
            is_last = i == len(semoss_messages) - 1
            content_parts = []

            ## =============== NEW PARTS STRUCTURE ============
            if message.parts:
                is_assistant = message.io != "INPUT"
                # Collect media parts that must be moved out of assistant turns
                assistant_media_parts = []

                for p in message.parts:
                    if p.type == SEMOSSMessagePartType.TEXT:
                        content_parts.append(self._build_text_content_part(p.text))

                    elif p.type == SEMOSSMessagePartType.MEDIA:
                        if is_assistant:
                            # Anthropic does not allow image blocks in assistant turns;
                            # add a text placeholder and queue the image for a synthetic user message.
                            file_name = (
                                getattr(p.media_info, "file_name", None) or "image"
                            )
                            content_parts.append(
                                self._build_text_content_part(
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
                            content_parts.append(media_content)

                    elif p.type == SEMOSSMessagePartType.TOOL_CALL:
                        # Server-tool calls (like web_search) replay as
                        # `server_tool_use` blocks so Anthropic pairs them with
                        # the matching `*_tool_result` block. Client-tool calls
                        # stay as plain `tool_use`.
                        tool_use_part = AnthropicToolUseContentPart(
                            type=(
                                "server_tool_use"
                                if p.tool_call.server_tool
                                else "tool_use"
                            ),
                            id=p.tool_call.id,
                            name=p.tool_call.function.name,
                            input=p.tool_call.function.parameters,
                        )
                        content_parts.append(tool_use_part)

                    elif p.type == SEMOSSMessagePartType.TOOL_RESULT:
                        # Server-tool results (like web_search) must round-trip as the
                        # provider-specific result block inside the assistant turn,
                        # not as a generic `tool_result` block (Anthropic only accepts
                        # those in user turns).
                        if p.tool_result.server_tool:
                            try:
                                result_content = json.loads(p.tool_result.output)
                            except (json.JSONDecodeError, TypeError):
                                result_content = p.tool_result.output
                            content_parts.append(
                                AnthropicServerToolResultContentPart(
                                    type=f"{p.tool_result.tool_name}_tool_result",
                                    tool_use_id=p.tool_result.id,
                                    content=result_content,
                                )
                            )
                        else:
                            tool_result_part = AnthropicToolResultContentPart(
                                tool_use_id=p.tool_result.id,
                                content=p.tool_result.output,
                            )
                            content_parts.append(tool_result_part)

                    elif p.type == SEMOSSMessagePartType.THINKING:
                        thinking_dict = {
                            "type": "thinking",
                            "thinking": p.thinking,
                        }
                        if self.thinking_signature:
                            thinking_dict["signature"] = self.thinking_signature
                        content_parts.append(thinking_dict)

                anthropic_messages.append(
                    AnthropicMessage(
                        role=(
                            AnthropicRoles.USER
                            if message.io == "INPUT"
                            else AnthropicRoles.ASSISTANT
                        ),
                        content=content_parts,
                    )
                )

                # Inject a synthetic user message with the images so Claude can see them
                if assistant_media_parts:
                    synthetic_content = [
                        self._build_text_content_part("Here is the generated image:")
                    ] + assistant_media_parts
                    anthropic_messages.append(
                        AnthropicMessage(
                            role=AnthropicRoles.USER,
                            content=synthetic_content,
                        )
                    )

                # handle parameters update based on last message same as w/o parts
                if is_last:
                    param_map = message.param_map
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
                            param_map.pop("built_in_tools")
                        )
                        if built_in_tools:
                            if "tools" in param_map:
                                param_map["tools"].extend(built_in_tools)
                            else:
                                param_map["tools"] = built_in_tools
                    if "tool_choice" in param_map:
                        param_map["tool_choice"] = self._build_tool_choice(
                            param_map["tool_choice"]
                        )

            ## =============== LEGACY MESSAGE TYPE STRUCTURE ============
            else:
                # --- 1. HANDLE USER TEXT / MEDIA MESSAGES ---
                if (
                    message.type == SEMOSSMessageType.INPUT_TEXT
                    or message.type == SEMOSSMessageType.INPUT_MEDIA
                ):
                    if message.content and message.content not in IGNORED_CONTENT:
                        content_parts.append(
                            self._build_text_content_part(message.content)
                        )

                    if message.media_content:
                        media_contents_parts = self._build_media_content_part(
                            message.media_content
                        )
                        content_parts.extend(media_contents_parts)

                    # Only append if we actually have content
                    if content_parts:
                        anthropic_messages.append(
                            AnthropicMessage(
                                role=AnthropicRoles.USER,
                                content=content_parts,
                            )
                        )

                # --- 2. HANDLE ASSISTANT RESPONSES (TOOL CALLS OR TEXT) ---
                elif message.type == SEMOSSMessageType.RESPONSE_TOOL:
                    # Handle assistant tool calls
                    if message.tool_calls:
                        # When thinking is enabled and we have thinking content, add it FIRST
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

                        if content_parts:
                            anthropic_messages.append(
                                AnthropicMessage(
                                    role=AnthropicRoles.ASSISTANT,
                                    content=content_parts,
                                )
                            )

                # --- 3. HANDLE TOOL EXECUTION RESULTS (WITH IMAGES) ---
                elif message.type == SEMOSSMessageType.INPUT_TOOL_EXEC:
                    # Handle tool execution results
                    if message.tool_call_id:
                        # Build content - can be string or array with images
                        if message.media_content:
                            # Add text if present (and valid)
                            if (
                                message.content
                                and message.content not in IGNORED_CONTENT
                            ):
                                content_parts.append(
                                    {"type": "text", "text": message.content}
                                )

                            # Add images
                            for media in message.media_content:
                                if media.type == SEMOSSMediaInputType.BASE64:
                                    content_parts.append(
                                        {
                                            "type": "image",
                                            "source": {
                                                "type": "base64",
                                                "media_type": media.mime_type,
                                                "data": media.data,
                                            },
                                        }
                                    )
                                elif media.type == SEMOSSMediaInputType.URL:
                                    media_data, media_type = fetch_and_encode_image(
                                        media.url
                                    )
                                    content_parts.append(
                                        {
                                            "type": "image",
                                            "source": {
                                                "type": "base64",
                                                "media_type": media_type,
                                                "data": media_data,
                                            },
                                        }
                                    )

                            tool_result = AnthropicToolResultContentPart(
                                tool_use_id=message.tool_call_id,
                                content=content_parts,
                            )
                        else:
                            # Simple string content for the tool result
                            text_content = message.content
                            if text_content in IGNORED_CONTENT or text_content is None:
                                # CRITICAL: We cannot have an empty tool result.
                                # If we have no content, provide a placeholder so the chain isn't broken.
                                text_content = "Tool executed successfully."

                            tool_result = AnthropicToolResultContentPart(
                                tool_use_id=message.tool_call_id,
                                content=text_content,
                            )

                        # We ALWAYS append tool results, even if we had to inject placeholder text
                        anthropic_messages.append(
                            AnthropicMessage(
                                role=AnthropicRoles.USER,
                                content=[tool_result],
                            )
                        )

                # --- 4. HANDLE ASSISTANT TEXT RESPONSES ---
                elif message.type == SEMOSSMessageType.RESPONSE_TEXT:
                    # Filter out "(no content)" noise
                    if message.content and message.content not in IGNORED_CONTENT:
                        content_parts.append(
                            self._build_text_content_part(message.content)
                        )

                    # Only append if we actually have valid content
                    if content_parts:
                        anthropic_messages.append(
                            AnthropicMessage(
                                role=AnthropicRoles.ASSISTANT,
                                content=content_parts,
                            )
                        )

                # --- 5. HANDLE PARAMETER UPDATES ---
                if is_last:
                    param_map = message.param_map
                    # ... (rest of the param_map logic remains the same) ...
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
                            param_map.pop("built_in_tools")
                        )
                        if built_in_tools:
                            if "tools" in param_map:
                                param_map["tools"].extend(built_in_tools)
                            else:
                                param_map["tools"] = built_in_tools
                    if "tool_choice" in param_map:
                        param_map["tool_choice"] = self._build_tool_choice(
                            param_map["tool_choice"]
                        )
                    if "base64Docs" in param_map:
                        base64_docs = self._handle_base_64_docs_direct(
                            param_map.pop("base64Docs")
                        )
                        anthropic_messages[len(anthropic_messages) - 1].content.extend(
                            base64_docs
                        )
                # --- POST-PROCESSING: Merge consecutive same-role messages ---
                if anthropic_messages:
                    merged_messages = []
                    for msg in anthropic_messages:
                        if merged_messages and merged_messages[-1].role == msg.role:
                            # Same role as previous - merge content into the previous message
                            prev_msg = merged_messages[-1]
                            # Ensure content is a list for both messages
                            prev_content = (
                                prev_msg.content
                                if isinstance(prev_msg.content, list)
                                else [prev_msg.content]
                            )
                            new_content = (
                                msg.content
                                if isinstance(msg.content, list)
                                else [msg.content]
                            )
                            prev_msg.content = prev_content + new_content
                        else:
                            merged_messages.append(msg)
                    anthropic_messages = merged_messages

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
        # if schema_dict.get("type") != "object":
        #     raise ValueError("Top-level schema must be an object.")

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
            anthropic_media_parts.append(self._build_media_content_single_part(media))

        return anthropic_media_parts

    def _build_media_content_single_part(
        self, media: SEMOSSMediaContent
    ) -> Union[AnthropicImageContentPart, AnthropicDocumentContentPart]:
        """Build Anthropic media content part from SEMOSS media content."""
        if media.type == SEMOSSMediaInputType.URL:
            return self._build_url_media_content(media)
        elif media.type == SEMOSSMediaInputType.BASE64:
            return self._build_base64_media_content(media)
        else:
            raise ValueError(f"Unknown media type: {media.type}")

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

        data = media_content.data
        mime_type = media_content.mime_type

        # Handle data URI format: 'data:<mime_type>;base64,<data>'
        if data.startswith("data:") and ";base64," in data:
            # Extract mime type from data URI if not already set
            data_uri_mime = data.split(";base64,")[0].replace("data:", "")
            if not mime_type:
                mime_type = data_uri_mime
            # Extract just the base64 data (after the comma)
            data = data.split(";base64,")[1]

        if not mime_type:
            mime_type = get_image_extension(data)

        # Normalize short mime types (e.g., 'jpeg' -> 'image/jpeg', 'png' -> 'image/png')
        if mime_type and "/" not in mime_type:
            image_extensions = ["jpeg", "jpg", "png", "gif", "webp", "bmp", "tiff"]
            if mime_type.lower() in image_extensions:
                mime_type = f"image/{mime_type.lower()}"

        if mime_type == "image/jpg":
            mime_type = "image/jpeg"

        media_source = AnthropicMediaSourceBase64(
            media_type=mime_type,
            data=data,
        )
        if mime_type and mime_type.startswith("image"):
            return AnthropicImageContentPart(source=media_source)
        else:
            return AnthropicDocumentContentPart(source=media_source)

    def _convert_mcp_to_anthropic_tools(self, mcp_tools: List[Dict]) -> List[Dict]:
        """
        Convert MCP-formatted tools to Anthropic tool format.
        Supports two formats:
        1. Standard MCP format: {"name": ..., "description": ..., "inputSchema": {...}}
        2. OpenAI/Claude Code format: {"type": "function", "function": {"name": ..., "description": ..., "parameters": {...}}}
        """
        anthropic_tools = []

        for tool in mcp_tools:
            # Detect format: OpenAI/Claude Code style has "function" key
            if "function" in tool:
                # OpenAI/Claude Code format
                func_def = tool["function"]
                name = func_def["name"]
                description = func_def.get("description", "")
                schema = func_def.get("parameters", {})
            else:
                # Standard MCP format
                name = tool["name"]
                description = tool.get("description", "")
                schema = tool.get("inputSchema", {})

            anthropic_tool = {
                "name": name,
                "description": description,
                "input_schema": {
                    "type": schema.get("type", "object"),
                    "properties": {},
                    "required": schema.get("required", []),
                },
            }

            # Copy properties, filtering out metadata fields
            properties = schema.get("properties", {})
            for prop_name, prop_def in properties.items():
                # Filter out JSON Schema metadata fields that aren't needed
                anthropic_tool["input_schema"]["properties"][prop_name] = {
                    k: v for k, v in prop_def.items() if k not in ("title", "$schema")
                }

            anthropic_tools.append(anthropic_tool)

        return anthropic_tools

    def _resolve_extended_thinking(
        self,
        thinking: Optional[str | bool] = None,
        thinking_budget: Optional[int] = None,
        param_map: Optional[Dict[str, Any]] = {},
    ) -> Dict[str, Any] | None:
        """
        Honor the thinking keys passed in the param map first and then use anything passed from the SMSS.
        If I get this from Claude Code I get something like: {'budget_tokens': 31999, 'type': 'enabled'}
        If I get this from SEMOSS I get separate keys 'thinking': True, 'thinking_budget': 1000
        """
        ## SMSS THINKING
        smss_thinking = string_to_bool(thinking)
        if smss_thinking:
            smss_thinking = "enabled"
        else:
            smss_thinking = "disabled"

        smss_thinking_budget = thinking_budget if thinking_budget else 0

        ## RESOLVING PARAM MAP
        param_map_thinking = param_map.get("thinking", "disabled")
        # I get this from Claude Code
        if isinstance(thinking, dict):
            param_map_thinking = thinking.get("type", "disabled")
            param_map_budget_tokens = thinking.get("budget_tokens", 0)
        else:
            param_map_thinking = string_to_bool(thinking)
            if param_map_thinking:
                param_map_thinking = "enabled"
            else:
                param_map_thinking = "disabled"
            param_map_budget_tokens = param_map.get("thinking_budget", 0)

        resolved_thinking = (
            "enabled"
            if param_map_thinking == "enabled" or smss_thinking == "enabled"
            else "disabled"
        )

        if resolved_thinking == "disabled":
            return None

        # If you set thinking as enabled but don't have a thinking budget I am going to set it for you..
        if param_map_budget_tokens >= 1024:
            resolved_thinking_budget = param_map_budget_tokens
        elif smss_thinking_budget >= 1024:
            resolved_thinking_budget = smss_thinking_budget
        else:
            resolved_thinking_budget = 1024

        return {"type": "enabled", "budget_tokens": resolved_thinking_budget}

    def _convert_args_to_provider_config(
        self,
        model_settings: ModelSettings,
        history: List[AnthropicMessage] | None = None,
        **kwargs,
    ) -> AnthropicRequestConfig:
        """
        Converts the arguments to a provider-specific configuration.
        """

        # CODEX SPECIFIC HANDLING
        instructions = kwargs.pop("instructions", None)

        system_prompt = kwargs.pop("system_prompt", None)
        if instructions and not system_prompt:
            system_prompt = instructions

        tools = kwargs.pop("tools", None)

        thinking_map = self._resolve_extended_thinking(
            thinking=model_settings.thinking,  # SMSS SETTING
            thinking_budget=model_settings.thinking_budget,  # SMSS SETTING
            param_map=kwargs,
        )

        max_tokens = (
            kwargs.pop("max_tokens", None)
            or kwargs.pop("max_completion_tokens", None)
            or self.model_limits.max_completion_tokens
        )

        # MAX TOKENS MUST BE STRICTLY GREATER THAN THINKING BUDGET
        if thinking_map and thinking_map.get("type") == "enabled":
            budget_tokens = thinking_map.get("budget_tokens", 0)
            if max_tokens is None or max_tokens <= budget_tokens:
                model_cap = self._get_model_max_output_tokens(self.model_name)
                max_tokens = min(budget_tokens * 2, model_cap)
                if max_tokens <= budget_tokens:
                    max_tokens = min(budget_tokens + 1024, model_cap)

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

        if "use_history" in kwargs:
            use_history = kwargs.pop("use_history")
            if string_to_bool(use_history) is False:
                history = history[-1:] if history else []

        return AnthropicRequestConfig(
            model=self.model_name,
            system=system_prompt,
            messages=[
                message.model_dump(mode="json", exclude_none=True)
                for message in history
            ],
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

    def _parse_claude_model_name(
        self, model_name: str
    ) -> tuple[str | None, str | None, str | None]:
        """
        Extract model tier and version from various provider formats.

        Handles:
        - claude-sonnet-4-5-20250929 (Anthropic API)
        - claude-sonnet-4-5@20250929 (GCP Vertex AI)
        - anthropic.claude-3-5-sonnet-20241022-v2:0 (AWS Bedrock)
        - claude-3-opus-20240229 (older format)
        - claude-sonnet-4-5 (alias)

        Returns:
            (tier, major_version, minor_version) e.g. ("sonnet", "4", "5")
        """
        name_lower = model_name.lower()

        tier = None
        for t in ("opus", "sonnet", "haiku"):
            if t in name_lower:
                tier = t
                break

        if tier is None:
            return (None, None, None)

        version_pattern = re.search(
            r"(?:claude[- ]?)?(\d+)[.-](\d+)?[.-]?"
            + tier
            + r"|"
            + tier
            + r"[.-]?(\d+)[.-](\d+)?",
            name_lower,
        )

        if version_pattern:
            groups = version_pattern.groups()
            if groups[0]:
                return (tier, groups[0], groups[1])
            else:
                return (tier, groups[2], groups[3])

        fallback_pattern = re.search(r"(\d+)(?:[.-](\d+))?", name_lower)
        if fallback_pattern:
            return (tier, fallback_pattern.group(1), fallback_pattern.group(2))

        return (tier, None, None)

    def _get_model_max_output_tokens(self, model_name: str) -> int:
        """Get max output tokens for a model, handling various provider formats."""
        tier, major, minor = self._parse_claude_model_name(model_name)

        if tier is None:
            # Not a recognized Claude model, return conservative default
            return 4_096

        # Try exact match
        key = (tier, major, minor)
        if key in MODEL_MAX_OUTPUT_TOKENS:
            return MODEL_MAX_OUTPUT_TOKENS[key]

        # Try without minor version
        key_no_minor = (tier, major, None)
        if key_no_minor in MODEL_MAX_OUTPUT_TOKENS:
            return MODEL_MAX_OUTPUT_TOKENS[key_no_minor]

        # Fallback by version only
        version_key = (major, minor)
        if version_key in VERSION_FALLBACKS:
            return VERSION_FALLBACKS[version_key]

        version_key_no_minor = (major, None)
        if version_key_no_minor in VERSION_FALLBACKS:
            return VERSION_FALLBACKS[version_key_no_minor]

        # Ultimate fallback
        return 4_096

    def _handle_base_64_docs_direct(
        self, base_64_docs: List[Dict[str, str] | Dict[str, str]]
    ) -> List[AnthropicDocumentContentPart]:
        """
        Handle base64 documents passed directly in the param map (not saved to room history).
        Expects a list of dicts with keys "data" (base64 string) and "mime_type".
        """
        content_parts = []
        if isinstance(base_64_docs, list):
            for doc in base_64_docs:
                if isinstance(doc, dict) and "data" in doc and "mime_type" in doc:
                    media_source = AnthropicMediaSourceBase64(
                        media_type=doc["mime_type"],
                        data=doc["data"],
                    )
                    content_part = AnthropicDocumentContentPart(source=media_source)
                    content_parts.append(content_part)
        elif (
            isinstance(base_64_docs, dict)
            and "data" in base_64_docs
            and "mime_type" in base_64_docs
        ):
            media_source = AnthropicMediaSourceBase64(
                media_type=base_64_docs["mime_type"],
                data=base_64_docs["data"],
            )
            content_part = AnthropicDocumentContentPart(source=media_source)
            content_parts.append(content_part)
        else:
            raise ValueError(
                "base64Docs must be a dict or list of dicts with 'data' and 'mime_type' keys."
            )
        return content_parts
