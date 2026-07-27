import base64, json
from typing import List, Dict, Any, Tuple, Union
from google.genai.types import Content, Part
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSMessagePartType,
    SEMOSSMediaContent,
    SEMOSSMediaInputType,
    ModelSettings,
)

# from google.genai.types import (
#     EnterpriseWebSearch,
# )

from ...text_generation.abstract_text_generation_client import ModelLimits
from .google_genai_models import GoogleRoles
from google.genai import types
from ...utils import string_to_bool
from ..semoss_base.reasoning import normalize_reasoning


class GoogleGenAIMessageBuilder:

    def _normalize_web_search_tool(self, value: Any) -> str:
        """
        Controls which Google built-in web search tool is used when `built_in_tools`
        includes `web_search`.

        Supported values:
        - "enterprise" | "enterprise_web_search" (default)
        - "google" | "google_search"
        """
        if value is None:
            return "enterprise"
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"enterprise", "enterprise_web_search"}:
                return "enterprise"
            if normalized in {"google", "google_search"}:
                return "google"
        raise ValueError(
            f"Unsupported web search tool: {value!r}. Use 'enterprise' or 'google'."
        )

    def build_messages(
        self,
        semoss_messages: List[SEMOSSMessage],
        model_settings: ModelSettings,
        model_limits: ModelLimits,
    ) -> Dict[str, Any]:
        """Convert SEMOSS messages to Google GenAI Content."""
        self.model_settings = model_settings
        self.model_limits = model_limits
        google_messages = []

        pending_tool_responses = []
        expected_tool_count = 0

        for i, message in enumerate(semoss_messages):
            if message.parts:
                tool_id_to_name = {}
                parts = []
                for p in message.parts:
                    if p.type == SEMOSSMessagePartType.TEXT:
                        parts.append(self._build_text_content_part(p.text))

                    elif p.type == SEMOSSMessagePartType.MEDIA:
                        media_content = self._build_media_content_single_part(
                            p.media_info
                        )
                        parts.append(media_content)

                    elif p.type == SEMOSSMessagePartType.TOOL_CALL:
                        tool_id_to_name.update(
                            {p.tool_call.id: p.tool_call.function.name}
                        )
                        ts_bytes = None
                        if p.tool_call.thought_signature:
                            ts_bytes = base64.b64decode(p.tool_call.thought_signature)
                        if ts_bytes:
                            fc_part = types.Part(
                                function_call=types.FunctionCall(
                                    name=p.tool_call.function.name,
                                    args=p.tool_call.function.parameters,
                                ),
                                thought_signature=ts_bytes,
                            )
                        else:
                            fc_part = Part.from_function_call(
                                name=p.tool_call.function.name,
                                args=p.tool_call.function.parameters,
                            )
                        parts.append(fc_part)

                    elif p.type == SEMOSSMessagePartType.TOOL_RESULT:
                        parts.append(
                            Part.from_function_response(
                                name=tool_id_to_name.get(
                                    p.tool_result.id, "unknown_tool"
                                ),
                                response={"result": p.tool_result.output},
                            )
                        )

                    elif p.type == SEMOSSMessagePartType.THINKING:
                        thinking_dict = {
                            "type": "thinking",
                            "thinking": p.thinking,
                        }
                        if self.thinking_signature:
                            thinking_dict["signature"] = self.thinking_signature
                        parts.append(thinking_dict)

                google_messages.append(
                    Content(
                        role=(
                            GoogleRoles.USER.value
                            if message.io == "INPUT"
                            else GoogleRoles.MODEL.value
                        ),
                        parts=parts,
                    )
                )

            else:
                parts = []
                if (
                    message.type == SEMOSSMessageType.INPUT_TEXT
                    or message.type == SEMOSSMessageType.INPUT_MEDIA
                ):
                    if message.content:
                        parts.append(self._build_text_content_part(message.content))

                    if message.media_content:
                        parts.extend(
                            self._build_media_content_parts(message.media_content)
                        )

                    google_messages.append(
                        Content(
                            role=GoogleRoles.USER.value,
                            parts=parts,
                        )
                    )

                elif message.type == SEMOSSMessageType.RESPONSE_TOOL:
                    if message.tool_calls:
                        expected_tool_count = len(message.tool_calls)

                        for tool_call in message.tool_calls:
                            args = tool_call.get("function").get("arguments")

                            # Handle case where arguments is a JSON string instead of dict
                            if isinstance(args, str):
                                args = json.loads(args)

                            ts_bytes = None
                            ts_b64 = tool_call.get("thought_signature")
                            if ts_b64:
                                ts_bytes = base64.b64decode(ts_b64)

                            if ts_bytes:
                                fc_part = types.Part(
                                    function_call=types.FunctionCall(
                                        name=tool_call.get("function").get("name"),
                                        args=args,
                                    ),
                                    thought_signature=ts_bytes,
                                )
                            else:
                                fc_part = Part.from_function_call(
                                    name=tool_call.get("function").get("name"),
                                    args=args,
                                )
                            parts.append(fc_part)

                        google_messages.append(
                            Content(
                                role=GoogleRoles.MODEL.value,
                                parts=parts,
                            )
                        )

                elif message.type == SEMOSSMessageType.INPUT_TOOL_EXEC:
                    if expected_tool_count > 0:
                        tool_name = None
                        for j in range(i - 1, -1, -1):
                            prev_msg = semoss_messages[j]
                            if prev_msg.type == SEMOSSMessageType.RESPONSE_TOOL:
                                if prev_msg.tool_calls:
                                    for tool_call in prev_msg.tool_calls:
                                        if str(tool_call.get("id")) == str(
                                            message.tool_call_id
                                        ):
                                            tool_name = tool_call["function"]["name"]
                                            break
                                break

                        if tool_name and message.content:
                            pending_tool_responses.append(
                                Part.from_function_response(
                                    name=tool_name, response={"result": message.content}
                                )
                            )

                            if len(pending_tool_responses) == expected_tool_count:
                                google_messages.append(
                                    Content(
                                        role=GoogleRoles.USER.value,
                                        parts=pending_tool_responses,
                                    )
                                )
                                pending_tool_responses = []
                                expected_tool_count = 0

                elif message.type == SEMOSSMessageType.RESPONSE_TEXT:
                    if message.content:
                        parts.append(self._build_text_content_part(message.content))

                    google_messages.append(
                        Content(
                            role=GoogleRoles.MODEL.value,
                            parts=parts,
                        )
                    )

            if i == len(semoss_messages) - 1:
                provider_config, stream = self._convert_args_to_provider_config(
                    **message.param_map
                )

        return {
            "messages": google_messages,
            "provider_config": provider_config,
            "stream": stream,
        }

    # Canonical effort -> Gemini 3 ThinkingLevel. The SDK enum only has
    # MINIMAL/LOW/MEDIUM/HIGH, so xhigh/max collapse to HIGH and none->MINIMAL.
    _GEMINI_THINKING_LEVEL = {
        "none": types.ThinkingLevel.MINIMAL,
        "minimal": types.ThinkingLevel.MINIMAL,
        "low": types.ThinkingLevel.LOW,
        "medium": types.ThinkingLevel.MEDIUM,
        "high": types.ThinkingLevel.HIGH,
        "xhigh": types.ThinkingLevel.HIGH,
        "max": types.ThinkingLevel.HIGH,
    }

    def _resolve_thinking_config(
        self, param_map: Dict[str, Any]
    ) -> types.ThinkingConfig:
        """
        Honor the thinking/effort keys passed in the param map first, then fall
        back to SMSS. Gemini 3.x uses `thinking_level`; Gemini 2.5/2.0 uses
        `thinking_budget` (setting both on a Gemini 3 model returns a 400).
        """
        resolved = normalize_reasoning(param_map, self.model_settings)
        if resolved is None:
            return types.ThinkingConfig(include_thoughts=False)

        model_name = (self.model_settings.model_name or "").lower()
        if "gemini-3" in model_name and "gemini-3-pro-image" not in model_name:
            return types.ThinkingConfig(
                include_thoughts=True,
                thinking_level=self._GEMINI_THINKING_LEVEL.get(
                    resolved.effort, types.ThinkingLevel.HIGH
                ),
            )
        return types.ThinkingConfig(
            include_thoughts=True, thinking_budget=resolved.budget
        )

    def _convert_args_to_provider_config(
        self, **kwargs
    ) -> Tuple[types.GenerateContentConfig, bool]:
        """
        Convert our CFG arguments to a GenerateContentConfig object.
        """
        # CODEX SPECIFIC HANDLING
        instructions = kwargs.pop("instructions", None)

        system_prompt = kwargs.pop("system_prompt", None)

        if instructions and not system_prompt:
            system_prompt = instructions

        structured_response_schema = kwargs.pop("schema", None)

        tools, tool_config = self.build_tools(kwargs)

        max_output_tokens = kwargs.pop("max_new_tokens", None)
        if max_output_tokens is None:
            max_output_tokens = kwargs.pop("max_completion_tokens", None)
        if max_output_tokens is None:
            max_output_tokens = kwargs.pop("max_tokens", None)
        if max_output_tokens is None:
            max_output_tokens = self.model_limits.max_completion_tokens

        stream = kwargs.pop("streaming", None)
        if stream is None:
            stream = kwargs.pop("stream", True)

        if isinstance(stream, str):
            try:
                stream = string_to_bool(stream)
            except ValueError:
                stream = True

        thinking_config = self._resolve_thinking_config(kwargs)

        response_modalities = (
            [m.upper() for m in self.model_settings.modalities]
            if self.model_settings.modalities
            else ["TEXT"]
        )

        response_mime_type = kwargs.pop("response_mime_type", None)
        if (structured_response_schema and "IMAGE" in response_modalities) or (
            tools and "IMAGE" in response_modalities
        ):
            response_modalities.remove("IMAGE")

        if structured_response_schema is not None and response_mime_type is None:
            response_mime_type = "application/json"

        config = types.GenerateContentConfig(
            http_options=kwargs.pop("http_options", None),
            system_instruction=system_prompt,
            max_output_tokens=max_output_tokens,
            temperature=kwargs.pop("temperature", None),
            top_p=kwargs.pop("top_p", None),
            top_k=kwargs.pop("top_k", None),
            seed=kwargs.pop("seed", None),
            stop_sequences=kwargs.pop("stop_sequences", None),
            presence_penalty=kwargs.pop("presence_penalty", None),
            frequency_penalty=kwargs.pop("frequency_penalty", None),
            safety_settings=None,  # TODO: Pass this from the init.. this lives in smss
            response_schema=structured_response_schema,
            response_mime_type=response_mime_type,
            tools=tools,
            tool_config=tool_config,
            thinking_config=thinking_config,
            response_modalities=response_modalities,
        )

        return config, stream

    def _build_text_content_part(self, content: str) -> Part:
        """Build a text content part for Google GenAI."""
        return Part.from_text(text=content)

    def _message_type_to_role(self, message_type: SEMOSSMessageType) -> GoogleRoles:
        """Convert SEMOSS message type to Google GenAI role."""
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
            return GoogleRoles.USER
        elif message_type in assistant_message_types:
            return GoogleRoles.MODEL
        else:
            raise ValueError(f"Unsupported SEMOSS message type: {message_type}")

    def _build_media_content_parts(
        self, media_content: List[SEMOSSMediaContent]
    ) -> List[Part]:
        """Convert SEMOSS media contents to Google GenAI Part."""
        google_media_parts = []
        for media in media_content:
            google_media_parts.append(self._build_media_content_single_part(media))

        return google_media_parts

    def _build_media_content_single_part(self, media: SEMOSSMediaContent) -> Part:
        """Convert SEMOSS media content to Google GenAI Part."""
        if media.type == SEMOSSMediaInputType.URL and media.url:
            return Part.from_uri(file_uri=media.url)
        elif media.type == SEMOSSMediaInputType.BASE64:
            if not media.mime_type or not media.data:
                raise ValueError(
                    f"Missing required base64 data or mime type when building Google GenAI media part."
                )
            return Part.from_bytes(data=media.data, mime_type=media.mime_type)
        else:
            raise ValueError(f"Unsupported SEMOSSMediaContent type: {media.type}")

    _GOOGLE_SCHEMA_ALLOWED = {
        "type",
        "format",
        "description",
        "default",
        "enum",
        "example",
        "properties",
        "required",
        "items",
        "min_items",
        "max_items",
        "minItems",
        "maxItems",
        "min_length",
        "max_length",
        "minLength",
        "maxLength",
        "pattern",
        "minimum",
        "maximum",
        "nullable",
        "any_of",
        "anyOf",
        "property_ordering",
        "propertyOrdering",
        "min_properties",
        "max_properties",
        "minProperties",
        "maxProperties",
        "additional_properties",
        "additionalProperties",
    }

    def _sanitize_schema_for_google(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize a JSON Schema fragment so it conforms to the subset accepted by
        google.genai.types.Schema. Translates numeric exclusiveMinimum/exclusiveMaximum
        (JSON Schema Draft 2020-12) into Google's supported minimum/maximum, and drops
        keywords Google's pydantic Schema model does not declare (which would otherwise
        raise extra_forbidden).
        """
        if not isinstance(schema, dict):
            return schema

        excl_min = schema.get("exclusiveMinimum")
        excl_max = schema.get("exclusiveMaximum")
        is_int = schema.get("type") == "integer"

        out: Dict[str, Any] = {}
        for k, v in schema.items():
            if k in ("exclusiveMinimum", "exclusiveMaximum"):
                continue
            if k == "title":
                continue
            if k not in self._GOOGLE_SCHEMA_ALLOWED:
                continue
            if k == "properties" and isinstance(v, dict):
                out[k] = {
                    pk: self._sanitize_schema_for_google(pv) for pk, pv in v.items()
                }
            elif k == "items" and isinstance(v, dict):
                out[k] = self._sanitize_schema_for_google(v)
            elif k in ("anyOf", "any_of") and isinstance(v, list):
                out[k] = [self._sanitize_schema_for_google(s) for s in v]
            else:
                out[k] = v

        if isinstance(excl_min, (int, float)) and not isinstance(excl_min, bool):
            if is_int:
                translated = int(excl_min) + 1
                out["minimum"] = (
                    max(out["minimum"], translated) if "minimum" in out else translated
                )
        if isinstance(excl_max, (int, float)) and not isinstance(excl_max, bool):
            if is_int:
                translated = int(excl_max) - 1
                out["maximum"] = (
                    min(out["maximum"], translated) if "maximum" in out else translated
                )

        return out

    def convert_mcp_to_google_tools(self, mcp_tools: List[Dict]) -> List[Dict]:
        """
        Convert MCP-formatted tools to Google GenAI function calling format.
        Args:
            mcp_tools: List of tools in MCP format
        Returns:
            List of function declarations for Google GenAI
        """
        function_declarations = []

        for tool in mcp_tools:
            input_schema = tool.get("inputSchema", {}) or {}
            sanitized_properties = {
                prop_name: self._sanitize_schema_for_google(prop_def)
                for prop_name, prop_def in input_schema.get("properties", {}).items()
            }
            function_declaration = {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": {
                    "type": input_schema.get("type", "object"),
                    "properties": sanitized_properties,
                    "required": input_schema.get("required", []),
                },
            }

            function_declarations.append(function_declaration)

        return function_declarations

    def _build_built_in_tools(
        self, built_in_tools: List[str], web_search_tool: str = "enterprise"
    ) -> List[types.Tool]:
        """Add built-in Google GenAI tools based on names."""
        google_built_in_tools: List[types.Tool] = []
        for tool in built_in_tools:
            if tool.lower() == "web_search":
                if web_search_tool == "google":
                    google_built_in_tools.append(
                        types.Tool(google_search=types.GoogleSearch())
                    )
                else:
                    google_built_in_tools.append(
                        types.Tool(enterprise_web_search=types.EnterpriseWebSearch())
                    )
            if tool.lower() == "websearch_retrieval":
                google_built_in_tools.append(
                    types.Tool(google_search_retrieval=types.GoogleSearchRetrieval())
                )
            if tool.lower() == "google_maps":
                google_built_in_tools.append(types.Tool(google_maps=types.GoogleMaps()))
            if tool.lower() == "code_execution":
                google_built_in_tools.append(
                    types.Tool(code_execution=types.ToolCodeExecution())
                )
        return google_built_in_tools

    def build_tools(
        self, param_map: Dict[str, Any]
    ) -> Tuple[List[types.Tool], Union[types.ToolConfig, None]]:

        tools = param_map.get("tools", [])
        built_in_tools = param_map.get("built_in_tools", [])
        tool_choice = param_map.get("tool_choice", {})
        web_search_tool = "enterprise"
        if any(
            isinstance(t, str) and t.lower() == "web_search"
            for t in (built_in_tools or [])
        ):
            web_search_tool = self._normalize_web_search_tool(
                param_map.get("web_search_tool", param_map.get("web_search_type"))
            )

        if tools:
            func_declarations = self.convert_mcp_to_google_tools(tools)
            tools = [types.Tool(function_declarations=func_declarations)]

        if built_in_tools:
            google_built_in_tools = self._build_built_in_tools(
                built_in_tools, web_search_tool=web_search_tool
            )
            tools.extend(google_built_in_tools)

        tool_config = self._create_tool_config(tool_choice, tools) if (tools) else None

        return tools, tool_config

    def _create_tool_config(
        self,
        tool_choice: Dict[str, str],
        tools: List[types.Tool],
    ) -> Union[types.ToolConfig, None]:
        """
        Create a tool configuration from the tool choice.
        SEMOSS tool_type options [auto, required, forced, none]
        Google GenAI tool_type options [AUTO, REQUIRED, FORCED, NONE]
        """

        tool_choice_type = tool_choice.get("type", "auto").lower()
        tool_choice_name = tool_choice.get("name")

        all_tool_names: List[str] = [
            func.name
            for tool in tools
            if tool.function_declarations
            for func in tool.function_declarations
            if func.name
        ]

        if tool_choice_type == "required":
            mode = types.FunctionCallingConfigMode.ANY
            allowed_function_names = (
                all_tool_names if tool_choice_name is None else [tool_choice_name]
            )
        elif tool_choice_type == "forced" and tool_choice_name:
            mode = types.FunctionCallingConfigMode.ANY
            allowed_function_names = [tool_choice_name]
        elif tool_choice_type == "none":
            mode = types.FunctionCallingConfigMode.NONE
            allowed_function_names = None
        else:
            mode = types.FunctionCallingConfigMode.AUTO
            allowed_function_names = None

        function_calling_config = types.FunctionCallingConfig(
            mode=mode,
            allowed_function_names=allowed_function_names,
        )

        return types.ToolConfig(function_calling_config=function_calling_config)
