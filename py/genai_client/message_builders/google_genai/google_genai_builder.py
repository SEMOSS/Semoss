from typing import List, Dict, Any, Tuple, Union
from google.genai.types import Content, Part
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSImageContent,
    SEMOSSImageType,
)
from .google_genai_models import GoogleRoles
from google.genai import types


class GoogleGenAIMessageBuilder:

    def build_messages(self, semoss_messages: List[SEMOSSMessage]) -> Dict[str, Any]:
        """Convert SEMOSS messages to Google GenAI Content."""
        google_messages = []
        param_map = {}
        stream = False

        pending_tool_responses = []
        expected_tool_count = 0

        for i, message in enumerate(semoss_messages):
            parts = []

            if (
                message.type == SEMOSSMessageType.INPUT_TEXT
                or message.type == SEMOSSMessageType.INPUT_MEDIA
            ):
                if message.content:
                    parts.append(self._build_text_content_part(message.content))

                if message.image_content:
                    parts.extend(self._build_image_content_parts(message.image_content))

                google_messages.append(
                    Content(
                        role=GoogleRoles.USER,
                        parts=parts,
                    )
                )

            elif message.type == SEMOSSMessageType.RESPONSE_TOOL:
                if message.tool_calls:
                    expected_tool_count = len(message.tool_calls)

                    for tool_call in message.tool_calls:
                        parts.append(
                            Part.from_function_call(
                                name=tool_call["function"]["name"],
                                args=tool_call["function"]["arguments"],
                            )
                        )

                    google_messages.append(
                        Content(
                            role=GoogleRoles.MODEL,
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
                                    role=GoogleRoles.USER,
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
                        role=GoogleRoles.MODEL,
                        parts=parts,
                    )
                )

            if i == len(semoss_messages) - 1:
                param_map, stream = self._convert_args_to_provider_config(
                    **message.param_map
                )

        return {
            "messages": google_messages,
            "param_map": param_map,
            "stream": stream,
        }

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
            function_declaration = {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": {
                    "type": tool["inputSchema"]["type"],
                    "properties": {},
                    "required": tool["inputSchema"].get("required", []),
                },
            }

            for prop_name, prop_def in tool["inputSchema"]["properties"].items():
                function_declaration["parameters"]["properties"][prop_name] = {
                    k: v for k, v in prop_def.items() if k != "title"
                }

            function_declarations.append(function_declaration)

        return function_declarations

    def _convert_args_to_provider_config(
        self, **kwargs
    ) -> Tuple[types.GenerateContentConfig, bool]:
        """
        Convert our CFG arguments to a GenerateContentConfig object.
        """
        context = kwargs.pop("context", None)

        structured_response_schema = kwargs.pop("schema", None)

        response_mime_type = kwargs.pop("response_mime_type", None)
        if structured_response_schema is not None and response_mime_type is None:
            response_mime_type = "application/json"

        tools = kwargs.pop("tools", None)
        if tools is not None:
            func_declarations = self.convert_mcp_to_google_tools(tools)

            tools = [types.Tool(function_declarations=func_declarations)]

        tool_choice = kwargs.pop("tool_choice", None)
        if tool_choice is not None and tools is not None:
            tool_config = self._create_tool_config(tool_choice, tools)
        else:
            tool_config = None

        max_output_tokens = kwargs.get("max_new_tokens", None)
        if max_output_tokens is None:
            max_output_tokens = kwargs.get("max_completion_tokens", None)
        if max_output_tokens is None:
            max_output_tokens = kwargs.get("max_tokens", None)

        stream = kwargs.pop("stream", False)
        if not stream:
            kwargs.pop("streaming", None)

        config = types.GenerateContentConfig(
            http_options=kwargs.pop("http_options", None),
            system_instruction=context,
            max_output_tokens=max_output_tokens,
            temperature=kwargs.pop("temperature", None),
            top_p=kwargs.pop("top_p", None),
            top_k=kwargs.pop("top_k", None),
            stop_sequences=kwargs.pop("stop_sequences", None),
            presence_penalty=kwargs.pop("presence_penalty", None),
            frequency_penalty=kwargs.pop("frequency_penalty", None),
            # TODO: Pass this from the init.. this lives in smss
            safety_settings=None,
            response_schema=structured_response_schema,
            response_mime_type=response_mime_type,
            tools=tools,
            tool_config=tool_config,
        )
        return config, stream

    def _create_tool_config(
        self, tool_choice: Dict[str, str], tools: List[types.Tool]
    ) -> Union[types.ToolConfig, None]:
        """
        Create a tool configuration from the tool choice.
        SEMOSS tool_type options [auto, required, forced, none]
        Google GenAI tool_type options [AUTO, REQUIRED, FORCED, NONE]
        """
        tool_type = tool_choice.get("type", "auto").lower()
        tool_name = tool_choice.get("name", None)

        all_tool_names = [
            name
            for tool in tools
            for func in tool.function_declarations
            for name in [func.name]
        ]

        if tool_type == "auto":
            mode = types.FunctionCallingConfigMode.AUTO
            allowed_function_names = None
        elif tool_type == "required":
            mode = types.FunctionCallingConfigMode.ANY
            allowed_function_names = (
                all_tool_names if tool_name is None else [tool_name]
            )
        elif tool_type == "forced":
            mode = types.FunctionCallingConfigMode.ANY
            allowed_function_names = [tool_name] if tool_name else None
        elif tool_type == "none":
            mode = types.FunctionCallingConfigMode.NONE
            allowed_function_names = None
        else:
            return None

        function_calling_config = types.FunctionCallingConfig(
            mode=mode,
            allowed_function_names=allowed_function_names,
        )

        return types.ToolConfig(function_calling_config=function_calling_config)

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

    def _build_image_content_parts(
        self, image_content: List[SEMOSSImageContent]
    ) -> List[Part]:
        """Convert SEMOSS image content to Google GenAI Part."""
        google_image_parts = []
        for image in image_content:
            if image.type == SEMOSSImageType.URL and image.url:
                google_image_parts.append(Part.from_uri(file_uri=image.url))
            elif image.type == SEMOSSImageType.BASE64:
                if not image.mime_type or not image.data:
                    raise ValueError(
                        f"Missing required base64 data or mime type when building Google GenAI image part."
                    )
                google_image_parts.append(
                    Part.from_bytes(data=image.data, mime_type=image.mime_type)
                )
            else:
                raise ValueError(f"Unsupported SEMOSSImageContent type: {image.type}")
        return google_image_parts

    def _handle_tools_conversion(self, tools: List[Dict]) -> List[types.Tool]:
        """
        Converting from the OpenAI tools format I recieve to the Google Gen AI tools format.
        This is only used when I don't get the messages as message_json.
        Therefore I need to assume they are in OpenAI format
        """
        google_tools = []

        for tool in tools:
            if tool.get("type", None) == "function":
                func_def = tool["function"]

                parameters_schema = None
                if "parameters" in func_def:
                    params = func_def["parameters"]

                    properties = {}
                    for prop_name, prop_def in params.get("properties", {}).items():
                        properties[prop_name] = types.Schema(
                            type=prop_def["type"].upper(),
                            description=prop_def.get("description", ""),
                        )

                    parameters_schema = types.Schema(
                        type="OBJECT",
                        properties=properties,
                        required=params.get("required", []),
                    )

                function_declaration = types.FunctionDeclaration(
                    name=func_def["name"],
                    description=func_def["description"],
                    parameters=parameters_schema,
                )

                google_tools.append(
                    types.Tool(function_declarations=[function_declaration])
                )

        return google_tools
