from typing import List, Dict, Any, Tuple
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

    def build_messages(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Tuple[List[Content], Dict[str, Any]]:
        """Convert SEMOSS messages to Google GenAI Content and return the param map from the latest message."""
        google_messages = []
        param_map = {}

        for i, message in enumerate(semoss_messages):
            parts = []

            content = message.content
            if content:
                parts.extend([self._build_text_content_part(content)])

            if message.image_content:
                parts.extend(self._build_image_content_parts(message.image_content))

            # TODO: Handle tool calls and responses..

            role = self._message_type_to_role(message.type)
            google_messages.append(
                Content(
                    role=role,
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
        response_schema = kwargs.pop("schema", None)
        response_mime_type = kwargs.pop("response_mime_type", None)
        if response_schema is not None and response_mime_type is None:
            response_mime_type = "application/json"

        tools = kwargs.pop("tools", None)
        if tools is not None:
            func_declarations = self.convert_mcp_to_google_tools(tools)

            tools = [types.Tool(function_declarations=func_declarations)]

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
            response_schema=response_schema,
            response_mime_type=response_mime_type,
            tools=tools,
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
