import base64
import re
from typing import Any, List, Dict
from .semoss_models import (
    SEMOSSMediaContent,
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSMediaInputType,
    ModelSettings,
)


class SEMOSSMessageBuilder:

    def build_messages(
        self,
        input_messages: List[Dict],
        param_map: Dict[str, Any],
        model_settings: ModelSettings,
    ) -> List[SEMOSSMessage]:
        """Convert a list of input messages to SEMOSSMessage objects."""
        semoss_messages = []
        param_map.pop("question", None)

        for i, message in enumerate(input_messages):
            message_type = message.get("type")
            if message_type is None:
                raise ValueError("Message type cannot be None")

            content = self._get_content(message)

            # If this is the last message, update the param map
            if i == len(input_messages) - 1:
                updated_param_map = self._update_param_map(
                    param_map, model_settings, message
                )
            else:
                updated_param_map = message.get("paramMap", {})

            # Extract thinking block info from message and add to param_map
            if message_type == "RESPONSE_TOOL":
                if message.get("thinking"):
                    updated_param_map["thinking"] = message.get("thinking")
                if message.get("thinking_signature"):
                    updated_param_map["thinking_signature"] = message.get(
                        "thinking_signature"
                    )

            semoss_message = SEMOSSMessage(
                type=message_type, content=content, param_map=updated_param_map
            )

            # Handle tool calls from RESPONSE_TOOL messages
            if message_type == "RESPONSE_TOOL" and message.get("tool_responses"):
                tool_calls = []
                for tool_resp in message["tool_responses"]:
                    tool_calls.append(
                        {
                            "function": {
                                "name": tool_resp["name"],
                                "arguments": tool_resp["arguments"],
                            },
                            "id": str(tool_resp["id"]),
                            "type": "function",
                        }
                    )
                semoss_message.tool_calls = tool_calls

            # Handle tool execution results
            if message_type == "INPUT_TOOL_EXEC":
                semoss_message.tool_call_id = message.get("tool_call_id")
                semoss_message.content = message.get(
                    "inputUIPrompt", ""
                ) or message.get("content", "")

            if message.get("mediaInputs"):
                semoss_message.media_content = self._parse_media_content(
                    message["mediaInputs"]
                )

            semoss_message.tokens = message.get("tokens", 0)

            semoss_messages.append(semoss_message)

        return semoss_messages

    def _update_param_map(
        self,
        msg_param_map: Dict[str, Any],
        model_settings: ModelSettings,
        message: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Update the last message param map"""
        param_map = msg_param_map.copy()

        token_params = ["max_tokens", "max_completion_tokens", "max_new_tokens"]
        token_param = next((p for p in token_params if p in param_map), None)

        if token_param:
            param_map[model_settings.tokens_param_name] = param_map.pop(token_param)

        param_map.pop("model_name", None)

        # Adding system prompt to param map if exists in message
        system_prompt_params = ["system_prompt", "systemPrompt", "system", "context"]
        system_prompt_param = next(
            (p for p in system_prompt_params if p in message), None
        )

        if system_prompt_param:
            param_map["system_prompt"] = message[system_prompt_param]

        return param_map

    def _get_content(self, message: Dict[str, Any]) -> str:
        """Get the content of the message based on its type."""
        message_type = message.get("type")

        # Handle tool execution responses
        if message_type == "INPUT_TOOL_EXEC":
            return message.get("inputUIPrompt", "")

        role = self._get_role(message_type)
        if role == "user":
            return message.get("inputPrompt", "")
        elif role == "assistant":
            return message.get("content", "")
        else:
            raise ValueError(f"Unknown message type: {message_type}")

    def _get_role(self, input_type: SEMOSSMessageType) -> str:
        """Get the role based on the SEMOSS message type."""
        if input_type in [
            SEMOSSMessageType.INPUT_TEXT,
            SEMOSSMessageType.INPUT_MEDIA,
            SEMOSSMessageType.INPUT_TOOL_EXEC,
        ]:
            return "user"
        elif input_type in [
            SEMOSSMessageType.RESPONSE_TEXT,
            SEMOSSMessageType.RESPONSE_MEDIA,
            SEMOSSMessageType.RESPONSE_TOOL,
        ]:
            return "assistant"
        else:
            raise ValueError(f"Unknown message type: {input_type}")

    def _parse_media_content(
        self, media_info_list: List[Dict[str, str]]
    ) -> List[SEMOSSMediaContent]:
        """Parse image content into SEMOSSMediaContent objects."""
        semoss_media_contents = []
        for media_info in media_info_list:
            mime_type = media_info.get("mimeType", None)
            file_format = media_info.get("fileFormat", None)
            file_name = media_info.get("fileName", None)
            url = media_info.get("sourceUrl", None)
            base_64_data = media_info.get("base64Data", None)

            # Check base64Data FIRST - if we have base64 data, use it regardless of URL
            if base_64_data:
                input_type = SEMOSSMediaInputType.BASE64
                data = base_64_data
            elif url:
                # Check if the URL is actually base64 data
                if self._is_base64_data(url):
                    input_type = SEMOSSMediaInputType.BASE64
                    data = url
                else:
                    input_type = SEMOSSMediaInputType.URL
                    data = url
            else:
                raise ValueError("Image content must have either a URL or base64 data.")

            semoss_media_contents.append(
                SEMOSSMediaContent(
                    type=input_type,
                    data=data,
                    format=file_format,
                    mime_type=mime_type,
                    file_name=file_name,
                    url=url,
                )
            )

        return semoss_media_contents

    def _is_base64_data(self, value: str) -> bool:
        """
        Check if a string value is actually base64 data rather than a URL.

        Handles two cases:
        1. Data URI format: 'data:<mime_type>;base64,<data>'
        2. Raw base64 string: A string containing only valid base64 characters

        Returns True if the value appears to be base64 data, False otherwise.
        """
        if not value or not isinstance(value, str):
            return False

        # Check for data URI scheme (e.g., 'data:image/png;base64,...')
        if value.startswith("data:") and ";base64," in value:
            return True

        # Check if it looks like a URL (has a scheme like http://, https://, file://, etc.)
        url_pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://")
        if url_pattern.match(value):
            return False

        # Check if the string is valid base64
        # Base64 strings only contain A-Z, a-z, 0-9, +, /, and = for padding
        # They should also have a length that is a multiple of 4 (with padding)
        base64_pattern = re.compile(r"^[A-Za-z0-9+/]*={0,2}$")

        # Remove any whitespace that might be in the base64 string
        cleaned_value = value.replace("\n", "").replace("\r", "").replace(" ", "")

        # Check if the string matches base64 pattern and has reasonable length
        # (at least 20 chars to avoid false positives with short strings)
        if len(cleaned_value) >= 20 and base64_pattern.match(cleaned_value):
            # Try to decode it to verify it's valid base64
            try:
                base64.b64decode(cleaned_value, validate=True)
                return True
            except Exception:
                return False

        return False
