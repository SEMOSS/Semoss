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

            # If this is the last message, update the param map
            if i == len(input_messages) - 1:
                updated_param_map = self._update_param_map(
                    param_map, model_settings, message
                )
            else:
                updated_param_map = message.get("paramMap", {})

            # ---- Parts-based support (schemaVersion 2) ----
            parts = message.get("parts") or []
            schema_version = message.get("schemaVersion", 1)

            if isinstance(parts, list) and parts and schema_version == 2:
                tool_result_parts = [
                    p
                    for p in parts
                    if isinstance(p, dict) and p.get("type") == "TOOL_RESULT"
                ]
                if tool_result_parts:
                    tokens = message.get("tokens", 0)
                    for tr_part in tool_result_parts:
                        tr = tr_part.get("toolResult") or {}
                        tool_call_id = tr.get("toolCallId") or message.get(
                            "tool_call_id"
                        )
                        output = tr.get("output") or message.get("inputUIPrompt", "")
                        semoss_message = SEMOSSMessage(
                            type=SEMOSSMessageType.INPUT_TOOL_EXEC,
                            content=output,
                            param_map=updated_param_map,
                            tokens=tokens,
                        )
                        semoss_message.tool_call_id = tool_call_id
                        semoss_messages.append(semoss_message)
                    continue

                content = self._get_content_from_parts(parts)
                media_content = self._parse_media_from_parts(parts)

                semoss_message = SEMOSSMessage(
                    type=message_type, content=content, param_map=updated_param_map
                )

                if media_content:
                    semoss_message.media_content = media_content

                semoss_message.tokens = message.get("tokens", 0)
                semoss_messages.append(semoss_message)
                continue

            # ---- Legacy format (schemaVersion 1 or no schemaVersion) ----
            content = self._get_content(message)

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

            if message_type == "INPUT_TOOL_EXEC":
                semoss_message.tool_call_id = message.get("tool_call_id")
                semoss_message.content = message.get("inputUIPrompt", "")

            if message.get("mediaInputs"):
                semoss_message.media_content = self._parse_media_content(
                    message["mediaInputs"]
                )

            semoss_message.tokens = message.get("tokens", 0)

            semoss_messages.append(semoss_message)

        return semoss_messages

    def _get_content_from_parts(self, parts: List[Dict]) -> str:
        """Extract text content from schemaVersion 2 parts array."""
        text_parts = []
        for part in parts:
            if isinstance(part, dict) and part.get("type") == "TEXT":
                text = part.get("text") or part.get("uiText") or ""
                if text:
                    text_parts.append(text)
        return "\n".join(text_parts) if text_parts else ""

    def _parse_media_from_parts(self, parts: List[Dict]) -> List[SEMOSSMediaContent]:
        """Extract media content from schemaVersion 2 parts array."""
        media_contents = []
        for part in parts:
            if isinstance(part, dict) and part.get("type") == "MEDIA":
                media_info = part.get("mediaInfo", {})
                if not media_info:
                    continue

                mime_type = media_info.get("mimeType")
                file_format = media_info.get("fileFormat")
                file_name = media_info.get("fileName")
                url = media_info.get("sourceUrl")
                base_64_data = media_info.get("base64Data")

                if url:
                    input_type = SEMOSSMediaInputType.URL
                    data = url
                elif base_64_data:
                    input_type = SEMOSSMediaInputType.BASE64
                    data = base_64_data
                else:
                    # Skip if no valid data source
                    continue

                media_contents.append(
                    SEMOSSMediaContent(
                        type=input_type,
                        data=data,
                        format=file_format,
                        mime_type=mime_type,
                        file_name=file_name,
                        url=url,
                    )
                )

        return media_contents if media_contents else None

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

            if url:
                input_type = SEMOSSMediaInputType.URL
            elif base_64_data:
                input_type = SEMOSSMediaInputType.BASE64
            else:
                raise ValueError("Image content must have either a URL or base64 data.")

            if type == SEMOSSMediaInputType.URL:
                data = url
            else:
                data = base_64_data

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
