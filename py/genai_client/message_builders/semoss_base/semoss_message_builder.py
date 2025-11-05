from typing import Any, List, Dict
from .semoss_models import (
    SEMOSSImageContent,
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSImageType,
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
                semoss_message.content = message.get("inputUIPrompt", "")

            if message.get("imageInfos"):
                semoss_message.image_content = self._parse_image_content(
                    message["imageInfos"]
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

    def _parse_image_content(
        self, image_infos: List[Dict[str, str]]
    ) -> List[SEMOSSImageContent]:
        """Parse image content into SEMOSSImageContent objects."""
        semoss_image_contents = []
        for image_info in image_infos:
            mime_type = image_info.get("mimeType", None)
            img_format = image_info.get("format", None)
            file_name = image_info.get("fileName", None)
            url = image_info.get("imageUrl", None)
            base_64_data = image_info.get("base64Data", None)

            if url:
                img_type = SEMOSSImageType.URL
            elif base_64_data:
                img_type = SEMOSSImageType.BASE64
            else:
                raise ValueError("Image content must have either a URL or base64 data.")

            if type == SEMOSSImageType.URL:
                data = url
            else:
                data = base_64_data

            semoss_image_contents.append(
                SEMOSSImageContent(
                    type=img_type,
                    data=data,
                    format=img_format,
                    mime_type=mime_type,
                    file_name=file_name,
                    url=url,
                )
            )

        return semoss_image_contents
