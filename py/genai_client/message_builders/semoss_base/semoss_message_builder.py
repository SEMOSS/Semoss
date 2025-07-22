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
        """
        Convert a list of input messages (as dictionaries) to a list of SEMOSSMessage objects.
        Right now I can get a param map from two sources:
            1. The param map passed with the ask
            2. The param map passed with the last message
        I need to update the final message param map with the param map passed with the ask and update the token name param
        """
        semoss_messages = []

        param_map.pop("question", None)

        for i, message in enumerate(input_messages):
            message_type = message.get("type")
            if message_type is None:
                raise ValueError("Message type cannot be None")

            content = self._get_content(message)
            msg_param_map = message.get("paramMap", {})

            # If this is the last message I have to update the param map
            if i == len(input_messages) - 1:
                msg_param_map = self._update_param_map(
                    msg_param_map, param_map, model_settings
                )

            semoss_message = SEMOSSMessage(
                type=message_type, content=content, param_map=msg_param_map
            )

            if message.get("imageInfos"):
                semoss_message.image_content = self._parse_image_content(
                    message["imageInfos"]
                )

            semoss_messages.append(semoss_message)
        return semoss_messages

    def _update_param_map(
        self,
        final_param_map: Dict[str, Any],
        ask_param_map: Dict[str, Any],
        model_settings: ModelSettings,
    ) -> Dict[str, Any]:
        """Update the last message param map with the param map passed with the ask and update the token name param"""
        final_param_map.update(ask_param_map)

        if (
            not model_settings.tokens_param_name
            or model_settings.tokens_param_name in final_param_map
        ):
            return final_param_map

        token_params = ["max_tokens", "max_completion_tokens", "max_new_tokens"]
        token_param = next((p for p in token_params if p in final_param_map), None)

        if token_param:
            final_param_map[model_settings.tokens_param_name] = final_param_map.pop(
                token_param
            )

        final_param_map.pop("model_name", None)

        return final_param_map

    def _get_content(self, message: Dict[str, Any]) -> str:
        """Get the content of the message based on its type."""
        message_type = message.get("type")
        if message_type is None:
            raise ValueError("Message type cannot be None")
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
