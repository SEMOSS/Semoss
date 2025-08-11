from typing import List, Dict, Any, Tuple
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
)
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSImageContent,
    SEMOSSImageType,
)


class AnthropicMessageBuilder:

    def build_messages(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Tuple[List[AnthropicMessage], Dict[str, Any]]:
        """Convert SEMOSS messages to Anthropic messages and return the param map from the latest message"""
        anthropic_messages = []
        param_map = {}
        for i, message in enumerate(semoss_messages):
            is_last = i == len(semoss_messages) - 1
            # Get the role based on the SEMOSS message type
            role = self._message_type_to_role(message.type)

            content_parts = []
            # Handle text content
            if message.content:
                content_parts.append(self._build_text_content_part(message.content))

            if message.image_content:
                image_contents_parts = self._build_image_content_part(
                    message.image_content
                )
                content_parts.extend(image_contents_parts)

            anthropic_messages.append(
                AnthropicMessage(
                    role=role,
                    content=content_parts,
                )
            )

            if is_last:
                param_map = message.param_map
                param_map = self._clean_param_map(param_map)

        return anthropic_messages, param_map

    def _clean_param_map(self, param_map: Dict[str, Any]) -> Dict[str, Any]:
        """Remove any keys that are not needed in the param map."""
        keys_to_remove = ["history"]
        for key in keys_to_remove:
            param_map.pop(key, None)
        return param_map

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
