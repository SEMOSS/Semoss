from typing import List, Dict, Any, Tuple
from ...utils import (
    get_image_extension,
    fetch_and_encode_image,
)
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSImageContent,
    SEMOSSImageType,
)
from .bedrock_models import (
    BedrockRoles,
    BedrockMessage,
    ContentBlock,
    BedrockImageBlock,
    BedrockImageSource,
    BedrockDocumentBlock,
    BedrockDocumentSource,
)


class BedrockMessageBuilder:
    def build_messages(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Tuple[List[BedrockMessage], Dict[str, Any]]:
        bedrock_messages = []
        param_map = {}
        for i, message in enumerate(semoss_messages):
            is_last = i == len(semoss_messages) - 1
            role = self._message_type_to_role(message.type)

            content_blocks = []

            if message.content:
                content_blocks.append(self._build_text_content_block(message.content))

            if message.image_content:
                image_blocks = self._build_image_blocks(message.image_content)
                content_blocks.extend(image_blocks)

            bedrock_messages.append(
                BedrockMessage(
                    role=role,
                    content=content_blocks,
                )
            )

            if is_last:
                param_map = message.param_map

        return bedrock_messages, param_map

    def _message_type_to_role(self, message_type: SEMOSSMessageType) -> BedrockRoles:
        """Convert SEMOSS message type to Bedrock role."""
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
            return BedrockRoles.USER
        elif message_type in assistant_message_types:
            return BedrockRoles.ASSISTANT
        else:
            raise ValueError(f"Unknown SEMOSS message type: {message_type}")

    def _build_text_content_block(self, content: str) -> ContentBlock:
        """Build a text content block."""
        return ContentBlock(text=content)

    def _build_image_blocks(
        self, image_content: List[SEMOSSImageContent] = []
    ) -> List[ContentBlock]:

        bedrock_content_blocks = []
        for image in image_content:
            if image.type == SEMOSSImageType.URL:
                image_block = self._build_url_image_content(image)
                content_block = ContentBlock(image=image_block)
                bedrock_content_blocks.append(content_block)
            elif image.type == SEMOSSImageType.BASE64:
                image_block = self._build_base64_image_content(image)
                content_block = ContentBlock(image=image_block)
                bedrock_content_blocks.append(content_block)
            else:
                raise ValueError(f"Unsupported SEMOSS image type: {image.type}")

        return bedrock_content_blocks

    def _build_url_image_content(
        self, image_content: SEMOSSImageContent
    ) -> BedrockImageBlock:
        """Build a Bedrock image block from a URL."""
        img_bytes, media_type = fetch_and_encode_image(image_content.url)
        if media_type == "image/jpg":
            media_type = "image/jpeg"

        media_type = media_type.split("/")[-1].lower()

        image_source = BedrockImageSource(bytes=img_bytes)
        return BedrockImageBlock(source=image_source, format=media_type)

    def _build_base64_image_content(
        self, image_content: SEMOSSImageContent
    ) -> BedrockImageBlock:
        """Build a Bedrock image block from base64 data."""
        if not image_content.data:
            raise ValueError("Base64 image content requires 'data' field.")

        if not image_content.mime_type:
            image_content.mime_type = get_image_extension(image_content.data)

        if image_content.mime_type == "image/jpg":
            image_content.mime_type = "image/jpeg"
        media_type = image_content.mime_type.split("/")[-1].lower()

        image_source = BedrockImageSource(bytes=image_content.data)
        return BedrockImageBlock(source=image_source, format=media_type)
