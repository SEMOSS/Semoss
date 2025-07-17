from typing import List, Dict, Any, Tuple
from google.genai.types import Content, Part
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSImageContent,
    SEMOSSImageType,
)
from .google_genai_models import GoogleRoles


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
                param_map = message.param_map
        return google_messages, param_map

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
                google_image_parts.append(Part.from_bytes(data=image.data))
            else:
                raise ValueError(f"Unsupported SEMOSSImageContent type: {image.type}")
        return google_image_parts
