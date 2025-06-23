from typing import List, Dict, Any, Tuple
from google.genai.types import Content, Part
from ...utils import (
    get_image_extension,
    fetch_and_encode_image,
    image_to_base64,
)
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
        google_messages = []
        param_map = {}

        for i, message in enumerate(semoss_messages):
            parts = []
            is_last = i == len(semoss_messages) - 1
            role = self._message_type_to_role(message.type)
            content = message.content
            if content:
                parts = [Part.from_text(text=content)]

            if message.image_content:
                pass

            # TODO: Handle tool calls and responses..

            google_messages.append(
                Content(
                    role=role,
                    parts=parts,
                )
            )

            if is_last:
                param_map = message.param_map
        return google_messages, param_map

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
