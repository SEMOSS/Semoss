from typing import List, Dict, Any, Tuple, Union
import base64
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
    BedrockMessage,
    BedrockImageBlock,
    BedrockImageSource,
    BedrockInferenceConfig,
    BedrockRequest,
    BedrockSystemBlock,
    BedrockTextContentBlock,
    BedrockImageContentBlock,
)


class BedrockMessageBuilder:
    def build_messages(
        self, semoss_messages: List[SEMOSSMessage], system_prompt: str = None
    ) -> Dict[str, Any]:
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
                inference_config, param_map = self._build_request_parameters(
                    message.param_map
                )
                system_block = self.build_system_block(system_prompt)
                param_map = self.clean_param_map(param_map)

        return BedrockRequest(
            messages=bedrock_messages,
            system=system_block,
            inferenceConfig=inference_config,
            additionalModelRequestFields=param_map,
        ).model_dump(exclude_none=True, by_alias=True)

    def clean_param_map(self, param_map: Dict[str, Any]) -> Dict[str, Any]:
        param_map.pop("max_completion_tokens", None)
        param_map.pop("max_tokens", None)
        param_map.pop("max_new_tokens", None)
        param_map.pop("model_name", None)
        param_map.pop("history", None)
        param_map.pop("use_history", None)
        param_map.pop("context", None)
        param_map.pop("image_url", None)
        param_map.pop("image_encoded", None)
        return param_map

    def _message_type_to_role(self, message_type: SEMOSSMessageType) -> str:
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
            return "user"
        elif message_type in assistant_message_types:
            return "assistant"
        else:
            raise ValueError(f"Unknown SEMOSS message type: {message_type}")

    def _build_text_content_block(self, content: str) -> BedrockTextContentBlock:
        """Build a text content block."""
        return BedrockTextContentBlock(text=content)

    def _build_image_blocks(
        self, image_content: List[SEMOSSImageContent] = []
    ) -> List[BedrockImageContentBlock]:

        bedrock_content_blocks = []
        for image in image_content:
            if image.type == SEMOSSImageType.URL:
                image_block = self._build_url_image_content(image)
                content_block = BedrockImageContentBlock(image=image_block)
                bedrock_content_blocks.append(content_block)
            elif image.type == SEMOSSImageType.BASE64:
                image_block = self._build_base64_image_content(image)
                content_block = BedrockImageContentBlock(image=image_block)
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

        try:
            img_bytes = base64.b64decode(img_bytes)
        except Exception as e:
            raise ValueError(f"Could not decode base64 image data: {e}")

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

        if image_content.data.startswith("data:"):
            base64_data = image_content.data.split(",")[1]
        else:
            base64_data = image_content.data

        try:
            image_bytes = base64.b64decode(base64_data)
        except Exception as e:
            raise ValueError(f"Could not decode base64 image data: {e}")

        image_source = BedrockImageSource(bytes=image_bytes)
        return BedrockImageBlock(source=image_source, format=media_type)

    def _build_request_parameters(
        self, param_map: Dict[str, Any]
    ) -> Tuple[BedrockInferenceConfig, Dict[str, Any]]:
        max_tokens = (
            param_map.pop("max_tokens", None)
            or param_map.pop("max_completion_tokens", None)
            or param_map.pop("max_new_tokens", None)
        )
        stop_sequences = param_map.pop("stop_sequences", None) or param_map.pop(
            "stop_sequence",
            None,
        )
        temperature = param_map.pop("temperature", None)
        top_p = param_map.pop("top_p", None) or param_map.pop(
            "topP",
            None,
        )

        return (
            BedrockInferenceConfig(
                maxTokens=max_tokens,
                stopSequences=stop_sequences,
                temperature=temperature,
                topP=top_p,
            ),
            param_map,
        )

    def build_system_block(
        self, system_prompt: str = None
    ) -> Union[List[BedrockSystemBlock], None]:
        """Build a system content block."""
        if system_prompt:
            return [BedrockSystemBlock(text=system_prompt)]
        else:
            return None
