from typing import List, Dict, Any, Tuple, Union
from ...utils import (
    get_image_extension,
    fetch_and_encode_image,
)
from .openai_models import (
    OpenAIRoles,
    OpenAIMessage,
    OpenAIImageURL,
    OpenAIImageContentPart,
    OpenAITextContentPart,
    OpenAIImageDetail,
)
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSImageContent,
    SEMOSSImageType,
)
from ...text_generation.openai_clients.abstract_openai_client import ModelSettings


class OpenAIMessageBuilder:

    def __init__(self, model_settings: ModelSettings):
        """Initialize the OpenAI message builder with a specific model name."""
        self.model_settings = model_settings

    def build_messages(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Tuple[List[OpenAIMessage], Dict[str, Any]]:
        """Convert SEMOSS messages to OpenAI messages and return the param map from the latest message"""
        openai_messages = []
        param_map = {}

        for i, message in enumerate(semoss_messages):
            is_last = i == len(semoss_messages) - 1
            # Get the role based on the SEMOSS message type
            role = self._message_type_to_role(message.type)

            content_parts = []

            # Handle text content
            if message.content:
                content_parts.append(self._build_text_content_part(message.content))

            # Handle image content
            if message.image_content:
                image_content_parts = self._build_image_content_parts(
                    message.image_content
                )
                content_parts.extend(image_content_parts)

            if len(content_parts) == 1 and isinstance(
                content_parts[0], OpenAITextContentPart
            ):
                content = content_parts[0].text
            else:
                content = content_parts

            openai_messages.append(
                OpenAIMessage(
                    role=role,
                    content=content,
                )
            )

            if is_last:
                param_map = message.param_map

        return openai_messages, param_map

    def build_request(self, semoss_messages: List[SEMOSSMessage]) -> Dict[str, Any]:
        """Build complete OpenAI request with messages and parameters"""

        messages, param_map = self.build_messages(semoss_messages)
        request_params = self.build_request_parameters(param_map)

        # Convert messages to dict format for the request
        messages_dict = []
        for message in messages:
            msg_dict = {"role": message.role}

            if isinstance(message.content, str):
                msg_dict["content"] = message.content
            else:
                # Convert content parts to dict format
                content_list = []
                for part in message.content:
                    if isinstance(part, OpenAITextContentPart):
                        content_list.append({"type": "text", "text": part.text})
                    elif isinstance(part, OpenAIImageContentPart):
                        content_list.append(
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": part.image_url.url,
                                    "detail": part.image_url.detail,
                                },
                            }
                        )
                msg_dict["content"] = content_list

            messages_dict.append(msg_dict)

        request_params["messages"] = messages_dict

        # Apply final model-specific kwargs handling
        request_params = self._update_model_specific_kwargs(**request_params)

        return request_params

    def _message_type_to_role(self, message_type: SEMOSSMessageType) -> str:
        """Convert SEMOSS message type to OpenAI role."""
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
            if self.model_settings.user_role:
                return self.model_settings.user_role
            else:
                # DEFAULT USER ROLE
                return OpenAIRoles.USER.value
        elif message_type in assistant_message_types:
            if self.model_settings.ai_role:
                return self.model_settings.ai_role
            else:
                # DEFAULT ASSISTANT ROLE
                return OpenAIRoles.ASSISTANT.value
        else:
            raise ValueError(f"Unknown message type: {message_type}")

    def _build_text_content_part(self, content: str) -> OpenAITextContentPart:
        """Build OpenAI text content part"""
        return OpenAITextContentPart(text=content)

    def _build_image_content_parts(
        self, image_content: List[SEMOSSImageContent] = []
    ) -> List[OpenAIImageContentPart]:
        """Build OpenAI image content parts from SEMOSS image content."""
        openai_image_parts = []

        for image in image_content:
            if image.type == SEMOSSImageType.URL:
                openai_image_parts.append(self._build_url_image_content(image))
            elif image.type == SEMOSSImageType.BASE64:
                openai_image_parts.append(self._build_base64_image_content(image))
            else:
                raise ValueError(f"Unknown image type: {image.type}")

        return openai_image_parts

    def _build_url_image_content(
        self, image_content: SEMOSSImageContent
    ) -> OpenAIImageContentPart:
        """Build OpenAI image content part from URL"""
        if not image_content.url:
            raise ValueError(
                "The image type was specified as URL but no URL was provided."
            )

        image_url = OpenAIImageURL(url=image_content.url, detail=OpenAIImageDetail.AUTO)

        return OpenAIImageContentPart(image_url=image_url)

    def _build_base64_image_content(
        self, image_content: SEMOSSImageContent
    ) -> OpenAIImageContentPart:
        """Build OpenAI image content part from base64"""
        if not image_content.data:
            raise ValueError(
                "The image type was specified as base64 but no data was provided."
            )

        if not image_content.mime_type:
            image_content.mime_type = get_image_extension(image_content.data)

        if image_content.mime_type == "image/jpg":
            image_content.mime_type = "image/jpeg"

        # OpenAI expects base64 images in data URI format
        data_uri = f"data:{image_content.mime_type};base64,{image_content.data}"

        image_url = OpenAIImageURL(url=data_uri, detail=OpenAIImageDetail.AUTO)

        return OpenAIImageContentPart(image_url=image_url)

    def build_request_parameters(self, param_map: Dict[str, Any]) -> Dict[str, Any]:
        """Build OpenAI request parameters from param_map with model-specific handling"""
        # Extract common parameters that OpenAI supports
        max_tokens = (
            param_map.get("max_tokens", None)
            or param_map.get("max_completion_tokens", None)
            or param_map.get("max_new_tokens", None)
        )

        temperature = param_map.get("temperature", None)
        top_p = param_map.get("top_p", None)
        frequency_penalty = param_map.get("frequency_penalty", None)
        presence_penalty = param_map.get("presence_penalty", None)
        stop = param_map.get("stop", None) or param_map.get("stop_sequences", None)
        stream = param_map.get("stream", None)
        logit_bias = param_map.get("logit_bias", None)
        user = param_map.get("user", None)

        # Build the parameters dict, only including non-None values
        params = {"model": self.model_settings.model_name}

        if max_tokens is not None:
            params["max_tokens"] = max_tokens
        if temperature is not None:
            params["temperature"] = temperature
        if top_p is not None:
            params["top_p"] = top_p
        if frequency_penalty is not None:
            params["frequency_penalty"] = frequency_penalty
        if presence_penalty is not None:
            params["presence_penalty"] = presence_penalty
        if stop is not None:
            params["stop"] = stop
        if stream is not None:
            params["stream"] = stream
        if logit_bias is not None:
            params["logit_bias"] = logit_bias
        if user is not None:
            params["user"] = user

        # Apply model-specific updates
        params = self._update_model_specific_kwargs(**params)

        return params

    def _update_model_specific_kwargs(self, **kwargs) -> dict:
        """
        Update the kwargs based on the model name to ensure compatibility with the model's capabilities.
        Returns:
            dict: Updated kwargs
        """
        updated_kwargs = kwargs.copy()

        # Handle o1-mini (doesn't support system/developer roles)
        if self.model_settings.model_name.startswith("o1-mini"):
            # Remove temperature - only 1.0 is supported
            if "temperature" in updated_kwargs and updated_kwargs["temperature"] != 1.0:
                del updated_kwargs["temperature"]

            updated_kwargs["stream"] = False

            # Convert system/developer messages to user messages
            if "messages" in updated_kwargs:
                messages = updated_kwargs["messages"]
                for i, msg in enumerate(messages):
                    if msg.get("role") in ["system", "developer"]:
                        original_role = msg.get("role").upper()
                        messages[i]["role"] = "user"
                        messages[i][
                            "content"
                        ] = f"{original_role}: {messages[i]['content']}"
                updated_kwargs["messages"] = messages

        # Handle regular o1 models
        elif (
            self.model_settings.model_name == "o1"
            or self.model_settings.model_name.startswith("o1-preview")
        ):
            # Temperature - only 1.0 is supported
            if "temperature" in updated_kwargs and updated_kwargs["temperature"] != 1.0:
                del updated_kwargs["temperature"]

            updated_kwargs["stream"] = False

        # Handle o3-mini
        elif self.model_settings.model_name.startswith("o3-mini"):
            # Remove temperature - only 1.0 is supported
            if "temperature" in updated_kwargs and updated_kwargs["temperature"] != 1.0:
                del updated_kwargs["temperature"]

            updated_kwargs["stream"] = False

        return updated_kwargs