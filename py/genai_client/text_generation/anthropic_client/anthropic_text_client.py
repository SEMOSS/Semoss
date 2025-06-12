from typing import List, Optional, Dict, Any, Union, Tuple
from pydantic import BaseModel
from ...clients.google_clients import (
    GoogleClient,
    GoogleClientConfig,
    GoogleClientType,
)

from ...utils import StringEnum, classify_url
from ...constants import AskModelEngineResponse, TEMPLATE, TEMPLATE_NAME, FULL_PROMPT
from ..abstract_text_generation_client import AbstractTextGenerationClient, AskSettings


class Roles(StringEnum):
    USER = "user"
    ASSISTANT = "assistant"


class ContentType(StringEnum):
    TEXT = "text"
    IMAGE = "image"


class ImageType(StringEnum):
    URL = "url"
    BASE64 = "base64"


class ImageMediaType(StringEnum):
    JPEG = "image/jpeg"
    PNG = "image/png"
    WEBP = "image/webp"
    GIF = "image/gif"


class ImageSource(BaseModel):
    type: ImageType
    media_type: ImageMediaType
    data: Optional[str] = None
    url: Optional[str] = None


class ImageContentPart(BaseModel):
    type: str = "image"
    image: ImageSource


class DocumentContentPart(BaseModel):
    type: str = "document"
    media_type: str
    data: Optional[str] = None


# FOR HISTORY
class ToolContentPart(BaseModel):
    name: str
    tool_use_id: str
    content: str


class ThinkingContentPart(BaseModel):
    type: str = "thinking"
    thinking: str
    signature: Optional[str] = None


class TextContentPart(BaseModel):
    type: str = "text"
    text: str


class Message(BaseModel):
    role: Roles
    content: Union[
        str,
        List[
            Union[
                TextContentPart,
                ImageContentPart,
                ToolContentPart,
                ThinkingContentPart,
                DocumentContentPart,
            ]
        ],
    ]


class AnthropicRequestConfig(BaseModel):
    model: str
    messages: List[Dict[str, Any]]
    system: Optional[str] = None
    stream: Optional[bool] = None
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    top_k: Optional[int] = None
    top_p: Optional[float] = None
    container: Optional[str] = None
    stop_sequences: Optional[List[str]] = None


class AnthropicTextClient(AbstractTextGenerationClient):
    def __init__(
        self,
        provider: str,
        **kwargs,
    ):
        super().__init__(
            template=kwargs.pop(TEMPLATE, None),
            template_name=kwargs.pop(TEMPLATE_NAME, None),
            **kwargs,
        )

        self.provider = provider.lower()
        self.client = self._get_client(**kwargs)

    def _get_client(self, **kwargs):
        if self.provider == "google":
            self.client_config = GoogleClientConfig(
                type=GoogleClientType.ANTHROPIC,
                service_account_credentials=kwargs.pop(
                    "service_account_credentials", None
                ),
                service_account_key_file=kwargs.pop("service_account_key_file", None),
                region=kwargs.pop("region", None),
                project=kwargs.pop("project", None),
                api_key=kwargs.pop("api_key", None),
            )
            return GoogleClient(config=self.client_config).client
        else:
            raise ValueError(
                f"Provider '{self.provider}' is not supported for Anthropic Text Client."
            )

    def ask_call(
        self,
        question: str = None,
        context: str = None,
        use_history: bool = True,
        history: List[Dict] = None,
        prefix="",
        **kwargs,
    ):
        if self.client is None:
            raise ValueError("Anthropic client is not initialized.")

        self.ask_settings = self.get_ask_settings(history, use_history, **kwargs)

        converted_history, system_prompt_from_history = self._convert_history(
            question=question,
        )

        self.request_config = self._convert_args_to_provider_config(
            context=context,
            history=converted_history,
            **kwargs,
        )

        response = self.client.messages.create(
            **self.request_config.model_dump(exclude_none=True),
        )

        model_engine_response = AskModelEngineResponse(
            response=response.content[0].text,
            response_tokens=response.usage.output_tokens,
            prompt_tokens=response.usage.input_tokens,
            messageType="CHAT",
        )

        return model_engine_response

    def _convert_args_to_provider_config(
        self, context: str = None, history: List[Message] = None, **kwargs
    ) -> AnthropicRequestConfig:
        """
        Converts the arguments to a provider-specific configuration.
        """

        max_tokens = (
            kwargs.pop("max_tokens", None)
            or kwargs.pop("max_completion_tokens", None)
            or self.model_limits.max_completion_tokens
        )

        return AnthropicRequestConfig(
            model=self.model_name,
            system=context,
            messages=[message.model_dump(mode="json") for message in history],
            stream=self.ask_settings.streaming,
            max_tokens=max_tokens,
            temperature=kwargs.pop("temperature", None),
            top_k=kwargs.pop("top_k", None),
            top_p=kwargs.pop("top_p", None),
            container=kwargs.pop("container", None),
            stop_sequences=kwargs.pop("stop_sequences", None),
        )

    def _convert_history(
        self,
        question: str = None,
    ) -> Tuple[List[Message], str]:
        """
        Converts the history to a list of messages.
        """
        messages = []
        system_message = None
        if self.ask_settings.use_history and self.ask_settings.history:
            for message in self.ask_settings.history:
                role = message.get("role", Roles.USER)
                # Supporting this for now
                if role == "system":
                    system_message = content
                    continue

                if role != Roles.USER and role != Roles.ASSISTANT:
                    role = Roles.ASSISTANT

                content = message.get("content", None)
                tool_calls = message.get("tool_calls", [])
                content_parts = []

                message = Message(role=role, content=TextContentPart(text=content))

                messages.append(message)

        if question:
            user_message = Message(
                role=Roles.USER,
                content=[TextContentPart(text=question)],
            )

            if self.ask_settings.image_url:
                for image_url in self.ask_settings.image_url:
                    if classify_url(image_url) == "web_url":
                        image_source = ImageSource(
                            type=ImageType.URL,
                            media_type=ImageMediaType.JPEG,
                            url=image_url,
                        )

            messages.append(user_message)

        return messages, system_message
