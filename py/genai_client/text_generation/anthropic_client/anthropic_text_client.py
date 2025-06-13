from typing import List, Optional, Dict, Any, Union, Tuple
import json
from pydantic import BaseModel
from ...clients.google_clients import (
    GoogleClient,
    GoogleClientConfig,
    GoogleClientType,
)
from ...utils import (
    StringEnum,
    get_image_extension,
    fetch_and_encode_image,
)
from ...constants import AskModelEngineResponse, TEMPLATE, TEMPLATE_NAME, FULL_PROMPT
from ..abstract_text_generation_client import AbstractTextGenerationClient, AskSettings


class Roles(StringEnum):
    USER = "user"
    ASSISTANT = "assistant"


class ImageType(StringEnum):
    URL = "url"
    BASE64 = "base64"


class ImageMediaType(StringEnum):
    JPEG = "image/jpeg"
    PNG = "image/png"
    WEBP = "image/webp"
    GIF = "image/gif"


class ImageSourceURL(BaseModel):
    type: ImageType = ImageType.URL
    url: str


class ImageSourceBase64(BaseModel):
    type: ImageType
    media_type: ImageMediaType
    data: Optional[str] = None


class ImageContentPart(BaseModel):
    type: str = "image"
    source: Union[ImageSourceURL, ImageSourceBase64]


class DocumentContentPart(BaseModel):
    type: str = "document"
    media_type: str
    data: Optional[str] = None


# FOR HISTORY
class ToolUseContentPart(BaseModel):
    type: str = "tool_use"
    id: str
    name: str
    input: Dict[str, Any]


# FOR HISTORY
class ToolResultContentPart(BaseModel):
    type: str = "tool_result"
    tool_use_id: str
    content: str


# FOR CALLING TOOLS
class ToolCall(BaseModel):
    name: str
    description: Optional[str] = None
    input_schema: Optional[Dict[str, Any]] = None


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
                ToolUseContentPart,
                ToolResultContentPart,
                ThinkingContentPart,
                DocumentContentPart,
            ]
        ],
    ]


# Mimicking the structure of the usage object from the Anthropic API
class Usage(BaseModel):
    input_tokens: int
    output_tokens: int


class StreamingResponse(BaseModel):
    text: str
    usage: Usage


class AnthropicRequestConfig(BaseModel):
    model: str
    messages: List[Dict[str, Any]]
    system: Optional[str] = None
    tools: Optional[List[Dict]] = None
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

        if self.ask_settings.full_prompt:
            self.ask_settings.history = self.ask_settings.full_prompt
            converted_history, system_prompt_from_history = self._convert_history()
        else:
            converted_history, system_prompt_from_history = self._convert_history(
                question=question,
            )

        if system_prompt_from_history and context is None:
            context = system_prompt_from_history

        self.request_config = self._convert_args_to_provider_config(
            context=context,
            history=converted_history,
            **kwargs,
        )

        if self.ask_settings.streaming:
            response = self._handle_streaming(
                prefix=prefix, converted_history=converted_history
            )
            response_text = response.text
            usage = response.usage
        else:
            response = self.client.messages.create(
                **self.request_config.model_dump(exclude_none=True),
            )
            if response.stop_reason == "tool_use":
                return self._parse_tools_call_response(
                    response,
                    prompt_tokens=response.usage.input_tokens,
                    response_tokens=response.usage.output_tokens,
                )
            response_text = response.content[0].text
            usage = Usage(
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
            )

        return AskModelEngineResponse(
            response=response_text,
            response_tokens=usage.output_tokens,
            prompt_tokens=usage.input_tokens,
            messageType="CHAT",
        )

    def _parse_tools_call_response(
        self, response, prompt_tokens: int = None, response_tokens: int = None
    ) -> AskModelEngineResponse:
        tools_result = []
        for content in response.content:
            if content.type == "tool_use":
                tool_use = {
                    "id": content.id,
                    "name": content.name,
                    "arguments": content.input,
                    "type": "function",
                }
                tools_result.append(tool_use)

        return AskModelEngineResponse(
            response=tools_result,
            response_tokens=response_tokens,
            prompt_tokens=prompt_tokens,
            messageType="TOOL",
        )

    def _handle_streaming(
        self, prefix: str = "", converted_history: List[Message] = None
    ) -> StreamingResponse:

        final_response = ""

        with self.client.messages.stream(
            **self.request_config.model_dump(exclude_none=True),
        ) as stream:
            for text in stream.text_stream:
                final_response += text
                print(
                    prefix + text,
                    end="",
                )

        input_tokens = self._count_tokens(converted_history=converted_history)
        output_tokens = self._count_tokens(response_string=final_response)
        usage = Usage(input_tokens=input_tokens, output_tokens=output_tokens)

        return StreamingResponse(
            text=final_response,
            usage=usage,
        )

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

        tools = kwargs.pop("tools", None)
        if tools is not None:
            tools = self._handle_tools_conversion(tools)
            tools = [tools.model_dump(mode="json") for tools in tools]
            self.ask_settings.streaming = False

        return AnthropicRequestConfig(
            model=self.model_name,
            system=context,
            messages=[message.model_dump(mode="json") for message in history],
            tools=tools,
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
                    system_message = message.get("content", "")
                    continue

                content_parts = []

                if role == Roles.USER:
                    message_content = message.get("content", "")
                    content_parts.append(TextContentPart(text=message_content))

                # our roles can be assistant, user, tool or system
                elif role != Roles.USER:
                    tool_calls = message.get("tool_calls", None)
                    if tool_calls:
                        for tool_call in tool_calls:

                            arguments = tool_call.get("function").get("arguments", {})
                            if isinstance(arguments, str):
                                try:
                                    arguments = json.loads(arguments)
                                except json.JSONDecodeError:
                                    arguments = {}

                            content_parts.append(
                                ToolUseContentPart(
                                    id=tool_call.get("id", ""),
                                    name=tool_call.get("function").get("name", ""),
                                    input=arguments,
                                )
                            )

                    if role == "tool":
                        role = Roles.USER
                        tool_call_result_id = message.get("tool_call_id", "")
                        tool_result_content = message.get("content", "")
                        content_parts.append(
                            ToolResultContentPart(
                                tool_use_id=tool_call_result_id,
                                content=tool_result_content,
                            )
                        )

                    if role == "assistant":
                        message_content = message.get("content", "")
                        content_parts.append(TextContentPart(text=message_content))

                message = Message(role=role, content=content_parts)

                messages.append(message)

        if question:
            user_message = Message(
                role=Roles.USER,
                content=[TextContentPart(text=question)],
            )

            if self.ask_settings.image_url:
                for image_url in self.ask_settings.image_url:

                    image_content_part = self._create_image_part(
                        image_type="url",
                        data=image_url,
                    )

                    user_message.content.append(image_content_part)

            if self.ask_settings.image_encoded:
                for image_encoded in self.ask_settings.image_encoded:

                    image_content_part = self._create_image_part(
                        image_type="base64",
                        data=image_encoded,
                    )

                    user_message.content.append(image_content_part)

            messages.append(user_message)

        messages = self._filter_incomplete_tool_conversations(messages)

        return messages, system_message

    def _filter_incomplete_tool_conversations(
        self, messages: List[Message]
    ) -> List[Message]:
        """
        Remove any trailing tool_use messages that don't have corresponding tool_result messages.
        Anthropic's API doesn't allow incomplete tool conversations.
        """
        if not messages:
            return messages

        last_message = messages[-1]
        if last_message.role == Roles.ASSISTANT and any(
            part.type == "tool_use"
            for part in last_message.content
            if hasattr(part, "type")
        ):

            return messages[:-1]

        return messages

    def _create_image_part(self, image_type: str, data: str) -> ImageContentPart:
        if image_type == "url":
            if self.provider == "google":
                try:
                    base64_encoded = fetch_and_encode_image(data)
                except Exception as e:
                    raise ValueError(f"Failed to fetch and encode image: {e}")
                image_source = ImageSourceBase64(
                    type=ImageType.BASE64,
                    media_type=base64_encoded[1],
                    data=base64_encoded[0],
                )
            else:
                image_source = ImageSourceURL(
                    type=ImageType.URL,
                    url=data,
                )
        elif image_type == "base64":
            image_extension = get_image_extension(data)
            if not image_extension:
                raise ValueError("Unable to determine image extension from data.")

            try:
                media_type = ImageMediaType(f"image/{image_extension.lower()}")
            except ValueError:
                raise ValueError(
                    f"Unsupported image extension '{image_extension}' for base64 data."
                )

            image_source = ImageSourceBase64(
                type=ImageType.BASE64,
                media_type=media_type,
                data=data,
            )

        else:
            raise ValueError(f"Unsupported image type '{image_type}'.")

        return ImageContentPart(source=image_source)

    def _handle_tools_conversion(self, tools: List[Dict]) -> List[ToolCall]:
        """
        Converts tools to ToolContentPart objects.
        """
        tool_calls = []
        for tool in tools:
            if tool.get("type", None) == "function":
                func_def = tool["function"]

                parameters_schema = None
                if "parameters" in func_def:
                    params = func_def["parameters"]

                    properties = {}
                    for prop_name, prop_def in params.get("properties", {}).items():
                        properties[prop_name] = {
                            "type": prop_def.get("type", "string"),
                            "description": prop_def.get("description", ""),
                        }
                    parameters_schema = {
                        "type": "object",
                        "properties": properties,
                        "required": params.get("required", []),
                    }

                tool_call = ToolCall(
                    name=func_def.get("name", ""),
                    description=func_def.get("description", ""),
                    input_schema=parameters_schema,
                )
                tool_calls.append(tool_call)

        return tool_calls

    def _count_tokens(
        self, converted_history: List[Message] = None, response_string: str = None
    ) -> int:
        if not converted_history and not response_string:
            return 0
        if response_string:
            history = [{"role": "user", "content": response_string}]
        else:
            history = [message.model_dump(mode="json") for message in converted_history]
        response = self.client.messages.count_tokens(
            model=self.model_name, messages=history
        )
        return response.input_tokens if response else 0
