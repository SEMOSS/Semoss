import ast
from typing import List, Optional, Dict, Any, Tuple, Union
import json
import re
from pydantic import BaseModel
from ...clients.google_clients import (
    GoogleClient,
    GoogleClientConfig,
    GoogleClientType,
)
from ...utils import (
    get_image_extension,
    fetch_and_encode_image,
    is_base64_image_url,
)
from ...constants import AskModelEngineResponse, TEMPLATE, TEMPLATE_NAME
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...message_builders.anthropic.anthropic_message_builder import (
    AnthropicMessageBuilder,
)
from ...message_builders.anthropic.anthropic_models import (
    AnthropicRoles as Roles,
    AnthropicImageType as ImageType,
    AnthropicImageMediaType as ImageMediaType,
    AnthropicImageSourceURL as ImageSourceURL,
    AnthropicImageSourceBase64 as ImageSourceBase64,
    AnthropicImageContentPart as ImageContentPart,
    AnthropicToolUseContentPart as ToolUseContentPart,
    AnthropicToolResultContentPart as ToolResultContentPart,
    AnthropicTextContentPart as TextContentPart,
    AnthropicMessage as Message,
)
from anthropic import AnthropicBedrock


class ToolCall(BaseModel):
    name: str
    description: Optional[str] = None
    input_schema: Optional[Dict[str, Any]] = None


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
    betas: Optional[List[str]] = None
    system: Optional[str] = None
    tools: Optional[List[Dict]] = None
    tool_choice: Optional[Dict[str, str]] = None
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
        use_beta_header: Optional[Union[str, bool]] = False,
        **kwargs,
    ):
        super().__init__(
            template=kwargs.pop(TEMPLATE, None),
            template_name=kwargs.pop(TEMPLATE_NAME, None),
            **kwargs,
        )

        self.provider = provider.lower()
        self.use_beta_header = (
            use_beta_header.lower() in ["true", "1", "yes", "on"]
            if isinstance(use_beta_header, str)
            else use_beta_header
        )
        self.beta_feature_name = kwargs.pop("beta_feature_name", None)
        if self.use_beta_header and not self.beta_feature_name:
            raise ValueError(
                "beta_feature_name is required when use_beta_header is enabled."
            )

        self.client = self._get_client(**kwargs)
        self.using_semoss_msg_fmt = False

    def _get_client(self, **kwargs):
        # TODO: Implement support for Anthropic API directly
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
        elif self.provider == "bedrock":
            return AnthropicBedrock(
                aws_region=kwargs.pop("aws_region", None),
                aws_access_key=kwargs.pop("aws_access_key", None),
                aws_secret_key=kwargs.pop("aws_secret_key", None),
            )
        else:
            raise ValueError(
                f"Provider '{self.provider}' is not supported for Anthropic Text Client."
            )

    def ask_call(
        self,
        prefix="",
        **kwargs,
    ):
        if self.client is None:
            raise ValueError("Anthropic client is not initialized.")

        self.ask_settings = self.get_ask_settings(self.model_settings, **kwargs)

        # Handling new history format through message_json
        if self.ask_settings.semoss_messages:
            self.using_semoss_msg_fmt = True
            return self._handle_semoss_msgs(prefix=prefix)

        # Handling full prompt from Elsa...
        elif self.ask_settings.full_prompt:
            msg_history = self._handle_full_prompt_msgs(**kwargs)

        # Handling standard ask with question and legacy history
        else:
            msg_history = self._handle_standard_ask(**kwargs)

        if self.ask_settings.streaming:
            response = self._handle_streaming(prefix=prefix, msg_history=msg_history)
            response_text = response.text
            usage = response.usage
        else:
            if self.use_beta_header:
                response = self.client.beta.messages.create(
                    **self.request_config.model_dump(exclude_none=True),
                )
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

    def _handle_semoss_msgs(self, prefix):
        """Handle SEMOSS messages through AnthropicMessageBuilder"""
        try:
            msg_history, param_map = AnthropicMessageBuilder().build_messages(
                semoss_messages=self.ask_settings.semoss_messages,
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to build messages in Anthropic format from SEMOSS format: {e}"
            )

        # Create request config with tools from param_map
        self.request_config = self._convert_args_to_provider_config(
            history=msg_history,
            **param_map,
        )

        if self.ask_settings.streaming:
            response = self._handle_streaming(prefix=prefix, msg_history=msg_history)
            response_text = response.text
            usage = response.usage
        else:
            if self.use_beta_header:
                response = self.client.beta.messages.create(
                    **self.request_config.model_dump(exclude_none=True),
                )
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

            if "schema" in param_map:
                return self._parse_structured_json_response(
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

    def _handle_full_prompt_msgs(self, **kwargs):
        """
        This method will change when we go to the new history format.
        In the future we will not do this conversion
        Right now it is required for Elsa support
        But eventually full_prompt will assume the structure of the messages matches the Anthropic API
        """
        self.ask_settings.history = self.ask_settings.full_prompt
        msg_history, system_prompt_from_history = self._convert_history()
        if system_prompt_from_history and not self.ask_settings.system_prompt:
            self.ask_settings.system_prompt = system_prompt_from_history

        self.request_config = self._convert_args_to_provider_config(
            history=msg_history,
            **kwargs,
        )

        return msg_history

    def _handle_standard_ask(self, **kwargs):
        """This method will change when we go to the new history format"""
        msg_history, system_prompt_from_history = self._convert_history(
            question=kwargs.get("question"),
        )
        if system_prompt_from_history and not self.ask_settings.system_prompt:
            self.ask_settings.system_prompt = system_prompt_from_history

        self.request_config = self._convert_args_to_provider_config(
            history=msg_history,
            **kwargs,
        )

        return msg_history

    def _parse_structured_json_response(
        self, response, prompt_tokens: int = None, response_tokens: int = None
    ) -> AskModelEngineResponse:

        # replace the extra strings in structured json response
        match = re.search(r"\{.*\}", response.content[0].text, re.DOTALL)
        if match:
            response_text = match.group(0)
        else:
            response_text = response.content[0].text

        return AskModelEngineResponse(
            response=response_text,
            response_tokens=response_tokens,
            prompt_tokens=prompt_tokens,
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
        self, prefix: str = "", msg_history: List[Message] = None
    ) -> StreamingResponse:

        final_response = ""

        if self.use_beta_header:
            with self.client.beta.messages.stream(
                **self.request_config.model_dump(exclude_none=True),
            ) as stream:
                for text in stream.text_stream:
                    final_response += text
                    print(
                        prefix + text,
                        end="",
                    )
        else:
            with self.client.messages.stream(
                **self.request_config.model_dump(exclude_none=True),
            ) as stream:
                for text in stream.text_stream:
                    final_response += text
                    print(
                        prefix + text,
                        end="",
                    )

        input_tokens = self._count_tokens(msg_history=msg_history)
        output_tokens = self._count_tokens(response_string=final_response)
        usage = Usage(input_tokens=input_tokens, output_tokens=output_tokens)

        return StreamingResponse(
            text=final_response,
            usage=usage,
        )

    # Remove on message builder consolidation
    def _build_tool_choice(
        self, tool_choice: Dict[str, str]
    ) -> Union[Dict[str, str], None]:
        """
        Build the tool choice dictionary for Anthropic
        SEMOSS tool_type options [auto, required, forced, none]
        Anthropic type options [auto, any, tool, none]
        Anthropic types of any and tool are not available with extended thinking
        """
        tool_type = tool_choice.get("type", "auto").lower()
        tool_name = tool_choice.get("name", None)
        if tool_type == "auto":
            return {"type": "auto"}
        elif tool_type == "required":
            return {"type": "any"}
        elif tool_type == "forced" and tool_name:
            return {"type": "tool", "name": tool_name}
        elif tool_type == "none":
            return {"type": "none"}
        else:
            return None

    def _convert_args_to_provider_config(
        self, history: List[Message] = None, **kwargs
    ) -> AnthropicRequestConfig:
        """
        Converts the arguments to a provider-specific configuration.
        """

        system_prompt = kwargs.pop("context", None)
        if not system_prompt:
            system_prompt = self.ask_settings.system_prompt

        max_tokens = (
            kwargs.pop("max_tokens", None)
            or kwargs.pop("max_completion_tokens", None)
            or self.model_limits.max_completion_tokens
        )

        tools = kwargs.pop("tools", None)
        if tools:
            # Tools are already in Anthropic format from the message builder
            # Disable streaming when tools are present
            self.ask_settings.streaming = False

        # Remove on message builder consolidation
        if "tool_choice" in kwargs and not self.using_semoss_msg_fmt:
            kwargs["tool_choice"] = self._build_tool_choice(
                kwargs.pop("tool_choice", {})
            )

        return AnthropicRequestConfig(
            model=self.model_name,
            system=system_prompt,
            messages=[message.model_dump(mode="json") for message in history],
            betas=[self.beta_feature_name] if self.use_beta_header else None,
            tools=tools,
            tool_choice=kwargs.pop("tool_choice", None),
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
        Converts the history to a list of messages with full support for both legacy and structured content.
        """
        messages = []
        system_message = None

        if self.ask_settings.use_history and self.ask_settings.history:
            for message in self.ask_settings.history:
                role = message.get("role", Roles.USER)
                if role == "system":
                    system_message = message.get("content", "")
                    continue

                content_parts = []

                if role == Roles.USER:
                    message_content = message.get("content", "")
                    if isinstance(message_content, str):
                        content_parts.append(TextContentPart(text=message_content))
                    elif isinstance(message_content, list):
                        for part in message_content:
                            if isinstance(part, dict):
                                part_type = part.get("type", "")
                                # Text part
                                if part_type == "text" and "text" in part:
                                    content_parts.append(
                                        TextContentPart(text=part["text"])
                                    )
                                # Image part (support both "image", "image_url")
                                elif part_type in ["image", "image_url"]:
                                    img = part.get("image_url", None) or part.get(
                                        "url", None
                                    )
                                    if isinstance(img, dict):
                                        image_url = img.get("url", "")
                                    elif isinstance(img, str):
                                        image_url = img
                                    else:
                                        raise ValueError(
                                            f"Unrecognized image part: {part}"
                                        )

                                    if is_base64_image_url(image_url):
                                        content_parts.append(
                                            self._create_image_part(
                                                image_type="base64", data=image_url
                                            )
                                        )
                                    else:
                                        content_parts.append(
                                            self._create_image_part(
                                                image_type="url", data=image_url
                                            )
                                        )
                                else:
                                    pass
                            else:
                                raise ValueError(
                                    f"Content part must be dict: got {type(part)}"
                                )
                    else:
                        raise ValueError(
                            f"Message content of unsupported type: {type(message_content)}"
                        )

                    # Backward compatibility: Also check for image_url on message level
                    if message.get("image_url", None):
                        image_messages = message.get("image_url", [])
                        if isinstance(image_messages, dict):
                            image_messages = [image_messages]
                        for image in image_messages:
                            if isinstance(image, str):
                                try:
                                    image_dict = json.loads(image)
                                except json.JSONDecodeError:
                                    image_dict = ast.literal_eval(image)
                            else:
                                image_dict = image
                            image_url = image_dict.get("url", "")
                            if is_base64_image_url(image_url):
                                image_content_part = self._create_image_part(
                                    "base64", image_url
                                )
                            else:
                                image_content_part = self._create_image_part(
                                    "url", image_url
                                )
                            content_parts.append(image_content_part)

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

                msg_obj = Message(role=role, content=content_parts)
                messages.append(msg_obj)

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
            media_type = None
            if data.startswith("data:"):
                if ";" in data:
                    media_type_str = data.split(";")[0].replace("data:", "")
                    try:
                        media_type = ImageMediaType(media_type_str)
                    except ValueError:
                        pass
                data = data.split(",", 1)[1] if "," in data else data

            if media_type is None:
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
        self, msg_history: List[Message] = None, response_string: str = None
    ) -> int:
        if not msg_history and not response_string:
            return 0
        if response_string:
            history = [{"role": "user", "content": response_string}]
        else:
            history = [message.model_dump(mode="json") for message in msg_history]
        response = self.client.messages.count_tokens(
            model=self.model_name, messages=history
        )
        return response.input_tokens if response else 0
