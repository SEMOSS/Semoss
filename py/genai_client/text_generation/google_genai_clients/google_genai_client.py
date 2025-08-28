from typing import List, Optional, Dict
from pydantic import BaseModel
from google.genai import types
from ...clients.google_clients import (
    GoogleClient,
    GoogleClientConfig,
    GoogleClientType,
)
from ...utils import classify_url
from ...constants import AskModelEngineResponse, TEMPLATE, TEMPLATE_NAME
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...message_builders.google_genai.google_genai_models import GoogleRoles as Roles
from ...message_builders.google_genai.google_genai_builder import (
    GoogleGenAIMessageBuilder,
)


# Mimicking Google Gen AI's usage metadata structure
class UsageMetadata(BaseModel):
    candidates_token_count: int
    prompt_token_count: int


# Using this as a response model for streaming responses since Google Gen AI does not return usage metadata in streaming responses
class StreamingResponse(BaseModel):
    text: str
    usage_metadata: Optional[UsageMetadata] = None

    class Config:
        arbitrary_types_allowed = True


class ConvertedHistory(BaseModel):
    """
    Convert our history format to Google Gen AI's Content format.
    If I find system instructions, I will return these as well.
    """

    contents: List[types.Content]
    system_instructions: str | None = None


class GoogleGenAiTextClient(AbstractTextGenerationClient):
    def __init__(
        self,
        service_account_credentials: Dict = None,
        service_account_key_file: str = None,
        region: str = None,
        project: str = None,
        api_key: str = None,
        safety_settings: dict = None,
        **kwargs,
    ):
        super().__init__(
            template=kwargs.pop(TEMPLATE, None),
            template_name=kwargs.pop(TEMPLATE_NAME, None),
            **kwargs,
        )
        self.client_config = GoogleClientConfig(
            type=GoogleClientType.GOOGLE,
            service_account_credentials=service_account_credentials,
            service_account_key_file=service_account_key_file,
            region=region,
            project=project,
            api_key=api_key,
        )
        self.client = GoogleClient(config=self.client_config).client

        self.safety_settings = safety_settings

    def ask_call(
        self,
        prefix="",
        **kwargs,
    ):
        if self.client is None:
            raise ValueError("Google Gen AI client is not initialized.")

        self.ask_settings = self.get_ask_settings(self.model_settings, **kwargs)

        # Handling new history format through message_json
        if self.ask_settings.semoss_messages:
            return self._handle_semoss_messages(
                semoss_messages=self.ask_settings.semoss_messages, prefix=prefix
            )

        # Handling full prompt
        elif self.ask_settings.full_prompt:
            msg_history = self._handle_full_prompt_msgs(**kwargs)

        # Handling standard ask with question and legacy history
        else:
            msg_history = self._handle_standard_ask(
                **kwargs,
            )

        config = self._convert_args_to_provider_config(**kwargs)

        if self.ask_settings.streaming:
            # STREAMING
            response = self._handle_streaming(
                prefix=prefix,
                contents=msg_history,
                config=config,
            )
        else:
            # NON-STREAMING
            response = self.client.models.generate_content(
                model=self.model_name, contents=msg_history, config=config
            )

        response_tokens = response.usage_metadata.candidates_token_count
        prompt_tokens = response.usage_metadata.prompt_token_count

        # Returning a diff type of AskModelEngineResponse if there are function calls
        if len(getattr(response, "function_calls", None) or []) > 0:
            return self._parse_tools_call_response(
                response=response,
                response_tokens=response_tokens,
                prompt_tokens=prompt_tokens,
            )

        model_engine_response = AskModelEngineResponse(
            response=response.text,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            messageType="CHAT",
        )

        return model_engine_response

    def _handle_semoss_messages(self, semoss_messages: List[Dict], prefix):
        try:
            response = GoogleGenAIMessageBuilder().build_messages(semoss_messages)
            google_messages = response["messages"]
            provider_config = response["param_map"]
            stream = response["stream"]
        except Exception as e:
            raise RuntimeError(f"Failed to build messages from SEMOSS messages: {e}")

        if stream or self.ask_settings.streaming:
            model_response = self._handle_streaming(
                prefix=prefix,
                contents=google_messages,
                config=provider_config,
            )
        else:
            model_response = self.client.models.generate_content(
                model=self.model_name,
                contents=google_messages,
                config=provider_config,
            )

        response_tokens = model_response.usage_metadata.candidates_token_count
        prompt_tokens = model_response.usage_metadata.prompt_token_count

        # Returning a diff type of AskModelEngineResponse if there are function calls
        if len(getattr(model_response, "function_calls", None) or []) > 0:
            return self._parse_tools_call_response(
                response=model_response,
                response_tokens=response_tokens,
                prompt_tokens=prompt_tokens,
            )

        model_engine_response = AskModelEngineResponse(
            response=model_response.text,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            messageType="CHAT",
        )

        return model_engine_response

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
            context=self.ask_settings.system_prompt,
            history=msg_history,
            **kwargs,
        )

        return msg_history

    def _handle_standard_ask(self, **kwargs):
        """This method will change when we go to the new history format"""
        cnvtd_history = self._convert_history(
            question=kwargs.get("question"),
        )
        msg_history = cnvtd_history.contents
        system_prompt_from_history = cnvtd_history.system_instructions

        if system_prompt_from_history and not self.ask_settings.system_prompt:
            self.ask_settings.system_prompt = system_prompt_from_history

        self.request_config = self._convert_args_to_provider_config(
            context=self.ask_settings.system_prompt,
            history=msg_history,
            **kwargs,
        )

        return msg_history

    def _parse_tools_call_response(
        self,
        response: types.GenerateContentResponse,
        response_tokens: int,
        prompt_tokens: int,
    ) -> AskModelEngineResponse:
        tools_result = []
        for i, function_call in enumerate(response.function_calls):
            function_id = str(i)

            tools_result.append(
                {
                    "id": function_id,
                    "type": "function",
                    "name": function_call.name,
                    "arguments": getattr(function_call, "args", {}),
                }
            )
        return AskModelEngineResponse(
            response=tools_result,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            messageType="TOOL",
        )

    def _handle_streaming(
        self,
        contents: List[types.Content],
        config: types.GenerateContentConfig,
        prefix: Optional[str] = "",
    ) -> StreamingResponse:
        final_response = ""

        for chunk in self.client.models.generate_content_stream(
            model=self.model_name, contents=contents, config=config
        ):
            final_response += chunk.text
            print(prefix + chunk.text, end="")

        input_tokens = self._count_tokens(contents)

        response_content = [
            types.Content(
                role="model", parts=[types.Part.from_text(text=final_response)]
            )
        ]
        output_tokens = self._count_tokens(response_content)

        usage_metadata = UsageMetadata(
            candidates_token_count=output_tokens,
            prompt_token_count=input_tokens,
        )

        return StreamingResponse(text=final_response, usage_metadata=usage_metadata)

    def _convert_args_to_provider_config(self, **kwargs) -> types.GenerateContentConfig:
        """
        Convert our CFG arguments to a GenerateContentConfig object.
        """
        context = kwargs.pop("context", None)
        response_schema = kwargs.pop("schema", None)
        response_mime_type = kwargs.pop("response_mime_type", None)
        if response_schema is not None and response_mime_type is None:
            response_mime_type = "application/json"

        tools = kwargs.pop("tools", None)
        if tools is not None:
            tools = self._handle_tools_conversion(tools)

        config = types.GenerateContentConfig(
            http_options=kwargs.pop("http_options", None),
            system_instruction=context,
            max_output_tokens=kwargs.get(
                "max_new_tokens", self.model_limits.max_completion_tokens
            ),
            temperature=kwargs.pop("temperature", None),
            top_p=kwargs.pop("top_p", None),
            top_k=kwargs.pop("top_k", None),
            stop_sequences=kwargs.pop("stop_sequences", None),
            presence_penalty=kwargs.pop("presence_penalty", None),
            frequency_penalty=kwargs.pop("frequency_penalty", None),
            safety_settings=self.safety_settings,
            response_schema=response_schema,
            response_mime_type=response_mime_type,
            tools=tools,
        )
        return config

    def _count_tokens(self, contents: List[types.Content]) -> int:
        try:
            response = self.client.models.count_tokens(
                model=self.model_name,
                contents=contents,
            )
            return response.total_tokens
        except Exception as e:
            raise RuntimeError(f"Failed to count tokens: {e}")

    def _convert_history(
        self,
        question: str = None,
        image_url: List[str] = None,
        image_encoded: List[str] = None,
    ) -> ConvertedHistory:
        """
        Convert our history format to Google Gen AI's Content format.
        This is only used if I do not have message_json.
        This assumes OpenAI format.
        """
        google_history = []
        system_instructions = None

        if self.ask_settings.history is not None:
            for message in self.ask_settings.history:
                role = message.get("role", "user")
                content = message.get("content", "")
                tool_calls = message.get("tool_calls", None)
                if role == "system":
                    system_instructions = content
                    continue
                if role != "user":
                    role = Roles.MODEL

                parts = [types.Part.from_text(text=content)]

                if tool_calls:
                    tool_call_parts = []
                    for tool in tool_calls:
                        if tool.get("type") == "function":
                            tool_name = tool["function"].get("name")
                            tool_args = tool["function"].get("arguments", {})
                            tool_call_parts.append(
                                types.Part.from_function_call(
                                    name=tool_name, args=tool_args
                                )
                            )
                    parts.extend(tool_call_parts)

                message = types.Content(role=role, parts=parts)
                google_history.append(message)

        # If there is a question (not full prompt), add it as the last message
        if question:
            final_message_parts = []
            if image_url:
                image_url_parts = self._create_image_part(image_url)
                final_message_parts.extend(image_url_parts)
            if image_encoded:
                image_encoded_parts = self._create_image_part(image_encoded)
                final_message_parts.extend(image_encoded_parts)

            final_message_parts.append(types.Part.from_text(text=question))
            final_message = types.Content(role=Roles.USER, parts=final_message_parts)
            google_history.append(final_message)

        return ConvertedHistory(
            contents=google_history, system_instructions=system_instructions
        )

    def _create_image_part(self, image_url: List[str]) -> List[types.Part]:
        """
        Create image parts from a list of image URLs depending on their type.
        """
        image_parts = []
        for image in image_url:
            url_type = classify_url(image)
            if url_type == "web_url":
                image_parts.append(types.Part.from_uri(file_uri=image))
            elif url_type == "base64_image":
                image_parts.append(types.Part.from_bytes(data=image))
            else:
                raise ValueError("Invalid image URL format.")

        return image_parts

    def _handle_tools_conversion(self, tools: List[Dict]) -> List[types.Tool]:
        """
        Converting from the OpenAI tools format I recieve to the Google Gen AI tools format.
        This is only used when I don't get the messages as message_json.
        Therefore I need to assume they are in OpenAI format
        """
        google_tools = []

        for tool in tools:
            if tool.get("type", None) == "function":
                func_def = tool["function"]

                parameters_schema = None
                if "parameters" in func_def:
                    params = func_def["parameters"]

                    properties = {}
                    for prop_name, prop_def in params.get("properties", {}).items():
                        properties[prop_name] = types.Schema(
                            type=prop_def["type"].upper(),
                            description=prop_def.get("description", ""),
                        )

                    parameters_schema = types.Schema(
                        type="OBJECT",
                        properties=properties,
                        required=params.get("required", []),
                    )

                function_declaration = types.FunctionDeclaration(
                    name=func_def["name"],
                    description=func_def["description"],
                    parameters=parameters_schema,
                )

                google_tools.append(
                    types.Tool(function_declarations=[function_declaration])
                )

        return google_tools
