from typing import List, Optional, Dict
from pydantic import BaseModel
from google.genai import types
from ...clients.google_clients import (
    GoogleClient,
    GoogleClientConfig,
    GoogleClientType,
)
from ...utils import StringEnum, classify_url
from ...constants import AskModelEngineResponse, TEMPLATE, TEMPLATE_NAME
from ..abstract_text_generation_client import AbstractTextGenerationClient


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


class Roles(StringEnum):
    USER = "user"
    MODEL = "model"


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
        question: str = None,
        context: str = None,
        use_history: bool = True,
        history: List[Dict] = None,
        prefix="",
        **kwargs,
    ):
        if self.client is None:
            raise ValueError("Google Gen AI client is not initialized.")

        ask_settings = self.get_ask_settings(history, use_history, **kwargs)

        if ask_settings.full_prompt is not None:
            converted_history = self._convert_history(
                history=ask_settings.full_prompt,
            )
        else:
            converted_history = self._convert_history(
                history=ask_settings.history,
                question=question,
                image_url=ask_settings.image_url,
            )

        contents = converted_history.contents
        if converted_history.system_instructions is not None and context is not None:
            print(
                "There are multiple sets of system instructions.. Using context passed to ask_call()"
            )
        elif converted_history.system_instructions is not None:
            context = converted_history.system_instructions

        config = self._convert_args_to_provider_config(context=context, **kwargs)

        if ask_settings.streaming:
            # STREAMING
            response = self._handle_streaming(
                prefix=prefix,
                contents=contents,
                config=config,
            )
        else:
            # NON-STREAMING
            response = self.client.models.generate_content(
                model=self.model_name, contents=contents, config=config
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

    def _parse_tools_call_response(
        self,
        response: types.GenerateContentResponse,
        response_tokens: int,
        prompt_tokens: int,
    ) -> AskModelEngineResponse:
        tools_result = []
        for i, function_call in enumerate(response.function_calls):
            tools_result.append(
                {
                    # idk why google is not giving me an id here..
                    # They have a palce holder field for it, but it's always None
                    # I also don't need to pass it to the model in the history..
                    "id": i,
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
        prefix: str,
        contents: List[types.Content],
        config: types.GenerateContentConfig,
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

    def _handle_tools_conversion(self, tools: List[Dict]) -> List[types.Tool]:
        """
        Converting from the OpenAI tools format I recieve to the Google Gen AI tools format.
        """
        google_tools = []

        for tool in tools:
            if tool["type"] == "function":
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

    def _convert_args_to_provider_config(
        self, context: str = None, **kwargs
    ) -> types.GenerateContentConfig:
        """
        Convert our CFG arguments to a GenerateContentConfig object.
        """
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
        history: List[Dict] = None,
        question: str = None,
        image_url: List[str] = None,
        image_encoded: List[str] = None,
    ) -> ConvertedHistory:
        """
        Convert our history format to Google Gen AI's Content format.
        """
        google_history = []
        system_instructions = None

        if history is not None:
            for message in history:
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
