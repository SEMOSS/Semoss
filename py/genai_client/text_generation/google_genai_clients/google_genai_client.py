from typing import List, Optional, Dict
from pydantic import BaseModel
from google.genai import types
from ...clients.google_genai_client import GoogleGenAIClient, GoogleGenAIClientConfig
from ...utils import StringEnum, classify_url
from ...constants import AskModelEngineResponse, TEMPLATE, TEMPLATE_NAME, FULL_PROMPT
from ..abstract_text_generation_client import AbstractTextGenerationClient


class AskSettings(BaseModel):
    """
    Represents all of the conditional settings that affect the model call but are not passed
    as parameters to the model call itself.
    """

    full_prompt: Dict | None = None
    streaming: bool = False
    use_history: bool = True
    history: Optional[List[Dict]] = None
    image_url: Optional[List[str]] = None


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
        self.client_config = GoogleGenAIClientConfig(
            service_account_credentials=service_account_credentials,
            service_account_key_file=service_account_key_file,
            region=region,
            project=project,
            api_key=api_key,
        )
        self.client = GoogleGenAIClient(config=self.client_config).client

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

        ask_settings = self._get_ask_settings(history, use_history, **kwargs)

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

        model_engine_response = AskModelEngineResponse(
            response=response.text,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            messageType="CHAT",
        )

        return model_engine_response

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
        )
        return config

    def _get_ask_settings(
        self, history=None, use_history: bool = True, **kwargs
    ) -> AskSettings:
        """
        Get the ask settings from the provided keyword arguments.
        """
        full_prompt = kwargs.pop(FULL_PROMPT, None)
        streaming = kwargs.pop("streaming", False)
        image_url = kwargs.pop("image_url", None)
        # So we can send multiple images
        if isinstance(image_url, str):
            image_url = [image_url]
        if not streaming:
            streaming = kwargs.pop("stream", False)

        if not use_history:
            history = None

        return AskSettings(
            full_prompt=full_prompt,
            streaming=streaming,
            history=history,
            image_url=image_url,
        )

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
        self, history: List[Dict] = None, question: str = None, image_url: str = None
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
                if role == "system":
                    system_instructions = content
                    continue
                if role != "user":
                    role = Roles.MODEL
                message = types.Content(
                    role=role, parts=[types.Part.from_text(text=content)]
                )
                google_history.append(message)

        # If there is a question (not full prompt), add it as the last message
        if question:
            final_message_parts = []
            if image_url:
                image_parts = self._create_image_part(image_url)
                final_message_parts.extend(image_parts)
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
