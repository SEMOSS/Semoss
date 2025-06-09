from typing import List, Optional, Dict
from google import genai
from pydantic import BaseModel
from google.genai import types
from ...utils import StringEnum, classify_url
from ...constants import AskModelEngineResponse, TEMPLATE, TEMPLATE_NAME, FULL_PROMPT
from ..abstract_text_generation_client import AbstractTextGenerationClient


class AskSettings(BaseModel):
    """
    Represents all of the conditional settings that affect the model call but are not passed
    as parameters to the model call itself.
    """

    full_prompt: Dict | None = None
    structured_response: Dict | None = None
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


class GoogleGenAiClient(AbstractTextGenerationClient):
    def __init__(
        self,
        model_name: str,
        service_account_credentials: Dict = None,
        service_account_key_file: str = None,
        region: str = None,
        project: str = None,
        api_key: str = None,
        max_tokens: int = None,
        safety_settings: dict = None,
        **kwargs,
    ):
        super().__init__(
            template=kwargs.pop(TEMPLATE, None),
            template_name=kwargs.pop(TEMPLATE_NAME, None),
        )

        self.service_account_credentials = self._load_credentials(
            service_account_credentials, service_account_key_file
        )

        self.model_name = model_name
        self.client = self._get_client(project, region, api_key=api_key)
        self.max_tokens = max_tokens
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

    def _load_credentials(self, service_account_credentials, service_account_key_file):
        """Load service account credentials with required scopes"""
        from google.oauth2 import service_account

        service_account_info = service_account_credentials or service_account_key_file
        if not isinstance(service_account_info, dict):
            import json

            with open(service_account_info) as credentials_file:
                service_account_info = json.load(credentials_file)

        credentials = service_account.Credentials.from_service_account_info(
            service_account_info
        )

        scoped_credentials = credentials.with_scopes(
            [
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/generative-language",
            ]
        )

        return scoped_credentials

    def _get_client(
        self,
        project: str = None,
        location: str = None,
        api_key: str = None,
    ) -> genai.Client:
        """Initialize the Google Gen AI client with credentials from SMSS."""
        try:
            if api_key:
                return genai.Client(api_key=api_key)
            elif project is not None and location is not None:
                return genai.Client(
                    credentials=self.service_account_credentials,  # Use the processed credentials object
                    vertexai=True,
                    location=location,
                    project=project,
                )
            else:
                raise ValueError(
                    "Either api_key or both project and location must be provided."
                )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Google Gen AI client: {e}")

    def _convert_args_to_provider_config(
        self, context: str = None, **kwargs
    ) -> types.GenerateContentConfig:
        """
        Convert our CFG arguments to a GenerateContentConfig object.
        """
        config = types.GenerateContentConfig(
            http_options=kwargs.pop("http_options", None),
            system_instruction=context,
            max_output_tokens=kwargs.get("max_new_tokens", self.max_tokens),
            temperature=kwargs.pop("temperature", None),
            top_p=kwargs.pop("top_p", None),
            top_k=kwargs.pop("top_k", None),
            stop_sequences=kwargs.pop("stop_sequences", None),
            presence_penalty=kwargs.pop("presence_penalty", None),
            frequency_penalty=kwargs.pop("frequency_penalty", None),
            safety_settings=self.safety_settings,
            response_schema=kwargs.pop("schema", None),
        )
        return config

    def _get_ask_settings(
        self, history=None, use_history: bool = True, **kwargs
    ) -> AskSettings:
        """
        Get the ask settings from the provided keyword arguments.
        """
        full_prompt = kwargs.pop(FULL_PROMPT, None)
        structured_response = kwargs.pop("schema", None)
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
            structured_response=structured_response,
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
