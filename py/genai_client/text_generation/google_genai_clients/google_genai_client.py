from typing import List, Optional, Dict
from google import genai
from pydantic import BaseModel
from google.genai import types
from ...constants import AskModelEngineResponse, TEMPLATE, TEMPLATE_NAME, FULL_PROMPT
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...clients.client_initializer import google_initializer


class AskSettings(BaseModel):
    """
    Represents all of the conditional settings that affect the model call but are not passed
    as parameters to the model call itself.
    """

    full_prompt: Dict | None = None
    structured_response: Dict | None = None
    streaming: bool = False
    use_history: bool = True


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

        # google_initializer(
        #     region=region,
        #     service_account_credentials=service_account_credentials,
        #     service_account_key_file=service_account_key_file,
        #     project=project,
        # )

        self.service_account_credentials = self._load_credentials(
            service_account_credentials, service_account_key_file
        )

        self.model_name = model_name
        self.client = self._get_client(project, region, api_key=api_key)
        self.max_tokens = max_tokens
        self.safety_settings = safety_settings

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
        if not streaming:
            streaming = kwargs.pop("stream", False)

        if not use_history:
            history = None

        return AskSettings(
            full_prompt=full_prompt,
            structured_response=structured_response,
            streaming=streaming,
            history=history,
        )

    def _handle_streaming(
        self,
        prefix: str,
        prompt: str | Dict,
        config: types.GenerateContentConfig,
        history=None,
    ) -> str:
        """
        Handle the streaming response from the Google Gen AI client.
        """
        final_response = ""
        chat_session = self.client.chats.create(
            model=self.model_name, config=config, history=history
        )
        for chunk in chat_session.send_message_stream(prompt):
            final_response += chunk.text
            print(prefix + chunk.text, end="")
        return final_response

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
        config = self._convert_args_to_provider_config(context=context, **kwargs)

        if ask_settings.streaming:
            streaming_response = self._handle_streaming(
                prefix=prefix,
                prompt=question,
                config=config,
                history=ask_settings.history,
            )

        response = self.client.models.generate_content(
            model=self.model_name, contents=question, config=config
        )

        model_engine_response = AskModelEngineResponse(
            response=response.text,
        )

        print(response)

        return model_engine_response


types.GenerateContentResponse()

# class GenerateContentResponse(
#     candidates: list[Candidate] | None = None,
#     create_time: datetime | None = None,
#     response_id: str | None = None,
#     model_version: str | None = None,
#     prompt_feedback: GenerateContentResponsePromptFeedback | None = None,
#     usage_metadata: GenerateContentResponseUsageMetadata | None = None,
#     automatic_function_calling_history: list[Content] | None = None,
#     parsed: BaseModel | dict[Any, Any] | Enum | None = None
# )

# class GenerateContentConfig(
#     http_options: HttpOptions | None = None,
#     system_instruction: ContentUnion | None = None,
#     temperature: float | None = None,
#     top_p: float | None = None,
#     top_k: float | None = None,
#     candidate_count: int | None = None,
#     max_output_tokens: int | None = None,
#     stop_sequences: list[str] | None = None,
#     response_logprobs: bool | None = None,
#     logprobs: int | None = None,
#     presence_penalty: float | None = None,
#     frequency_penalty: float | None = None,
#     seed: int | None = None,
#     response_mime_type: str | None = None,
#     response_schema: SchemaUnion | None = None,
#     routing_config: GenerationConfigRoutingConfig | None = None,
#     model_selection_config: ModelSelectionConfig | None = None,
#     safety_settings: list[SafetySetting] | None = None,
#     tools: ToolListUnion | None = None,
#     tool_config: ToolConfig | None = None,
#     labels: dict[str, str] | None = None,
#     cached_content: str | None = None,
#     response_modalities: list[str] | None = None,
#     media_resolution: MediaResolution | None = None,
#     speech_config: SpeechConfigUnion | None = None,
#     audio_timestamp: bool | None = None,
#     automatic_function_calling: AutomaticFunctionCallingConfig | None = None,
#     thinking_config: ThinkingConfig | None = None
# )
