from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...clients.client_initializer import google_initializer
from ...constants import AskModelEngineResponse, TEMPLATE, TEMPLATE_NAME, FULL_PROMPT
from google import genai
from google.genai import types


class AbstractGoogleGenAiClient(AbstractTextGenerationClient):
    """
    Abstract class for Google Gen AI inference.
    """

    def __init__(
        self,
        model_name: str,
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

        self.model_name = model_name
        self.client = self._get_client(
            project, region, vertextai=False, api_key=api_key
        )
        self.max_tokens = max_tokens
        self.safety_settings = safety_settings or {}

    def _get_client(
        self,
        project: str = None,
        location: str = None,
        vertextai: bool = False,
        api_key: str = None,
    ):
        """Initialize the Google Gen AI client with credentials from SMSS."""
        try:
            if api_key:
                return genai.Client(api_key=api_key)
            elif project is not None and location is not None:
                return genai.Client(
                    project=project, location=location, vertexai=vertextai
                )
            else:
                raise ValueError(
                    "Either api_key or both project and location must be provided."
                )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Google Gen AI client: {e}")
