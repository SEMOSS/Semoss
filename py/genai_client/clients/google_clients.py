from typing import Optional, Dict
from google import genai
from pydantic import BaseModel, Field
from ..utils import StringEnum


class GoogleClientProviders(StringEnum):
    """Current providers we support for Google clients.
    Google covers the Google GenAI API & Gemini API.
    """

    GOOGLE = "google"
    ANTHROPIC = "anthropic"


class GoogleClientConfig(BaseModel):
    """
    For Google GenAI Vertex & Anthropic Vertex clients the service_account_credentials, region and project are required.
    For Gemini clients, the api_key is required.
    """

    provider: GoogleClientProviders = Field(
        description="The provider for the client (e.g., 'google', 'anthropic')"
    )
    service_account_credentials: Optional[Dict] = None
    service_account_key_file: Optional[str] = None
    region: Optional[str] = None
    project: Optional[str] = None
    api_key: Optional[str] = None


class GoogleClient:
    """
    A client for interacting with Google GenAI services.
    Use this class to load the client for text generation, embeddings, and other GenAI functionalities.
    """

    def __init__(self, config: GoogleClientConfig):
        self.config = config

        if config.service_account_credentials or config.service_account_key_file:
            self.service_account_credentials = self._load_credentials(
                config.service_account_credentials, config.service_account_key_file
            )

        self.client = self._get_client()

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

    def _get_client(self):
        if self.config.provider == GoogleClientProviders.GOOGLE:
            return self._get_google_client(
                project=self.config.project,
                location=self.config.region,
                api_key=self.config.api_key,
            )
        else:
            raise ValueError(f"Unsupported provider: {self.config.provider}. ")

    def _get_google_client(
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
                    credentials=self.service_account_credentials,
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
