from typing import Optional, Dict
from google import genai
from pydantic import BaseModel


class GoogleGenAIClientConfig(BaseModel):
    """
    Provide the first 4 parameters to initialize the Google GenAI client when using Vertex API.
    Provide only API key to initialize the client when using the Gemini API.
    """

    service_account_credentials: Optional[Dict] = None
    service_account_key_file: Optional[str] = None
    region: Optional[str] = None
    project: Optional[str] = None
    api_key: Optional[str] = None


class GoogleGenAIClient:
    """
    A client for interacting with Google GenAI services.
    Use this class to load the client for text generation, embeddings, and other GenAI functionalities.
    """

    def __init__(self, config: GoogleGenAIClientConfig):
        self.config = config

        self.service_account_credentials = self._load_credentials(
            config.service_account_credentials, config.service_account_key_file
        )

        self.client = self._get_client(
            config.project, config.region, api_key=config.api_key
        )

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
