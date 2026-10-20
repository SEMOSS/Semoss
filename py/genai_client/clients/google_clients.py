from typing import Optional, Dict, Union, cast
from google.genai import Client as GoogleGenAIClient
from anthropic import AnthropicVertex
from pydantic import BaseModel, Field
from ..utils import StringEnum


class GoogleClientType(StringEnum):
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

    type: GoogleClientType = Field(
        description="The type of client (e.g., 'google', 'anthropic')"
    )
    service_account_credentials: Optional[Dict] = None
    service_account_key_file: Optional[str] = None
    region: Optional[str] = None
    project: Optional[str] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    # Some engines set region="global" for regular inference, but Anthropic/
    # partner batch jobs reject "global" and tuned Gemini models may need a
    # specific region -- this lets the .smss configure a distinct region for
    # batch calls only. Base Gemini batch jobs work fine on "global".
    batch_region: Optional[str] = None


class GoogleClient:
    """
    A client for interacting with Google GenAI services.
    Use this class to load the client for text generation, embeddings, and other GenAI functionalities.
    """

    client: Union[GoogleGenAIClient, AnthropicVertex]

    def __init__(self, config: GoogleClientConfig):
        self.config = config

        if config.service_account_credentials or config.service_account_key_file:
            self.service_account_credentials = self._load_credentials(
                config.service_account_credentials, config.service_account_key_file
            )

        self.client = self._get_client()

    @property
    def anthropic_client(self) -> AnthropicVertex:
        """The underlying client narrowed to AnthropicVertex.

        Use this when the config type is ANTHROPIC so callers get a precisely
        typed client (with .messages/.beta) instead of the
        GoogleGenAIClient | AnthropicVertex union stored on self.client.
        """
        return cast(AnthropicVertex, self.client)

    @property
    def genai_client(self) -> GoogleGenAIClient:
        """The underlying client narrowed to GoogleGenAIClient.

        Use this when the config type is GOOGLE so callers get a precisely
        typed client instead of the union stored on self.client.
        """
        return cast(GoogleGenAIClient, self.client)

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

    def _get_client(self) -> Union[GoogleGenAIClient, AnthropicVertex]:
        if self.config.type == GoogleClientType.GOOGLE:
            return self._get_google_client()
        elif self.config.type == GoogleClientType.ANTHROPIC:
            return self._get_anthropic_client()
        else:
            raise ValueError(f"Unsupported provider type : {self.config.type}. ")

    def _get_google_client(
        self,
    ) -> GoogleGenAIClient:
        """Initialize the Google Gen AI client with credentials from SMSS."""
        try:
            if self.config.api_key:
                return GoogleGenAIClient(api_key=self.config.api_key)
            elif (
                self.config.project
                and self.config.region
                and self.service_account_credentials
            ):
                kwargs = {
                    "credentials": self.service_account_credentials,
                    "vertexai": True,
                    "location": self.config.region,
                    "project": self.config.project,
                }
                if self.config.base_url:
                    from google.genai.types import HttpOptions

                    kwargs["http_options"] = HttpOptions(
                        base_url=self.config.base_url
                    )
                return GoogleGenAIClient(**kwargs)
            else:
                raise ValueError(
                    "Either api_key or each of project, location and service account credentials must be provided."
                )
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Google Gen AI client: {e}")

    def _get_anthropic_client(self) -> AnthropicVertex:
        """Initialize the Anthropic Vertex client."""
        if (
            not self.config.project
            or not self.config.region
            or not self.service_account_credentials
        ):
            raise ValueError(
                "Project, region and service account credentials must be provided for Anthropic client."
            )

        return AnthropicVertex(
            credentials=self.service_account_credentials,
            project_id=self.config.project,
            region=self.config.region,
        )
