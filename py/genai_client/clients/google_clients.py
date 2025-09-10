from typing import Optional, Dict
from google import genai
from anthropic import AnthropicVertex
from anthropic import AnthropicBedrock
from pydantic import BaseModel, Field
from ..utils import StringEnum


class GoogleClientType(StringEnum):
    """Current providers we support for Google clients.
    Google covers the Google GenAI API & Gemini API.
    """

    GOOGLE = "google"
    ANTHROPIC = "anthropic"
    BEDROCK = "bedrock"


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
    aws_region: Optional[str] = None
    aws_access_key: Optional[str] = None
    aws_secret_key: Optional[str] = None


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
        if self.config.type == GoogleClientType.GOOGLE:
            return self._get_google_client()
        elif self.config.type == GoogleClientType.ANTHROPIC:
            return self._get_anthropic_client()
        elif self.config.type == GoogleClientType.BEDROCK:
            return self._get_bedrock_client()
        else:
            raise ValueError(f"Unsupported provider: {self.config.provider}. ")

    def _get_google_client(
        self,
    ) -> genai.Client:
        """Initialize the Google Gen AI client with credentials from SMSS."""
        try:
            if self.config.api_key:
                return genai.Client(api_key=self.config.api_key)
            elif (
                self.config.project
                and self.config.region
                and self.service_account_credentials
            ):
                return genai.Client(
                    credentials=self.service_account_credentials,
                    vertexai=True,
                    location=self.config.region,
                    project=self.config.project,
                )
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

    def _get_bedrock_client(self) -> AnthropicBedrock:
        """Initialize the Anthropic Bedrock client."""
        if (
            not self.config.aws_region
            or not self.config.aws_access_key
            or not self.config.aws_secret_key
        ):
            raise ValueError(
                "Region, Secret key and access key must be provided for Anthropic Bedrock client."
            )

        return AnthropicBedrock(
            aws_access_key=self.config.aws_access_key,
            aws_secret_key=self.config.aws_secret_key,
            aws_region=self.config.aws_region,
        )
