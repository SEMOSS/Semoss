import os
from typing import Dict
from google import genai
from google.genai import types
from google.oauth2 import credentials
from datetime import datetime, timedelta
import requests
import time
from dotenv import load_dotenv

from .google_genai_client import GoogleGenAiTextClient
from ...clients.google_clients import GoogleClient, GoogleClientConfig, GoogleClientType

load_dotenv()


class IBMApiConnectGoogleClient(GoogleClient):
    """
    Custom Google client that uses the IBM API Connect gateway instead of the direct Google API.
    """

    def __init__(
        self,
        config: GoogleClientConfig,
        api_gateway_base_url: str,
        api_gateway_token: str,
    ):
        self.config = config
        self.api_gateway_base_url = api_gateway_base_url
        self.api_gateway_token = api_gateway_token

        # Don't call parent __init__ since we need custom client initialization
        self.client = self._get_client()

    def _get_client(self):
        """Override to use IBM API Connect gateway instead of direct Google API."""
        if self.config.type == GoogleClientType.GOOGLE:
            return self._get_ibm_apiconnect_google_client()
        else:
            raise ValueError(
                f"IBM API Connect solo soporta el tipo de cliente Google, se recibió: {self.config.type}"
            )

    def _get_ibm_apiconnect_google_client(self) -> genai.Client:
        """Initialize Google Gen AI client through IBM API Connect gateway."""
        try:
            # Create credentials with API gateway token
            future_expiry = datetime.now() + timedelta(hours=1)
            creds = credentials.Credentials(
                token=self.api_gateway_token, expiry=future_expiry
            )

            # Configure HTTP options to point to API gateway
            http_options = types.HttpOptions(
                base_url=self.api_gateway_base_url,
            )

            # Create client with IBM API Connect configuration
            return genai.Client(
                credentials=creds,
                vertexai=True,
                location=self.config.region,
                project=self.config.project,
                http_options=http_options,
            )
        except Exception as e:
            raise RuntimeError(
                f"Error initializing IBM API Connect Google Gen AI client: {e}"
            )


class IBMApiConnectGoogleClientController(GoogleGenAiTextClient):
    """
    Google GenAI client controller that uses the IBM API Connect gateway.
    Inherits all functionality from GoogleGenAiTextClient but uses custom client initialization.
    """

    def __init__(self, **kwargs):
        # Extract IBM API Connect specific parameters
        self.api_gateway_base_url = kwargs.pop("api_gateway_base_url", None)
        self.apiconnect_url = os.getenv("ARCHITECTURE_ACCESS_TOKEN_URL")
        self.client_id = os.getenv("APICONNECT_CLIENT")
        self.client_secret = os.getenv("APICONNECT_SECRET")
        self.scope = os.getenv("APICONNECT_SCOPE")

        # Store initialization parameters for token renewal
        self.smss_init_model_engine_params = kwargs.copy()

        # Initialize with valid token
        self.__init_client_with_valid_token(**kwargs)

    def __init_client_with_valid_token(self, **kwargs):
        """Initialize client with a valid IBM API Connect token."""
        self.current_token = self.get_token()

        # Directly initialize necessary properties from AbstractTextGenerationClient
        from ...constants import TEMPLATE, TEMPLATE_NAME
        from ..abstract_text_generation_client import AbstractTextGenerationClient
        from ...retry_handler import RetryHandler

        # Initialize AbstractTextGenerationClient properties manually
        AbstractTextGenerationClient.__init__(
            self,
            template=kwargs.pop(TEMPLATE, None),
            template_name=kwargs.pop(TEMPLATE_NAME, None),
            **{
                k: v
                for k, v in kwargs.items()
                if k
                not in [
                    "service_account_credentials",
                    "service_account_key_file",
                    "region",
                    "project",
                    "api_key",
                ]
            },
        )

        # Set up GoogleGenAiTextClient specific properties you need
        self.client_config = GoogleClientConfig(
            type=GoogleClientType.GOOGLE,
            service_account_credentials=None,
            service_account_key_file=None,
            region=kwargs.get("region"),
            project=kwargs.get("project"),
            api_key=None,
        )

        self.safety_settings = kwargs.get("safety_settings")

        retries = kwargs.get("retries", 0)
        self.retry_handler = RetryHandler(max_retries=retries)

        # Create our custom IBM API Connect client
        self.client = self._create_ibm_apiconnect_client()

    def _create_ibm_apiconnect_client(self):
        """Create IBM API Connect client using current token."""
        if not self.api_gateway_base_url:
            raise ValueError("IBM API Connect base URL must be provided")

        # Create custom Google client configuration
        ibm_google_client = IBMApiConnectGoogleClient(
            config=self.client_config,
            api_gateway_base_url=self.api_gateway_base_url,
            api_gateway_token=self.current_token,
        )

        return ibm_google_client.client

    def get_token(self, cache={"token": None, "expires_at": 0}):
        """
        Get IBM API Connect token with caching.
        Similar to Azure OpenAI implementation but for IBM API Connect.
        """
        if cache["token"] and time.time() < cache["expires_at"]:
            return cache["token"]

        if not all([self.apiconnect_url, self.client_id, self.client_secret]):
            raise EnvironmentError(
                "Required IBM API Connect parameters not configured: apiconnect_url, client_id, client_secret"
            )

        try:
            response = requests.post(
                self.apiconnect_url,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=f"client_id={self.client_id}&client_secret={self.client_secret}&grant_type=client_credentials&scope={self.scope or ''}",
                timeout=30,
            )
            response.raise_for_status()

            token_data = response.json()
            cache["token"] = token_data["access_token"]
            cache["expires_at"] = (
                time.time() + token_data.get("expires_in", 3600) - 10
            )  # 10 second buffer

            return cache["token"]

        except requests.RequestException as e:
            raise RuntimeError(f"Error obtaining IBM API Connect token: {e}")
        except KeyError as e:
            raise RuntimeError(f"Invalid token response format: {e}")

    def ask_call(self, **kwargs):
        """
        Override ask_call to handle token renewal.
        If token is still valid, use current client. If expired, renew token and reinitialize client.
        """
        try:
            # Check if token is still valid by comparing with fresh token
            if self.current_token == self.get_token():
                return super().ask_call(**kwargs)
            else:
                # Token has expired, reinitialize client with new token
                self.__init_client_with_valid_token(
                    **self.smss_init_model_engine_params
                )
                return super().ask_call(**kwargs)
        except Exception as e:
            # If there's an authentication error, try to renew token
            if "401" in str(e) or "unauthorized" in str(e).lower():
                self.__init_client_with_valid_token(
                    **self.smss_init_model_engine_params
                )
                return super().ask_call(**kwargs)
            else:
                raise e

    def _handle_streaming(self, contents, config, prefix="", **kwargs):
        """
        Override streaming to handle token renewal.
        """
        try:
            # Check if token is still valid
            if self.current_token == self.get_token():
                return super()._handle_streaming(
                    contents=contents, config=config, prefix=prefix
                )
            else:
                # Token has expired, reinitialize client with new token
                self.__init_client_with_valid_token(
                    **self.smss_init_model_engine_params
                )
                return super()._handle_streaming(
                    contents=contents, config=config, prefix=prefix
                )
        except Exception as e:
            # If there's an authentication error, try to renew token
            if "401" in str(e) or "unauthorized" in str(e).lower():
                self.__init_client_with_valid_token(
                    **self.smss_init_model_engine_params
                )
                return super()._handle_streaming(
                    contents=contents, config=config, prefix=prefix
                )
            else:
                raise e

    def _count_tokens(self, contents, **kwargs):
        """
        Override token counting to handle token renewal.
        """
        try:
            # Check if token is still valid
            if self.current_token == self.get_token():
                return super()._count_tokens(contents)
            else:
                # Token has expired, reinitialize client with new token
                self.__init_client_with_valid_token(
                    **self.smss_init_model_engine_params
                )
                return super()._count_tokens(contents)
        except Exception as e:
            # If there's an authentication error, try to renew token
            if "401" in str(e) or "unauthorized" in str(e).lower():
                self.__init_client_with_valid_token(
                    **self.smss_init_model_engine_params
                )
                return super()._count_tokens(contents)
            else:
                raise e
