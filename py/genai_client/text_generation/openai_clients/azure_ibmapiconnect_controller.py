import os
from typing import List, Dict
from dotenv import load_dotenv
import requests
import time
from .openai_controller import AzureOpenAiClientController
from .openai_client import OpenAiClient


load_dotenv()


class IBMApiConnectAzureClientController(AzureOpenAiClientController):
    def __init__(self, **kwargs):
        """
        Initializes the IBM API Connect Azure Client Controller.
        """
        self.chat_type = kwargs.pop("chat_type", "chat-completion")
        self.api_version = kwargs.get("api_version")
        self.smss_init_model_engine_params = kwargs
        self.__init_client_with_valid_token(**self.smss_init_model_engine_params)

    def __init_client_with_valid_token(self, **kwargs):
        """
        Initializes the OpenAI client with a valid token.
        """
        self.token = self.get_token()
        kwargs["default_headers"] = {"Authorization": "Bearer " + self.token}
        kwargs["chat_type"] = self.chat_type

        if self.api_version and "api_version" not in kwargs:
            kwargs["api_version"] = self.api_version

        self.azure_openai_class = OpenAiClient(is_azure=True, **kwargs)

    def get_token(self, cache={"token": None, "expires_at": 0}):
        """
        Retrieves a valid token from IBM API Connect, using cache if not expired.
        """
        if cache["token"] and time.time() < cache["expires_at"]:
            return cache["token"]

        apiconnect_url = os.getenv("ARCHITECTURE_ACCESS_TOKEN_URL")
        client_id = os.getenv("APICONNECT_CLIENT")
        client_secret = os.getenv("APICONNECT_SECRET")
        scope = os.getenv("APICONNECT_SCOPE")

        if not all([apiconnect_url, client_id, client_secret]):
            raise EnvironmentError(
                "Required environment variables (ARCHITECTURE_ACCESS_TOKEN_URL, APICONNECT_CLIENT, APICONNECT_SECRET) not set"
            )

        r = requests.post(
            apiconnect_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=f"client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials&scope={scope}",
        )
        cache["token"] = r.json()["access_token"]
        cache["expires_at"] = time.time() + r.json()["expires_in"] - 10
        return cache["token"]

    def ask(self, **kwargs) -> Dict:
        """
        Sends a prompt to the OpenAI client and returns the response.
        If the token has expired, refreshes the token and reinitializes the client.
        """
        # If the token has not expired, use the current token
        if self.token == self.get_token():
            return self.azure_openai_class.ask(**kwargs)

        # If the token has expired, obtain a new token and reinitialize the client
        self.__init_client_with_valid_token(**self.smss_init_model_engine_params)
        return self.azure_openai_class.ask(**kwargs)

    def embeddings(self, **kwargs) -> List[float]:
        """
        Requests embeddings from the OpenAI client.
        If the token has expired, refreshes the token and reinitializes the client.
        """
        # If the token has not expired, use the current token
        if self.token == self.get_token():
            return self.azure_openai_class.embeddings(**kwargs)

        # If the token has expired, obtain a new token and reinitialize the client
        self.__init_client_with_valid_token(**self.smss_init_model_engine_params)
        return self.azure_openai_class.embeddings(**kwargs)
