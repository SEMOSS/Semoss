from typing import Dict, List, Optional, Any, Union
import requests
import logging
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class TGIRequest(BaseModel):
    inputs: str
    parameters: Optional[Dict[str, Any]] = None


class TGITokenizer:
    def __init__(self, endpoint: str, api_key: Optional[str] = "EMPTY"):
        self.api_key = api_key
        self.endpoint = endpoint

        if self.endpoint.endswith("/v1"):
            self.endpoint = self.endpoint.replace("v1", "tokenize")
        elif self.endpoint.endswith("/"):
            self.endpoint = self.endpoint + "tokenize"
        else:
            self.endpoint = self.endpoint + "/tokenize"

        self.tokenizer_available = self.ping_server()
        if not self.tokenizer_available:
            logger.warning("The Tokenizer server is not reachable.")
        else:
            logger.info("Successfully connected to the Tokenizer")

    def ping_server(self) -> bool:
        """Ping the server to verify availability."""
        try:
            url = self.endpoint.replace("/tokenize", "/health")
            response = requests.get(url)
            return response.status_code == 200
        except requests.RequestException:
            logger.warning("Failed to ping the server for the Tokenizer.")
            return False

    def tokenize(
        self, prompt: Optional[str] = None, params: Optional[str] = {}
    ) -> List[Dict[str, Union[str, int]]]:
        """Send a tokenization request to the server."""
        if not self.tokenizer_available:
            raise ConnectionError("Tokenizer server is not available.")

        request_payload = TGIRequest(inputs=prompt, params=params).model_dump()

        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}

        try:
            response = requests.post(
                self.endpoint, json=request_payload, headers=headers
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Tokenization request failed: {e}")
            raise

    def get_token_count(
        self,
        prompt: Optional[str] = None,
        params: Optional[Dict[str, Union[int, str]]] = {},
    ) -> int:
        tokenize_response = self.tokenize(prompt, params)
        return len(tokenize_response)
