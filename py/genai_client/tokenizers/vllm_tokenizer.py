from typing import Dict, List, Optional
import requests
import logging
from pydantic import BaseModel, model_validator

logger = logging.getLogger(__name__)


class VLLMRequest(BaseModel):
    """
    Request model for tokenization.
    Only send messages when tokenizing an OpenAI chat format.
    Messages honored over prompt if both are provided.
    This will not tokenize images.
    This is the format that vLLM accepts:
        {
            "model": "string",
            "prompt": "string",
            "add_special_tokens": true,
            "return_token_strs": false,
            "additionalProp1": {}
        }
    """

    model: Optional[str] = None
    prompt: Optional[str] = None
    add_special_tokens: Optional[bool] = False
    return_token_strs: Optional[bool] = False
    messages: Optional[List[Dict]] = None

    @model_validator(mode="after")
    def validate_request(self) -> "VLLMRequest":
        if self.prompt is None and self.messages is None:
            raise ValueError("Either 'prompt' or 'messages' must be provided.")
        if self.prompt and self.messages:
            self.prompt = None
        return self


class VLLMTokenizeResponse(BaseModel):
    count: int
    max_model_len: int
    tokens: List[int]
    token_strs: Optional[List[str]] = None


class VLLMTokenizer:
    def __init__(
        self, model_name: str, endpoint: str, api_key: Optional[str] = "EMPTY"
    ):
        self.model_name = model_name
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
            url = self.endpoint.replace("/tokenize", "/ping")
            response = requests.get(url)
            return response.status_code == 200
        except requests.RequestException:
            logger.warning("Failed to ping the server for the Tokenizer.")
            return False

    def tokenize(
        self,
        prompt: Optional[str] = None,
        messages: Optional[List[Dict]] = None,
        add_special_tokens: bool = False,
        return_token_strs: bool = False,
    ) -> VLLMTokenizeResponse:
        """Send a tokenization request to the server."""
        if not self.tokenizer_available:
            raise ConnectionError("Tokenizer server is not available.")

        request_payload = VLLMRequest(
            model=self.model_name,
            prompt=prompt,
            messages=messages,
            add_special_tokens=add_special_tokens,
            return_token_strs=return_token_strs,
        ).model_dump(exclude_none=True)

        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}

        try:
            response = requests.post(
                self.endpoint, json=request_payload, headers=headers
            )
            response.raise_for_status()
            return VLLMTokenizeResponse(**response.json())
        except requests.RequestException as e:
            logger.error(f"Tokenization request failed: {e}")
            raise

    def get_token_count(
        self,
        prompt: Optional[str] = None,
        messages: Optional[List[Dict]] = None,
        add_special_tokens: bool = False,
    ) -> int:
        tokenize_response = self.tokenize(prompt, messages, add_special_tokens, False)
        return tokenize_response.count

    def get_token_strs(
        self,
        prompt: Optional[str] = None,
        messages: Optional[List[Dict]] = None,
        add_special_tokens: bool = False,
    ) -> List[str]:
        tokenize_response = self.tokenize(prompt, messages, add_special_tokens, True)
        return tokenize_response.token_strs
