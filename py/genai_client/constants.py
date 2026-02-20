from typing import List, Any, Optional
import dataclasses

MODEL_NAME = "model_name"
MAX_TOKENS = "max_tokens"
MAX_INPUT_TOKENS = "max_input_tokens"
CHAT_TYPE = "chat_type"

# base client template keys
TEMPLATE = "template"
TEMPLATE_NAME = "template_name"
FULL_PROMPT = "full_prompt"
IMAGE_ENCODED = "image_encoded"
IMAGE_URL = "image_url"
IMAGE_EXTENSION = "jpeg"


@dataclasses.dataclass
class AbstractModelEngineResponse:
    """
    A model engine response object
    Attributes:
        response: response from api.
        responseTokens: response token count.
        promptTokens: prompt token count.
        usage_map: full token usage map for the response
    """

    response: Any = None
    response_tokens: int = 0
    prompt_tokens: int = 0
    usage_map: Optional[dict] = None

    def to_dict(self, additional_keys: Optional[dict] = None) -> dict:
        # Map attribute names to desired dictionary keys
        key_mapping = {
            "response_tokens": "numberOfTokensInResponse",
            "prompt_tokens": "numberOfTokensInPrompt",
            "usage_map": "providerUsageMap",
        }

        if additional_keys:
            key_mapping.update(additional_keys)

        # Filter out attributes with None values and use the custom keys
        non_none_attributes = {
            key_mapping.get(key, key): value
            for key, value in dataclasses.asdict(self).items()
            if value is not None
        }

        return non_none_attributes

    def __str__(self):
        return str(self.to_dict())


@dataclasses.dataclass
class AskModelEngineResponse(AbstractModelEngineResponse):
    """
    A text-generation model engine response object for text-generation

    Attributes:
        response: response from api.
        response_media: any type of media response from the api including base64 images, audio bytes, etc.
        responseTokens: response token count.
        promptTokens: prompt token count.
        thinkingTokens: token count for thinking response if enabled.
        cachedTokens: token count retrieved from cache if prompt caching is enabled.
        messageType: response message type
        thinking: list of thoughts generated during processing based on extended thinking
        warning: warning message sent back with the response when a param was adjusted at runtime.
        tokens: the response tokens
        logprobs: logprob for a given token
    """

    response: Any = ""
    response_media: Optional[List[Any]] = None
    response_tokens: int = 0
    thinking_tokens: Optional[int] = None
    prompt_tokens: int = 0
    cached_tokens: Optional[int] = None
    messageType: str = "CHAT"
    thinking: Optional[List[str]] = None
    warning: Optional[str] = None
    tokens: Optional[List[str]] = None
    logprobs: Optional[List[float]] = None

    def __post_init__(self):
        # Convert empty string to None for thinking field
        if self.thinking == "":
            self.thinking = None

    def to_dict(self) -> dict:

        additional_keys = {
            "thinking_tokens": "numberOfThinkingTokens",
            "cached_tokens": "numberOfCachedTokens",
        }

        return super().to_dict(additional_keys=additional_keys)


class EmbeddingsModelEngineResponse(AbstractModelEngineResponse):
    """
    A embeddings engine response object

    Attributes:
        response: response from api.
        responseTokens: response token count.
        promptTokens: prompt token count.
        warning: warning message sent back with the response when a param was adjusted at runtime.
        tokens: the response tokens
        logprobs: logprob for a given token
    """

    response: List[float]
    response_tokens: int = 0
    prompt_tokens: int = 0
