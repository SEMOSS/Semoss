from typing import List, Any, Optional, Dict
import dataclasses
from pydantic import BaseModel, Field

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
    """

    response: Any = None
    response_tokens: int = 0
    prompt_tokens: int = 0

    def to_dict(self):
        # Map attribute names to desired dictionary keys
        key_mapping = {
            "response_tokens": "numberOfTokensInResponse",
            "prompt_tokens": "numberOfTokensInPrompt",
        }

        # Filter out attributes with None values and use the custom keys
        non_none_attributes = {
            key_mapping.get(key, key): value
            for key, value in dataclasses.asdict(self).items()
            if value is not None
        }

        return non_none_attributes

    def __str__(self):
        return str(self.to_dict())


# I WILL CHANGE THIS NAME IN LAST COMMIT
class AskModelEngineResponse2(BaseModel):
    response: Any = ""
    prompt_tokens: int = Field(default=0, serialization_alias="numberOfTokensInPrompt")
    response_tokens: int = Field(
        default=0, serialization_alias="numberOfTokensInResponse"
    )
    cache_read_tokens: Optional[int] = Field(
        default=None, serialization_alias="numberOfCacheReadTokens"
    )
    cache_creation_tokens: Optional[int] = Field(
        default=None, serialization_alias="numberOfCacheCreationTokens"
    )
    # Observational only; already included in response_tokens. Do not add to billing math.
    thinking_tokens: Optional[int] = Field(
        default=None, serialization_alias="numberOfThinkingTokens"
    )
    schemaVersion: Optional[int] = None
    io: Optional[str] = None
    parts: Optional[List[Dict[str, Any]]] = None
    messageType: str = "CHAT"

    metadata: Optional[Dict[str, Any]] = None

    model_config = {"populate_by_name": True, "serialize_by_alias": True}


@dataclasses.dataclass
class AskModelEngineResponse(AbstractModelEngineResponse):
    """
    A text-generation model engine response object for text-generation

    Attributes:
        response: response from api.
        response_media: any type of media response from the api including base64 images, audio bytes, etc.
        responseTokens: response token count.
        promptTokens: prompt token count.
        messageType: response message type
        thinking: list of thoughts generated during processing based on extended thinking
        warning: warning message sent back with the response when a param was adjusted at runtime.
        tokens: the response tokens
        logprobs: logprob for a given token
    """

    response: Any = ""
    response_media: Optional[List[Any]] = None
    response_tokens: int = 0
    prompt_tokens: int = 0
    messageType: str = "CHAT"
    thinking: Optional[List[str]] = None

    # Parts-based response payload (preferred by newer Java servers).
    schemaVersion: Optional[int] = None
    io: Optional[str] = None  # "INPUT" | "OUTPUT"
    parts: Optional[List[Dict[str, Any]]] = None

    warning: Optional[str] = None
    tokens: Optional[List[str]] = None
    logprobs: Optional[List[float]] = None

    def __post_init__(self):
        # Convert empty string to None for thinking field
        if self.thinking == "":
            self.thinking = None


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


class EmbeddingsModelEngineResponse2(BaseModel):
    response: List[List[float]]
    response_tokens: int = Field(
        default=0, serialization_alias="numberOfTokensInResponse"
    )
    prompt_tokens: int = Field(
        default=0, serialization_alias="numberOfTokensInPrompt"
    )
    metadata: Optional[Dict[str, Any]] = None

    model_config = {"populate_by_name": True, "serialize_by_alias": True}
