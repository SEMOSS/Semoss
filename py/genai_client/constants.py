from typing import List, Any, Optional, Dict
import dataclasses
from pydantic import BaseModel, Field
from typing_extensions import deprecated

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

class AbstractModelEngineResponse(BaseModel):
    """
    An abstract model engine response object

    Attributes:
        response: response from api.
        prompt_tokens: prompt token count.
        response_tokens: response token count.
        usage_map: provider usage map if given, otherwise None
    """
    response: Any = ""
    prompt_tokens: int = Field(default=0, serialization_alias="numberOfTokensInPrompt")
    response_tokens: int = Field(
        default=0, serialization_alias="numberOfTokensInResponse"
    )
    usage_map: Optional[Any] = Field(default=None, serialization_alias="providerUsageMap")

# TODO: Change the name to AskModelEngineResponse before release.
class AskModelEngineResponse2(AbstractModelEngineResponse):
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
    thinking_tokens: Optional[int] = Field(default=None, serialization_alias="numberOfThinkingTokens")
    cached_tokens: Optional[int] = Field(default=None, serialization_alias="numberOfCachedTokens")
    schemaVersion: Optional[int] = None
    io: Optional[str] = None
    parts: Optional[List[Dict[str, Any]]] = None
    messageType: str = "CHAT"
    model_config = {"populate_by_name": True, "serialize_by_alias": True}

@deprecated("AskModelEngineResponse is deprecated. Use AskModelEngineResponse2 instead.")
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

    response_media: Optional[List[Any]] = None
    thinking_tokens: Optional[int] = Field(default=None, serialization_alias="numberOfThinkingTokens")
    cached_tokens: Optional[int] = Field(default=None, serialization_alias="numberOfCachedTokens")
    messageType: str = "CHAT"
    thinking: Optional[List[str]] = None

    # Parts-based response payload (preferred by newer Java servers).
    schemaVersion: Optional[int] = None
    io: Optional[str] = None  # "INPUT" | "OUTPUT"
    parts: Optional[List[Dict[str, Any]]] = None

    warning: Optional[str] = None
    tokens: Optional[List[str]] = None
    logprobs: Optional[List[float]] = None

    def to_dict(self, additional_keys: Optional[dict] = None) -> dict:
        # Build key mapping from field serialization aliases
        key_mapping = {}
        for field_name, field_info in self.__class__.model_fields.items():
            # Use serialization_alias if present, otherwise field name stays as is
            if field_info.serialization_alias:
                key_mapping[field_name] = field_info.serialization_alias
        
        if additional_keys:
            key_mapping.update(additional_keys)

        # Filter out attributes with None values and use the custom keys
        non_none_attributes = {
            key_mapping.get(key, key): value
            for key, value in self.model_dump().items()
            if value is not None
        }

        return non_none_attributes

    def __str__(self):
        return str(self.to_dict())


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