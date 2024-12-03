from typing import List, Any
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


class AskModelEngineResponse(AbstractModelEngineResponse):
    """
    A text-generation model engine response object for text-generation

    Attributes:
        response: response from api.
        responseTokens: response token count.
        promptTokens: prompt token count.
        warning: warning message sent back with the response when a param was adjusted at runtime.
        tokens: the response tokens
        logprobs: logprob for a given token
    """

    response: str = ""
    response_tokens: int = 0
    prompt_tokens: int = 0
    warning: str = None
    tokens: List[str] = None
    logprobs: List[float] = None


class InstructModelEngineResponse(AbstractModelEngineResponse):
    """
    A text-generation model engine response object for instruction specific text-generation

    Attributes:
        response: response from api.
        responseTokens: response token count.
        promptTokens: prompt token count.
        warning: warning message sent back with the response when a param was adjusted at runtime.
        tokens: the response tokens
        logprobs: logprob for a given token
    """

    response: List[str] = []
    response_tokens = 0
    prompt_tokens: int = 0
    warning: str = None
    tokens: List[str] = None
    logprobs: List[float] = None


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
