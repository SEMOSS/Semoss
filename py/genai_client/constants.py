import dataclasses

MODEL_NAME = "model_name"
MAX_TOKENS = "max_tokens"
MAX_INPUT_TOKENS = "max_input_tokens"
CHAT_TYPE = "chat_type"

# base client template keys
TEMPLATE = "template"
TEMPLATE_NAME = "template_name"
FULL_PROMPT = "full_prompt"

# Constants for Model Engine Response Payload
class ModelEngineResponseKeys:
    RESPONSE = "response"
    NUMBER_OF_TOKENS_IN_PROMPT = "numberOfTokensInPrompt"
    NUMBER_OF_TOKENS_IN_RESPONSE = "numberOfTokensInResponse"


@dataclasses.dataclass
class ModelEngineResponse:
    """A model engine response object

    Attributes:
        response: response from api.
        responseTokens: response token count.
        promptTokens: prompt token count.
    """
    
    response: str = ''
    response_tokens: int = 0
    prompt_tokens: int = 0

    def to_dict(self):
        return {
            "response": self.response,
            "numberOfTokensInPrompt": self.prompt_tokens,
            "numberOfTokensInResponse": self.response_tokens,
        }