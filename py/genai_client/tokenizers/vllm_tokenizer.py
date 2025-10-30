from pydantic import BaseModel
from ..utils import validate_with


class TokenizerConfig(BaseModel):
    endpoint: str
    model_name: str
    api_key: str = "EMPTY"


class ExternalTokenizer:
    @validate_with(TokenizerConfig)
    def __init__(self, endpoint: str, model_name: str, api_key: str = "EMPTY"):
        self.endpoint = endpoint
        self.model_name = model_name
        self.api_key = api_key
