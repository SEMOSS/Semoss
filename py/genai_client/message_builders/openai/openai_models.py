from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel
from ...utils import StringEnum


class OpenAIRoles(StringEnum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"
    DEVELOPER = "developer"
    TOOL = "tool"


class OpenAIImageDetail(StringEnum):
    LOW = "low"
    HIGH = "high"
    AUTO = "auto"


class OpenAIImageURL(BaseModel):
    url: str
    detail: Optional[OpenAIImageDetail] = OpenAIImageDetail.AUTO


class OpenAITextContentPart(BaseModel):
    type: str = "text"
    text: str


class OpenAIImageContentPart(BaseModel):
    type: str = "image_url"
    image_url: OpenAIImageURL


class OpenAIMessage(BaseModel):
    role: str
    content: Union[str, List[Union[OpenAITextContentPart, OpenAIImageContentPart]]]


class ChatCompletionsConfig(BaseModel):
    messages: List[OpenAIMessage]
    model: str
    frequency_penalty: Optional[float] = None
    logit_bias: Optional[Dict[str, float]] = None
    max_completion_tokens: Optional[int] = None
    temperature: Optional[float] = None
    presence_penalty: Optional[float] = None
    stop: Optional[Union[str, List[str]]] = None
    stream: Optional[bool] = None
    top_p: Optional[float] = None
