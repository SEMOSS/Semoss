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
    detail: Optional[str] = OpenAIImageDetail.AUTO.value


class OpenAITextContentPart(BaseModel):
    type: str = "text"
    text: str


class OpenAIImageContentPart(BaseModel):
    type: str = "image_url"
    image_url: OpenAIImageURL


class OpenAIResponsesImageContentPart(BaseModel):
    type: str = "input_image"
    image_url: str


class OpenAIMessage(BaseModel):
    role: str
    content: Union[
        str,
        List[
            Union[
                OpenAITextContentPart,
                OpenAIImageContentPart,
                OpenAIResponsesImageContentPart,
            ]
        ],
    ]
