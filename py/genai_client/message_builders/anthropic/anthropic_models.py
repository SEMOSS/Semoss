from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel
from ...utils import StringEnum


class AnthropicRoles(StringEnum):
    USER = "user"
    ASSISTANT = "assistant"


class AnthropicImageType(StringEnum):
    URL = "url"
    BASE64 = "base64"


class AnthropicImageMediaType(StringEnum):
    JPEG = "image/jpeg"
    PNG = "image/png"
    WEBP = "image/webp"
    GIF = "image/gif"


class AnthropicImageSourceURL(BaseModel):
    type: AnthropicImageType = AnthropicImageType.URL
    url: str


class AnthropicImageSourceBase64(BaseModel):
    type: AnthropicImageType = AnthropicImageType.BASE64
    media_type: AnthropicImageMediaType
    data: Optional[str] = None


class AnthropicImageContentPart(BaseModel):
    type: str = "image"
    source: Union[AnthropicImageSourceURL, AnthropicImageSourceBase64]


class AnthropicDocumentContentPart(BaseModel):
    type: str = "document"
    media_type: str
    data: Optional[str] = None


# FOR HISTORY
class AnthropicToolUseContentPart(BaseModel):
    type: str = "tool_use"
    id: str
    name: str
    input: Dict[str, Any]


# FOR HISTORY
class AnthropicToolResultContentPart(BaseModel):
    type: str = "tool_result"
    tool_use_id: str
    content: str


# FOR CALLING TOOLS
class AnthropicToolCall(BaseModel):
    name: str
    description: Optional[str] = None
    input_schema: Optional[Dict[str, Any]] = None


class AnthropicThinkingContentPart(BaseModel):
    type: str = "thinking"
    thinking: str
    signature: Optional[str] = None


class AnthropicTextContentPart(BaseModel):
    type: str = "text"
    text: str


class AnthropicMessage(BaseModel):
    role: AnthropicRoles
    content: Union[
        str,
        List[
            Union[
                AnthropicTextContentPart,
                AnthropicImageContentPart,
                AnthropicToolUseContentPart,
                AnthropicToolResultContentPart,
                AnthropicThinkingContentPart,
                AnthropicDocumentContentPart,
            ]
        ],
    ]
