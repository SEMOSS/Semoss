from typing import List, Optional, Dict, Any, Union, Literal
from pydantic import BaseModel
from enum import Enum


class BedrockRoles(Enum):
    USER = "user"
    ASSISTANT = "assistant"

    class Config:
        use_enum_values = True


class BedrockImageFormat(Enum):
    PNG = "png"
    JPEG = "jpeg"
    WEBP = "webp"
    GIF = "gif"

    class Config:
        use_enum_values = True


class BedrockDocumentFormat(Enum):
    PDF = "pdf"
    CSV = "csv"
    DOC = "doc"
    DOCX = "docx"
    XLS = "xls"
    XLSX = "xlsx"
    HTML = "html"
    TXT = "txt"
    MD = "md"

    class Config:
        use_enum_values = True


BYTE_TYPE = bytes


class BedrockImageSource(BaseModel):
    url: Optional[str] = None
    bytes: Optional[Union[str, BYTE_TYPE]] = None

    class Config:
        arbitrary_types_allowed = True


class BedrockImageBlock(BaseModel):
    source: BedrockImageSource
    format: Literal["png", "jpeg", "webp", "gif"]


class BedrockDocumentSource(BaseModel):
    bytes: Optional[str] = None
    s3Location: Optional[str] = None


class BedrockDocumentBlock(BaseModel):
    format: BedrockDocumentFormat
    name: str
    source: BedrockDocumentSource


class BedrockTextBlock(BaseModel):
    text: str


class DocumentContentBlock(BaseModel):
    document: BedrockDocumentBlock


class BedrockToolUseBlock(BaseModel):
    tool_use: Dict[str, Any]


class BedrockToolResultBlock(BaseModel):
    tool_result: Dict[str, Any]


class BedrockTextContentBlock(BaseModel):
    text: str


class BedrockImageContentBlock(BaseModel):
    image: BedrockImageBlock


class BedrockDocumentContentBlock(BaseModel):
    document: BedrockDocumentBlock


class BedrockToolUseContentBlock(BaseModel):
    toolUse: Dict[str, Any]


class BedrockToolResultContentBlock(BaseModel):
    toolResult: Dict[str, Any]


BedrockContentBlock = Union[
    BedrockTextContentBlock,
    BedrockImageContentBlock,
    BedrockDocumentContentBlock,
    BedrockToolUseContentBlock,
    BedrockToolResultContentBlock,
]


class BedrockMessage(BaseModel):
    role: str
    content: List[BedrockContentBlock]


class BedrockSystemBlock(BaseModel):
    text: str


class BedrockInferenceConfig(BaseModel):
    maxTokens: Optional[int] = None
    stopSequences: Optional[List[str]] = None
    temperature: Optional[float] = None
    topP: Optional[float] = None


class BedrockRequest(BaseModel):
    messages: List[BedrockMessage]
    system: Optional[List[BedrockSystemBlock]] = None
    inferenceConfig: Optional[BedrockInferenceConfig] = None
    additionalModelRequestFields: Optional[Dict[str, Any]] = None
    toolConfig: Optional[Any] = None
