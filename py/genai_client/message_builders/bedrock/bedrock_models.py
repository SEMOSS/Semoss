from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel
from ...utils import StringEnum


class BedrockRoles(StringEnum):
    USER = "user"
    ASSISTANT = "assistant"


class BedrockImageFormat(StringEnum):
    PNG = "png"
    jpeg = "jpeg"
    WEBP = "webp"
    GIF = "gif"


class BedrockImageSource(BaseModel):
    url: Optional[str] = None
    bytes: Optional[str] = None


class BedrockImageBlock(BaseModel):
    source: BedrockImageSource
    format: BedrockImageFormat


class BedrockDocumentFormat(StringEnum):
    PDF = "pdf"
    CSV = "csv"
    DOC = "doc"
    DOCX = "docx"
    XLS = "xls"
    XLSX = "xlsx"
    HTML = "html"
    TXT = "txt"
    MD = "md"


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


BedrockContentBlock = Union[
    BedrockTextBlock,
    BedrockImageBlock,
    BedrockDocumentBlock,
    BedrockToolUseBlock,
    BedrockToolResultBlock,
]


class BedrockMessage(BaseModel):
    role: str
    content: List[BedrockContentBlock]


## MODEL PARAMETERS -----------------------------------


class BedrockSystemBlock(BaseModel):
    text: str


class BedrockInferenceConfig(BaseModel):
    maxTokens: Optional[int] = None
    stopSequences: Optional[List[str]] = None
    temperature: Optional[float] = None
    topP: Optional[float] = None


class BedrockRequest(BaseModel):
    messages: List[BedrockMessage]
    system: Optional[BedrockSystemBlock] = None
    inferenceConfig: Optional[BedrockInferenceConfig] = None
    additionalModelRequestFields: Optional[Dict[str, Any]] = None
    toolConfig: Optional[Any] = None
