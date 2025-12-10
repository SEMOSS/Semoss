from typing import List, Optional, Dict, Any, Union, Literal
from pydantic import BaseModel, ConfigDict, field_validator
from enum import Enum
import re


class BedrockRoles(Enum):
    USER = "user"
    ASSISTANT = "assistant"

    class Config:
        use_enum_values = True


BYTE_TYPE = bytes


class BedrockURI(BaseModel):
    uri: str


class BedrockS3Location(BaseModel):
    s3Location: BedrockURI


class BedrockImageSource(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    bytes: Union[str, BYTE_TYPE] = None


class BedrockImageBlock(BaseModel):
    source: Union[BedrockImageSource, BedrockS3Location]
    format: Literal["png", "jpeg", "webp", "gif"]


class BedrockDocumentSource(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    bytes: Union[str, BYTE_TYPE] = None


class BedrockDocumentBlock(BaseModel):
    source: Union[BedrockDocumentSource, BedrockS3Location]
    format: Literal["pdf", "csv", "doc", "docx", "xls", "xlsx", "html", "txt", "md"]
    name: str

    @field_validator("name")
    @classmethod
    def clean_name(cls, v: str) -> str:
        """
        Clean the document name to conform to AWS Bedrock requirements:
        - Only alphanumeric, whitespace, hyphens, parentheses, square brackets, and periods
        - No consecutive whitespace characters
        """
        # Remove any path separators and get just the filename
        v = v.split("/")[-1].split("\\")[-1]

        # Remove file extension
        if "." in v:
            v = v.rsplit(".", 1)[0]  # Remove last extension only

        # Keep only allowed characters: alphanumeric, whitespace, hyphens, parentheses, square brackets, periods
        v = re.sub(r"[^\w\s\-\(\)\[\].]", "", v)

        # Replace consecutive whitespace with single space
        v = re.sub(r"\s+", " ", v)

        # Strip leading/trailing whitespace
        v = v.strip()

        # Ensure there's a valid name
        if not v:
            v = "document.pdf"

        return v


class BedrockTextBlock(BaseModel):
    text: str


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
