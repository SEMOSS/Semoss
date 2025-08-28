from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
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


class ToolFunctionParameters(BaseModel):
    """JSON schema for the function parameters."""

    type: str = Field("object", description="Must be 'object'")
    properties: Dict[str, Any] = Field(
        ..., description="Parameters definition as JSON schema"
    )
    required: Optional[List[str]] = Field(
        default_factory=list, description="List of required parameter names"
    )


class Tool_FunctionDef(BaseModel):
    """Function definition for OpenAI tools."""

    name: str = Field(..., description="The function name")
    description: Optional[str] = Field(None, description="What the function does")
    parameters: ToolFunctionParameters = Field(
        ..., description="Parameters JSON schema"
    )


class OpenAIToolChatCompletionContentPart(BaseModel):
    """Tool object for OpenAI chat.completions API."""

    type: str = "function"
    function: Tool_FunctionDef


class OpenAIToolResponsesContentPart(BaseModel):
    """Tool object for OpenAI responses API."""

    type: str = "function"
    name: str = Field(..., description="The function name")
    description: Optional[str] = Field(None, description="What the function does")
    parameters: ToolFunctionParameters = Field(
        ..., description="Parameters JSON schema"
    )


class OpenAIMessage(BaseModel):
    role: str
    content: Union[
        str,
        List[
            Union[
                OpenAITextContentPart,
                OpenAIImageContentPart,
                OpenAIResponsesImageContentPart,
                OpenAIToolChatCompletionContentPart,
                OpenAIToolResponsesContentPart,
            ]
        ],
    ]
