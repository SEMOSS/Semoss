from typing import List, Literal, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, field_validator
from ...utils import StringEnum
import json


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


class OpenAIFile(BaseModel):
    filename: str
    file_data: str


class OpenAITextContentPart(BaseModel):
    type: str = "text"
    text: str


class OpenAIImageContentPart(BaseModel):
    type: str = "image_url"
    image_url: OpenAIImageURL


class OpenAIFileContentPart(BaseModel):
    type: str = "file"
    file: OpenAIFile


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


class OpenAIToolFunctionPart(BaseModel):
    """Represents the function call portion inside a tool_call object."""

    name: str
    arguments: Any  # accepts either dict or str

    @field_validator("arguments", mode="before")
    def ensure_json_string(cls, v):
        """Ensure arguments is serialized JSON string (OpenAI requirement)."""
        if isinstance(v, (dict, list)):
            return json.dumps(v, ensure_ascii=False)
        return v


class OpenAIToolCall(BaseModel):
    """Represents a single tool_call object from OpenAI."""

    id: str
    type: str = "function"
    function: OpenAIToolFunctionPart


class OpenAIMessage(BaseModel):
    role: str
    content: Union[
        str,
        List[
            Union[
                OpenAITextContentPart,
                OpenAIImageContentPart,
                OpenAIFileContentPart,
                OpenAIToolChatCompletionContentPart,
            ]
        ],
    ]
    tool_calls: Optional[List[OpenAIToolCall]] = None
    tool_call_id: Optional[str] = None


# --------RESPONSES API MODELS ---------------
class OpenAIResponsesImageContentPart(BaseModel):
    type: str = "input_image"
    image_url: str


class OpenAIResponsesFileContentPart(BaseModel):
    type: str = "input_file"
    filename: str
    file_data: str


class OpenAIResponsesToolCallOutput(BaseModel):
    type: str = "function_call_output"
    call_id: str
    output: Any


class OpenAIResponsesToolCall(BaseModel):
    type: str = "function_call"
    call_id: str
    name: str
    arguments: Any  # may be dict or JSON string

    @field_validator("arguments", mode="before")
    def ensure_json_string(cls, v):
        import json

        if isinstance(v, (dict, list)):
            return json.dumps(v, ensure_ascii=False)
        return v


class OpenAIResponsesMessage(BaseModel):
    role: str
    content: Union[
        str,
        List[
            Union[
                OpenAITextContentPart,
                OpenAIResponsesImageContentPart,
                OpenAIResponsesFileContentPart,
            ]
        ],
    ]
