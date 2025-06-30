from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from ...utils import StringEnum


class SEMOSSMessageType(StringEnum):
    INPUT_TEXT = "INPUT_TEXT"
    INPUT_MEDIA = "INPUT_MEDIA"
    INPUT_TOOL_EXEC = "INPUT_TOOL_EXEC"
    RESPONSE_TEXT = "RESPONSE_TEXT"
    RESPONSE_TOOL = "RESPONSE_TOOL"
    RESPONSE_MEDIA = "RESPONSE_MEDIA"


class SEMOSSImageType(StringEnum):
    URL = "url"
    BASE64 = "base64"


class SEMOSSToolType(StringEnum):
    FUNCTION = "function"


class SEMOSSToolFunction(BaseModel):
    """Represents a tool function definition"""

    name: str
    description: str
    parameters: Dict[str, Any]


class SEMOSSToolCall(BaseModel):
    """Represents a tool call"""

    function: SEMOSSToolFunction
    type: SEMOSSToolType = SEMOSSToolType.FUNCTION
    id: Optional[str] = None


class SEMOSSToolResponse(BaseModel):
    """Represents a tool response"""

    id: str
    type: SEMOSSToolType
    name: str
    arguments: str


class SEMOSSImageContent(BaseModel):
    type: SEMOSSImageType
    data: Optional[str] = None
    format: Optional[str] = None
    mime_type: Optional[str] = None
    file_name: Optional[str] = None
    url: Optional[str] = None


class SEMOSSMessage(BaseModel):
    type: SEMOSSMessageType
    content: Optional[str] = None
    image_content: Optional[List[SEMOSSImageContent]] = None

    tool_calls: List[SEMOSSToolCall] = Field(default_factory=list, alias="tool_calls")
    tool_call_id: Optional[str] = Field(None, alias="tool_call_id")
    tool_responses: List[SEMOSSToolResponse] = Field(
        default_factory=list, alias="tool_responses"
    )

    param_map: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        allow_population_by_field_name = True
        use_enum_values = True
