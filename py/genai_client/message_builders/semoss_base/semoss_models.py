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
    tool_calls: Optional[List[SEMOSSToolCall]] = Field(default_factory=list)
    tool_call_id: Optional[str] = None
    tool_responses: Optional[List[SEMOSSToolResponse]] = Field(default_factory=list)

    param_map: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        allow_population_by_field_name = True
        use_enum_values = True


class AskSettings(BaseModel):
    """
    Represents all of the conditional settings that affect the model call but are not passed
    as parameters to the model call itself.

    *NOTE: The only purpose for this right now is for the new clients until we fully go to semoss messages.
    """

    full_prompt: Optional[List[Dict]] = None
    streaming: bool = False
    use_history: bool = True
    history: Optional[List[Dict]] = None
    image_url: Optional[List[str]] = None
    image_encoded: Optional[List[str]] = None
    semoss_messages: Optional[List[SEMOSSMessage]] = None
    system_prompt: Optional[str] = None
    extra_params: Optional[Dict[str, Any]] = None


class ModelSettings(BaseModel):
    """These are attributes I want set in the SMSS file for each model"""

    model_name: str
    context_window: Optional[int] = None
    max_completion_tokens: Optional[int] = None
    max_input_tokens: Optional[int] = None
    ai_role: Optional[str] = None
    user_role: Optional[str] = None
    system_role: Optional[str] = None
    model_type: Optional[str] = None
    chat_type: Optional[str] = None
    tokens_param_name: Optional[str] = None
