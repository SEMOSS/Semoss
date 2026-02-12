from typing import Dict, List, Optional, Union, Any, Literal
from pydantic import BaseModel, Field
from ...utils import StringEnum


class SEMOSSMediaInputType(StringEnum):
    """Represents media input types"""

    URL = "url"
    BASE64 = "base64"


class SEMOSSMediaContent(BaseModel):
    """Represents media content in a message"""

    type: SEMOSSMediaInputType
    data: Optional[str] = None
    format: Optional[str] = None
    mime_type: Optional[str] = None
    file_name: Optional[str] = None
    url: Optional[str] = None

    class Config:
        use_enum_values = True


class SEMOSSToolFunction(BaseModel):
    """Represents a tool function definition"""

    name: str
    description: str
    parameters: Dict[str, Any]


class SEMOSSToolCall(BaseModel):
    """Represents a tool call"""

    function: SEMOSSToolFunction
    type: Literal["function"]
    id: Optional[str] = None


class SEMOSSToolResponse(BaseModel):
    """Represents a tool response"""

    id: str
    type: Literal["function"]
    name: str
    arguments: str


# new parts
class SEMOSSMessagePartType(StringEnum):
    MEDIA = "MEDIA"
    TEXT = "TEXT"
    THINKING = "THINKING"
    TOOL_CALL = "TOOL_CALL"
    TOOL_RESULT = "TOOL_RESULT"
    SYSTEM = "SYSTEM"
    UNKNOWN = "UNKNOWN"


class SEMOSSMediaMessagePart(BaseModel):
    """Represents a text input message content"""

    mediaInfo: SEMOSSMediaContent
    type: Literal[SEMOSSMessagePartType.MEDIA] = SEMOSSMessagePartType.MEDIA


class SEMOSSSystemMessagePart(BaseModel):
    """Represents a system message content"""

    prompt: str
    type: Literal[SEMOSSMessagePartType.SYSTEM] = SEMOSSMessagePartType.SYSTEM


class SEMOSSTextMessagePart(BaseModel):
    """Represents a text message content"""

    text: str
    uiText: Optional[str] = None
    type: Literal[SEMOSSMessagePartType.TEXT] = SEMOSSMessagePartType.TEXT


class SEMOSSThinkingMessagePart(BaseModel):
    """Represents a thinking message content"""

    thinking: str
    type: Literal[SEMOSSMessagePartType.THINKING] = SEMOSSMessagePartType.THINKING


class SEMOSSThinkingMessagePart(BaseModel):
    """Represents a thinking message content"""

    thinking: str
    type: Literal[SEMOSSMessagePartType.THINKING] = SEMOSSMessagePartType.THINKING


class SEMOSSToolCallMessagePart(BaseModel):
    """Represents a tool call message content"""

    toolCall: SEMOSSToolCall
    type: Literal[SEMOSSMessagePartType.TOOL_CALL] = SEMOSSMessagePartType.TOOL_CALL


class SEMOSSToolResultMessagePart(BaseModel):
    """Represents a tool result message content"""

    toolResult: SEMOSSToolResponse
    type: Literal[SEMOSSMessagePartType.TOOL_RESULT] = SEMOSSMessagePartType.TOOL_RESULT


class SEMOSSUnknownMessagePart(BaseModel):
    """Represents an unknown message part content"""

    data: Any
    type: Literal[SEMOSSMessagePartType.UNKNOWN] = SEMOSSMessagePartType.UNKNOWN


# legacy message types for backwards compatibility
class SEMOSSMessageType(StringEnum):
    INPUT_TEXT = "INPUT_TEXT"
    INPUT_MEDIA = "INPUT_MEDIA"
    INPUT_TOOL_EXEC = "INPUT_TOOL_EXEC"
    RESPONSE_TEXT = "RESPONSE_TEXT"
    RESPONSE_TOOL = "RESPONSE_TOOL"
    RESPONSE_MEDIA = "RESPONSE_MEDIA"


class SEMOSSMessage(BaseModel):
    type: SEMOSSMessageType
    content: Optional[str] = None
    media_content: Optional[List[SEMOSSMediaContent]] = None
    tool_calls: Optional[List[SEMOSSToolCall]] = Field(default_factory=list)
    tool_call_id: Optional[str] = None
    tool_responses: Optional[List[SEMOSSToolResponse]] = Field(default_factory=list)
    tokens: Optional[int] = 0
    param_map: Dict[str, Any] = Field(default_factory=dict)
    # parts
    parts: Optional[
        List[
            Union[
                SEMOSSMediaMessagePart,
                SEMOSSSystemMessagePart,
                SEMOSSTextMessagePart,
                SEMOSSThinkingMessagePart,
                SEMOSSToolCallMessagePart,
                SEMOSSToolResultMessagePart,
                SEMOSSUnknownMessagePart,
            ]
        ]
    ] = None

    class Config:
        validate_by_name = True
        use_enum_values = True


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
    thinking: Optional[bool] = False
    thinking_budget: Optional[int] = None
    global_param_override: Optional[Dict[str, Any]] = None
    modalities: Optional[List[str]] = None
