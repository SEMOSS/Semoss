from typing import Dict, List, Optional, Union, Any, Literal
from pydantic import AliasChoices, BaseModel, Field, field_validator
from ...utils import StringEnum
import json
from deprecated import deprecated


class SEMOSSMediaInputType(StringEnum):
    """Represents media input types"""

    URL = "url"
    BASE64 = "base64"


class SEMOSSMediaContent(BaseModel):
    """Represents media content in a message"""

    type: SEMOSSMediaInputType
    data: Optional[str] = None
    format: Optional[str] = None
    mime_type: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("mime_type", "mimeType")
    )
    file_name: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("file_name", "fileName")
    )
    url: Optional[str] = None

    class Config:
        use_enum_values = True


class SEMOSSToolFunction(BaseModel):
    """Represents a tool function definition"""

    name: str
    description: str
    parameters: Union[Dict[str, Any], str] = {}

    @field_validator("parameters", mode="before")
    @classmethod
    def parse_parameters(cls, v):
        if v == "":
            return {}

        # If it's already a dict, return it
        if isinstance(v, dict):
            return v

        # If it's a string, try to parse as JSON
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, dict):
                    return parsed
                # If the first parse returns a string, the JSON was double-encoded
                if isinstance(parsed, str):
                    parsed2 = json.loads(parsed)
                    if isinstance(parsed2, dict):
                        return parsed2
                raise ValueError("Parsed JSON is not a dictionary")
            except (json.JSONDecodeError, ValueError):
                # Tool call arguments from models can contain deeply-nested
                # escape sequences (e.g. CSS/code edits with newlines and
                # quotes) that become malformed JSON after SEMOSS message
                # serialization round-trips. Return the raw string as-is
                # so the downstream message builder (e.g. OpenAI) can
                # forward it directly as the arguments string.
                return v

        return v


class SEMOSSToolCall(BaseModel):
    """Wrapper around the tool definition"""

    function: SEMOSSToolFunction
    type: Literal["function"]
    id: Optional[str] = None
    thought_signature: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("thought_signature", "thoughtSignature"),
    )  # Base64-encoded, Gemini thinking models only


class SEMOSSToolResponse(BaseModel):
    """Represents a tool response from the model"""

    id: str
    type: Literal["function"]
    name: str
    arguments: str


class SEMOSSToolExecution(BaseModel):
    """Represents a tool response output"""

    id: str = Field(validation_alias=AliasChoices("id", "toolCallId"))
    output: str
    # below are not actually used in the model calls but are useful to have in the tool response objects for better traceability and debugging
    tool_name: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("tool_name", "toolName")
    )
    tool_parameter_values: Optional[Dict[str, Any]] = Field(
        default=None,
        validation_alias=AliasChoices("tool_parameter_values", "toolParameterValues"),
    )
    tool_status: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("tool_status", "toolStatus")
    )


# =========== NEW MODELS FOR MESSAGE PARTS ===========
class SEMOSSMessagePartType(StringEnum):
    MEDIA = "MEDIA"
    TEXT = "TEXT"
    THINKING = "THINKING"
    TOOL_CALL = "TOOL_CALL"
    TOOL_RESULT = "TOOL_RESULT"
    SYSTEM = "SYSTEM"
    UNKNOWN = "UNKNOWN"


class SEMOSSMediaMessagePart(BaseModel):
    """Represents a media message content"""

    media_info: SEMOSSMediaContent = Field(
        validation_alias=AliasChoices("mediaInfo", "media_info")
    )
    type: Literal[SEMOSSMessagePartType.MEDIA] = SEMOSSMessagePartType.MEDIA


class SEMOSSSystemMessagePart(BaseModel):
    """Represents a system message content"""

    prompt: str
    type: Literal[SEMOSSMessagePartType.SYSTEM] = SEMOSSMessagePartType.SYSTEM


class SEMOSSTextMessagePart(BaseModel):
    """Represents a text message content"""

    text: str
    ui_text: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("uiText", "ui_text")
    )
    type: Literal[SEMOSSMessagePartType.TEXT] = SEMOSSMessagePartType.TEXT


class SEMOSSThinkingMessagePart(BaseModel):
    """Represents a thinking message content"""

    thinking: str
    type: Literal[SEMOSSMessagePartType.THINKING] = SEMOSSMessagePartType.THINKING


class SEMOSSToolCallMessagePart(BaseModel):
    """Represents a tool call message content"""

    tool_call: SEMOSSToolCall = Field(
        validation_alias=AliasChoices("toolCall", "tool_call")
    )
    type: Literal[SEMOSSMessagePartType.TOOL_CALL] = SEMOSSMessagePartType.TOOL_CALL


class SEMOSSToolResultMessagePart(BaseModel):
    """Represents a tool result message content"""

    tool_result: SEMOSSToolExecution = Field(
        validation_alias=AliasChoices("toolResult", "tool_result")
    )
    type: Literal[SEMOSSMessagePartType.TOOL_RESULT] = SEMOSSMessagePartType.TOOL_RESULT


class SEMOSSUnknownMessagePart(BaseModel):
    """Represents an unknown message part content"""

    data: Any
    type: Literal[SEMOSSMessagePartType.UNKNOWN] = SEMOSSMessagePartType.UNKNOWN


# =========== END NEW MODELS FOR MESSAGE PARTS ===========


# legacy message types for backwards compatibility
@deprecated(
    reason="Each part of a message now has a type to handle text w/ tool, text w/ media, etc",
    version="5.1.0",
)
class SEMOSSMessageType(StringEnum):
    INPUT_TEXT = "INPUT_TEXT"
    INPUT_MEDIA = "INPUT_MEDIA"
    INPUT_TOOL_EXEC = "INPUT_TOOL_EXEC"
    RESPONSE_TEXT = "RESPONSE_TEXT"
    RESPONSE_TOOL = "RESPONSE_TOOL"
    RESPONSE_MEDIA = "RESPONSE_MEDIA"


class SEMOSSMessage(BaseModel):
    # all of the below should be replaced with just parts
    type: SEMOSSMessageType
    content: Optional[str] = None
    media_content: Optional[List[SEMOSSMediaContent]] = None
    tool_calls: Optional[List[SEMOSSToolCall]] = Field(default_factory=list)
    tool_call_id: Optional[str] = None
    tool_responses: Optional[List[SEMOSSToolResponse]] = Field(default_factory=list)
    tokens: Optional[int] = 0
    param_map: Dict[str, Any] = Field(default_factory=dict)
    # parts
    # this will become mandatory once all the above are optional/removed
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
    io: Literal["INPUT", "OUTPUT"]

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
