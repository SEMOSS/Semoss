from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, field_validator

from ...utils import StringEnum


class AnthropicRoles(StringEnum):
    USER = "user"
    ASSISTANT = "assistant"


class AnthropicMediaType(StringEnum):
    URL = "url"
    BASE64 = "base64"


class AnthropicMediaSourceBase64(BaseModel):
    type: AnthropicMediaType = AnthropicMediaType.BASE64
    media_type: str
    data: str


class AnthropicURLMediaSource(BaseModel):
    type: AnthropicMediaType = AnthropicMediaType.URL
    url: str


class AnthropicImageContentPart(BaseModel):
    type: str = "image"
    source: AnthropicMediaSourceBase64


class AnthropicDocumentContentPart(BaseModel):
    type: str = "document"
    source: AnthropicMediaSourceBase64


# FOR HISTORY
class AnthropicToolUseContentPart(BaseModel):
    type: str = "tool_use"
    id: str
    name: str
    input: Dict[str, Any]

    @field_validator("input", mode="before")
    @classmethod
    def convert_empty_string_to_dict(cls, v):
        """Convert empty string to empty dict for tools with no arguments"""
        if v == "" or v is None:
            return {}
        if isinstance(v, str):
            # If it's a non-empty string, try to parse it as JSON
            import json

            try:
                return json.loads(v)
            except json.JSONDecodeError:
                # If it fails, return empty dict
                return {}
        return v


# FOR HISTORY
class AnthropicToolResultContentPart(BaseModel):
    type: str = "tool_result"
    tool_use_id: str
    content: Union[
        str,
        List[
            Union[
                "AnthropicTextContentPart",
                AnthropicImageContentPart,
                AnthropicDocumentContentPart,
            ]
        ],
    ]


class AnthropicServerToolResultContentPart(BaseModel):
    type: str
    tool_use_id: str
    content: Union[List[Dict[str, Any]], Dict[str, Any], str]


# FOR CALLING TOOLS
class AnthropicToolCall(BaseModel):
    name: str
    description: Optional[str] = None
    input_schema: Optional[Dict[str, Any]] = None


class AnthropicThinkingContentPart(BaseModel):
    type: str = "thinking"
    thinking: str
    signature: Optional[str] = None


class AnthropicCacheTTL(StringEnum):
    FIVE_MINUTES = "5m"
    ONE_HOUR = "1h"


class AnthropicCacheControl(BaseModel):
    type: str = "ephemeral"
    # Omitted entirely for the 5m default so the request matches what Anthropic
    # expects when no explicit lifetime is requested.
    ttl: Optional[AnthropicCacheTTL] = None


class AnthropicTextContentPart(BaseModel):
    type: str = "text"
    text: str
    cache_control: Optional[AnthropicCacheControl] = None


AnthropicToolResultContentPart.model_rebuild()


class AnthropicMessage(BaseModel):
    role: AnthropicRoles
    content: Union[
        str,
        List[
            Union[
                AnthropicTextContentPart,
                AnthropicImageContentPart,
                AnthropicToolUseContentPart,
                AnthropicServerToolResultContentPart,
                AnthropicToolResultContentPart,
                AnthropicThinkingContentPart,
                AnthropicDocumentContentPart,
            ]
        ],
    ]


class AnthropicRequestConfig(BaseModel):
    model: str
    messages: List[Dict[str, Any]]
    betas: Optional[List[str]] = None
    system: Optional[Union[str, List[Dict[str, Any]]]] = None
    cache_control: Optional[Dict[str, Any]] = None
    tools: Optional[List[Dict]] = None
    tool_choice: Optional[Dict[str, str]] = None
    max_tokens: Optional[int] = None
    extra_body: Optional[Dict[str, Any]] = None
    container: Optional[str] = None
    stop_sequences: Optional[List[str]] = None
    thinking: Optional[Dict[str, Any]] = None
    output_config: Optional[Dict[str, Any]] = None


class AnthropicMessageBuilderResponse(BaseModel):
    request_config: AnthropicRequestConfig
    streaming: bool
    has_structured_input: bool
    thinking: bool = False
    thinking_budget: Optional[int] = None
