import base64
from typing import Dict, List, Optional, Union, Any, Literal
from pydantic import AliasChoices, BaseModel, Field, field_validator
import urllib.request
from ...utils import StringEnum, deprecated
import json


class SEMOSSMediaInputType(StringEnum):
    """Represents media input types"""

    URL = "url"
    BASE64 = "base64"


MediaCategory = Literal["image", "video", "audio", "document", "unknown"]


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

    _EXTENSION_CATEGORIES: Dict[str, MediaCategory] = {
        # images
        "png": "image",
        "jpg": "image",
        "jpeg": "image",
        "gif": "image",
        "webp": "image",
        "bmp": "image",
        "svg": "image",
        "tiff": "image",
        "tif": "image",
        "heic": "image",
        "heif": "image",
        "ico": "image",
        "avif": "image",
        # videos
        "mp4": "video",
        "mov": "video",
        "avi": "video",
        "mkv": "video",
        "webm": "video",
        "wmv": "video",
        "flv": "video",
        "m4v": "video",
        # audio
        "mp3": "audio",
        "wav": "audio",
        "ogg": "audio",
        "flac": "audio",
        "m4a": "audio",
        "aac": "audio",
        "opus": "audio",
        "wma": "audio",
        # documents
        "pdf": "document",
        "doc": "document",
        "docx": "document",
        "xls": "document",
        "xlsx": "document",
        "ppt": "document",
        "pptx": "document",
        "txt": "document",
        "csv": "document",
        "rtf": "document",
        "odt": "document",
        "md": "document",
    }

    _MIME_PREFIX_CATEGORIES: Dict[str, MediaCategory] = {
        "image": "image",
        "video": "video",
        "audio": "audio",
    }

    def get_media_category(self) -> MediaCategory:
        """Determine the broad category of this media content.

        Inspects mime_type first (most reliable), then falls back to
        format, then to the file_name extension, then the URL extension.
        """
        if self.mime_type:
            prefix = self.mime_type.lower().split("/", 1)[0]
            if prefix in self._MIME_PREFIX_CATEGORIES:
                return self._MIME_PREFIX_CATEGORIES[prefix]
            if prefix == "application":
                subtype = self.mime_type.lower().split("/", 1)[-1]
                if any(
                    d in subtype
                    for d in (
                        "pdf",
                        "word",
                        "excel",
                        "sheet",
                        "presentation",
                        "document",
                    )
                ):
                    return "document"

        if self.format:
            ext = self.format.lower().lstrip(".")
            if ext in self._EXTENSION_CATEGORIES:
                return self._EXTENSION_CATEGORIES[ext]

        if self.file_name and "." in self.file_name:
            ext = self.file_name.rsplit(".", 1)[-1].lower()
            if ext in self._EXTENSION_CATEGORIES:
                return self._EXTENSION_CATEGORIES[ext]

        if self.url:
            path = self.url.split("?", 1)[0].split("#", 1)[0]
            if "." in path:
                ext = path.rsplit(".", 1)[-1].lower()
                if ext in self._EXTENSION_CATEGORIES:
                    return self._EXTENSION_CATEGORIES[ext]

        return "unknown"

    def is_image(self) -> bool:
        """Check if this media content represents an image."""
        return self.get_media_category() == "image"

    def is_video(self) -> bool:
        return self.get_media_category() == "video"

    def is_audio(self) -> bool:
        return self.get_media_category() == "audio"

    def is_document(self) -> bool:
        return self.get_media_category() == "document"

    _DEFAULT_FETCH_TIMEOUT_SECONDS: float = 30.0
    _DEFAULT_MAX_FETCH_BYTES: int = 50 * 1024 * 1024  # 50 MB

    def get_bytes(
        self,
        *,
        timeout: float = _DEFAULT_FETCH_TIMEOUT_SECONDS,
        max_bytes: int = _DEFAULT_MAX_FETCH_BYTES,
    ) -> bytes:
        """Return the raw bytes of this media, regardless of source type.

        For BASE64 content, decodes `data`. For URL content, fetches the
        URL and returns the response body. Populates `mime_type` from the
        response Content-Type if it wasn't already set.

        Raises:
            ValueError: if no data/URL is available, or content exceeds max_bytes.
            urllib.error.URLError: on network failures.
        """
        if self.type == SEMOSSMediaInputType.BASE64:
            if not self.data:
                raise ValueError("BASE64 media has no `data` field set")
            return base64.b64decode(self.data)

        if self.type == SEMOSSMediaInputType.URL:
            target = self.url or self.data
            if not target:
                raise ValueError("URL media has no `url` (or `data`) field set")

            req = urllib.request.Request(target, headers={"User-Agent": "SEMOSS/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                length_header = resp.headers.get("Content-Length")
                if length_header and int(length_header) > max_bytes:
                    raise ValueError(
                        f"Remote content length {length_header} exceeds max {max_bytes}"
                    )

                data = resp.read(max_bytes + 1)
                if len(data) > max_bytes:
                    raise ValueError(f"Remote content exceeds max {max_bytes} bytes")

                if not self.mime_type:
                    ct = resp.headers.get("Content-Type")
                    if ct:
                        self.mime_type = ct.split(";", 1)[0].strip()

                return data

        raise ValueError(f"Unsupported media input type: {self.type}")

    def to_base64(
        self,
        *,
        timeout: float = _DEFAULT_FETCH_TIMEOUT_SECONDS,
        max_bytes: int = _DEFAULT_MAX_FETCH_BYTES,
        in_place: bool = False,
    ) -> "SEMOSSMediaContent":
        """Return a SEMOSSMediaContent whose type is BASE64.

        If this content is already BASE64, returns self (or a copy) unchanged.
        If it's a URL, fetches the bytes and returns a new instance with
        the data encoded as base64. Set `in_place=True` to mutate this instance.
        """
        if self.type == SEMOSSMediaInputType.BASE64:
            return self

        raw = self.get_bytes(timeout=timeout, max_bytes=max_bytes)
        encoded = base64.b64encode(raw).decode("ascii")

        if in_place:
            self.type = SEMOSSMediaInputType.BASE64
            self.data = encoded
            return self

        return SEMOSSMediaContent(
            type=SEMOSSMediaInputType.BASE64,
            data=encoded,
            format=self.format,
            mime_type=self.mime_type,
            file_name=self.file_name,
            url=self.url,
        )


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

        if isinstance(v, dict):
            return v

        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, dict):
                    return parsed
                if isinstance(parsed, str):
                    parsed2 = json.loads(parsed)
                    if isinstance(parsed2, dict):
                        return parsed2
                raise ValueError("Parsed JSON is not a dictionary")
            except (json.JSONDecodeError, ValueError):
                return v

        return v


class SEMOSSToolCall(BaseModel):
    """Wrapper around the tool definition"""

    function: SEMOSSToolFunction
    type: Literal["function"]
    id: Optional[str] = None
    # Base64-encoded, Gemini thinking models only
    thought_signature: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("thought_signature", "thoughtSignature"),
    )
    # True when the call targeted a provider-side built-in tool (e.g. web_search).
    # Mirror of the same flag on SEMOSSToolExecution.
    server_tool: Optional[bool] = Field(
        default=None, validation_alias=AliasChoices("server_tool", "serverTool")
    )


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
    # True when the result came from a provider-side built-in tool (e.g. web_search).
    # these results must replay inside the assistant turn as a provider-specific result block,
    # not as a generic client `tool_result`.
    server_tool: Optional[bool] = Field(
        default=None, validation_alias=AliasChoices("server_tool", "serverTool")
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
