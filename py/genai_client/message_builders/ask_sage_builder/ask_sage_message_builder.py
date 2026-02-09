from pathlib import Path
import base64, re, mimetypes, uuid, requests
from urllib.parse import urlparse
from typing import List, Optional, Union, Dict, Any, Tuple
from pydantic import BaseModel
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    ModelSettings,
)


class AskSageMessage(BaseModel):
    user: str
    message: str


class AskSageRequest(BaseModel):
    message: Union[str, List[Dict[str, str]]]
    model: Optional[str] = None
    persona: Optional[int] = None
    tools: Optional[List[Dict[str, Any]]] = None
    tool_choice: Optional[Union[Dict[str, Any], str, None]] = None
    reasoning_effort: Optional[str] = None
    system_prompt: Optional[str] = None
    dataset: Optional[str] = None
    limit_references: Optional[int] = None
    temperature: Optional[float] = None
    live: Optional[int] = None
    streaming: Optional[bool] = True


class AskSageMessageBuilder:
    def __init__(self, model_settings: ModelSettings):
        self.model_settings = model_settings

    def build_request(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Tuple[AskSageRequest, List[str] | None]:
        if not semoss_messages:
            raise ValueError("semoss_messages cannot be empty.")
        ask_sage_messages = []

        for i, msg in enumerate(semoss_messages):
            message_role: str = self.map_message_role(msg.type)
            msg_content = msg.content if msg.content else ""

            ask_sage_messages.append(
                AskSageMessage(user=message_role, message=msg_content)
            )

        last_message = semoss_messages[-1]
        return self.convert_param_map(
            last_message.param_map, ask_sage_messages, last_message
        )

    def map_message_role(self, semoss_role: SEMOSSMessageType) -> str:
        """Convert SEMOSS message type to AskSage role."""
        user_message_types = [
            SEMOSSMessageType.INPUT_TEXT,
            SEMOSSMessageType.INPUT_MEDIA,
            SEMOSSMessageType.INPUT_TOOL_EXEC,
        ]
        assistant_message_types = [
            SEMOSSMessageType.RESPONSE_TEXT,
            SEMOSSMessageType.RESPONSE_MEDIA,
            SEMOSSMessageType.RESPONSE_TOOL,
        ]

        if semoss_role in user_message_types:
            return "me"
        elif semoss_role in assistant_message_types:
            return "sage"
        else:
            raise ValueError(f"Unsupported SEMOSS message type: {semoss_role}")

    def _build_tool_choice(
        self, tool_choice: Dict[str, str]
    ) -> Union[Dict[str, Any], str, None]:
        """
        Build the tool choice as string and dictionary for OpenAI
        SEMOSS tool_type options [auto, required, forced, none]
        OpenAI type options [auto, required, forced, none]
        OpenAI types of any and tool are not available with extended thinking
        """
        tool_type = tool_choice.get("type", "auto").lower()
        tool_name = tool_choice.get("name", None)

        if tool_type == "auto":
            return "auto"
        elif tool_type == "required":
            return "required"
        elif tool_type == "forced" and tool_name:
            return {"type": "function", "function": {"name": tool_name}}
        elif tool_type == "none":
            return "none"
        else:
            return None

    def convert_mcp_to_openai_chat_completions_tools(
        self, mcp_tools: List[Dict]
    ) -> List[Dict]:
        """
        Convert MCP-formatted tools to OpenAI function calling format.
        Args:
            mcp_tools: List of tools in MCP format
        Returns:
            List of OpenAI tools for Chat Completions
        """
        openai_tools = []

        for tool in mcp_tools:
            openai_tool = {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": {
                    "type": tool["inputSchema"]["type"],
                    "properties": {},
                    "required": tool["inputSchema"].get("required", []),
                },
            }

            for prop_name, prop_def in tool["inputSchema"]["properties"].items():
                converted_prop = {k: v for k, v in prop_def.items() if k != "title"}

                if prop_def.get("type") == "array":
                    converted_prop["type"] = "object"
                    converted_prop.pop("items", None)

                openai_tool["parameters"]["properties"][prop_name] = converted_prop

            openai_tools.append({"type": "function", "function": openai_tool})

        return openai_tools

    def convert_param_map(
        self,
        param_map: Dict[str, Any],
        messages: List[AskSageMessage],
        last_semoss_msg: SEMOSSMessage,
    ) -> Tuple[AskSageRequest, Optional[List[str]]]:
        """Convert SEMOSS param_map to AskSage request parameters."""
        messages_dicts: List[Dict[str, Any]] = [
            msg.model_dump(exclude_none=True) for msg in messages
        ]
        tools: List[Dict[str, Any]] = []
        tools_param = param_map.get("tools", None)
        if tools_param:
            tools = self.convert_mcp_to_openai_chat_completions_tools(tools_param)

        tool_choice: Union[Dict[str, Any], str, None] = None
        tool_choice_param = (
            param_map.get("tool_choice", None) if len(tools) > 0 else None
        )
        if tool_choice_param:
            tool_choice = self._build_tool_choice(tool_choice_param)

        media_paths: Optional[List[str]] = None
        if getattr(last_semoss_msg, "media_content", None):
            media_paths = self._handle_input_media(last_semoss_msg)

        return (
            AskSageRequest(
                message=messages_dicts,
                model=self.model_settings.model_name,
                persona=param_map.get("persona", None),
                tools=tools,
                tool_choice=tool_choice,
                reasoning_effort=param_map.get("reasoning_effort", None),
                system_prompt=param_map.get("instructions", None),
                dataset=param_map.get("dataset", "none"),
                limit_references=param_map.get("limit_references", None),
                temperature=param_map.get("temperature", None),
                live=param_map.get("live", None),
                streaming=param_map.get("streaming", True),
            ),
            media_paths,
        )

    def _handle_input_media(self, message: SEMOSSMessage) -> List[str]:
        if not getattr(message, "media_content", None):
            raise ValueError("No media_content found on message.")

        temp_dir = Path(__file__).resolve().parent / "temp_image_dir"
        temp_dir.mkdir(parents=True, exist_ok=True)

        def _unique_name(ext: Optional[str]) -> Path:
            ext = (ext or "").lstrip(".")
            return temp_dir / f"{uuid.uuid4().hex}{('.' + ext) if ext else ''}"

        def _ext_from_mime(mime: Optional[str]) -> Optional[str]:
            if not mime:
                return None
            known = {
                "image/png": "png",
                "image/jpeg": "jpg",
                "image/jpg": "jpg",
                "image/webp": "webp",
                "image/gif": "gif",
                "image/bmp": "bmp",
                "image/tiff": "tiff",
                "image/svg+xml": "svg",
            }
            if mime in known:
                return known[mime]
            guess = mimetypes.guess_extension(mime)
            return guess.lstrip(".") if guess else None

        def _ext_from_filename(name: Optional[str]) -> Optional[str]:
            if not name:
                return None
            suf = Path(name).suffix
            return suf.lstrip(".") if suf else None

        def _to_lower_str(v) -> str:
            if v is None:
                return ""
            v = getattr(v, "value", v)
            return str(v).lower()

        saved_paths: List[str] = []
        errors: List[str] = []
        if message.media_content:
            for idx, mc in enumerate(message.media_content):
                try:
                    mtype = _to_lower_str(getattr(mc, "type", None))

                    if mtype == "base64":
                        data = getattr(mc, "data", None)
                        if not data or not isinstance(data, str):
                            raise ValueError(
                                "Expected base64 string in media_content[i].data"
                            )

                        mime = getattr(mc, "mime_type", None)
                        ext = _ext_from_mime(mime) or _ext_from_filename(
                            getattr(mc, "file_name", None)
                        )

                        b64_payload = data
                        if not ext:
                            m = re.match(
                                r"^data:(?P<mime>[^;]+);base64,(?P<payload>.+)$",
                                data,
                                re.IGNORECASE,
                            )
                            if m:
                                mime = m.group("mime")
                                b64_payload = m.group("payload")
                                ext = _ext_from_mime(mime)
                        else:
                            m = re.match(
                                r"^data:[^;]+;base64,(?P<payload>.+)$",
                                data,
                                re.IGNORECASE,
                            )
                            if m:
                                b64_payload = m.group("payload")

                        raw = base64.b64decode(b64_payload, validate=True)
                        out_path = _unique_name(ext or "png")
                        out_path.write_bytes(raw)
                        saved_paths.append(str(out_path))
                        continue

                    url = getattr(mc, "url", None)
                    if not url or not isinstance(url, str):
                        raise ValueError(
                            "Expected URL in media_content[i].url for non-base64 media."
                        )

                    resp = requests.get(url, timeout=20)
                    resp.raise_for_status()

                    content_type = resp.headers.get("Content-Type", "")
                    content_type = content_type.split(";")[0].strip() or None
                    ext = (
                        _ext_from_mime(content_type)
                        or _ext_from_mime(getattr(mc, "mime_type", None))
                        or _ext_from_filename(getattr(mc, "file_name", None))
                    )

                    if not ext:
                        path_name = Path(urlparse(url).path).name
                        ext = _ext_from_filename(path_name)

                    out_path = _unique_name(ext or "png")
                    out_path.write_bytes(resp.content)
                    saved_paths.append(str(out_path))

                except Exception as e:
                    errors.append(f"media_content[{idx}]: {e}")

        if not saved_paths and errors:
            raise ValueError("Failed to process all media items:\n" + "\n".join(errors))

        return saved_paths
