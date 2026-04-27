"""Event mapping for the GitHub Copilot Python sidecar.

Produces the same envelope shape that GitHubCopilotPixelJobStreamer.java emits
for the in-Java SDK path, so the FE event contract is identical regardless of
which harness ran the turn.
"""

from datetime import datetime, timezone
from typing import Any, Optional

DESCRIPTION_LIMIT = 200
REPORT_INTENT_TOOL = "report_intent"

# Argument keys probed when extracting a short description from a tool_use input.
# Ordering mirrors GitHubCopilotPixelJobStreamer.extractDescription.
_DESCRIPTION_KEYS = (
    "description",
    "prompt",
    "file_path",
    "filePath",
    "path",
    "command",
    "pattern",
    "glob",
    "query",
    "url",
    "intent",
)


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_blank(s: Optional[str]) -> bool:
    return s is None or not str(s).strip()


def truncate(value: Optional[str]) -> Optional[str]:
    if is_blank(value):
        return None
    s = str(value)
    return s if len(s) <= DESCRIPTION_LIMIT else s[:DESCRIPTION_LIMIT] + "..."


def is_report_intent_tool(tool_name: Optional[str]) -> bool:
    return bool(tool_name) and tool_name.strip().lower() == REPORT_INTENT_TOOL


def extract_description(arguments: Any) -> Optional[str]:
    if isinstance(arguments, dict):
        for key in _DESCRIPTION_KEYS:
            value = arguments.get(key)
            if value is not None and not is_blank(str(value)):
                return truncate(str(value))
        return None
    if arguments is None:
        return None
    return truncate(str(arguments))


def envelope(event: str, uuid: str, session_id: str, data: dict) -> dict:
    """Wrap a payload in the {stream_type, data:{event, uuid, sessionId, data}} shape
    Java's GitHubCopilotPixelJobStreamer.pushEnvelope produces."""
    return {
        "stream_type": "content",
        "data": {
            "event": event,
            "uuid": uuid or "",
            "sessionId": session_id or "",
            "data": data,
        },
    }


# Builders for the three event types the FE consumes.

def build_assistant_text_envelope(
    *,
    event_id: str,
    text: str,
    is_partial: bool,
    timestamp: str,
    model: str,
    session_id: str,
    parent_tool_use_id: Optional[str] = None,
    display: Optional[str] = None,
) -> Optional[dict]:
    if is_blank(event_id) or is_blank(text):
        return None
    text_payload = {
        "eventId": event_id,
        "text": text,
        "timestamp": timestamp or "",
        "isPartial": is_partial,
    }
    if display:
        text_payload["display"] = display
    if model:
        text_payload["model"] = model
    if parent_tool_use_id:
        text_payload["parentToolUseId"] = parent_tool_use_id

    data: dict = {}
    if model:
        data["model"] = model
    if timestamp:
        data["timestamp"] = timestamp
    data["texts"] = [text_payload]
    return envelope("assistant", event_id, session_id, data)


def build_tool_invocation_envelope(
    *,
    tool_call_id: str,
    tool_name: str,
    description: Optional[str],
    timestamp: str,
    model: str,
    session_id: str,
    parent_tool_use_id: Optional[str] = None,
) -> Optional[dict]:
    if is_blank(tool_call_id) or is_blank(tool_name):
        return None
    invocation: dict = {
        "eventId": tool_call_id,
        "toolUseId": tool_call_id,
        "toolName": tool_name,
        "timestamp": timestamp or "",
    }
    if description:
        invocation["description"] = description
    if parent_tool_use_id:
        invocation["parentToolUseId"] = parent_tool_use_id

    data: dict = {}
    if model:
        data["model"] = model
    if timestamp:
        data["timestamp"] = timestamp
    data["toolInvocations"] = [invocation]
    return envelope("assistant", tool_call_id, session_id, data)


def build_tool_result_envelope(
    *,
    tool_call_id: str,
    status: str,
    content: Optional[str],
    is_partial: bool,
    timestamp: str,
    session_id: str,
    duration_ms: int = 0,
) -> Optional[dict]:
    if is_blank(tool_call_id):
        return None
    data: dict = {
        "kind": "tool-result",
        "eventId": tool_call_id,
        "toolUseId": tool_call_id,
        "status": status,
        "isPartial": is_partial,
        "durationMs": duration_ms,
        "timestamp": timestamp or "",
    }
    if content:
        data["content"] = content
    return envelope("tool_result", tool_call_id, session_id, data)


def build_session_error_envelope(
    *,
    error_type: Optional[str],
    message: Optional[str],
    status_code: Any,
    timestamp: str,
    session_id: str,
) -> dict:
    data: dict = {
        "errorType": error_type or "",
        "message": message or "GitHub Copilot session error",
        "timestamp": timestamp or "",
    }
    if status_code is not None:
        data["statusCode"] = status_code
    return envelope("session_error", "", session_id, data)


def build_user_prompt_envelope(prompt: str, prompt_id: str, session_id: str) -> dict:
    return envelope(
        "user_prompt",
        prompt_id,
        session_id,
        {
            "promptId": prompt_id,
            "text": prompt,
            "timestamp": iso_now(),
        },
    )


def normalize_tool_request(req: Any, *, timestamp: str) -> Optional[dict]:
    """Mirror Java's normalizeToolRequests: strip report_intent, surface
    intentionSummary or arguments-derived description."""
    if not isinstance(req, dict):
        return None
    tool_call_id = req.get("toolCallId") or req.get("tool_call_id") or req.get("id")
    tool_name = req.get("name") or req.get("toolName") or req.get("tool_name")
    if is_blank(tool_call_id) or is_blank(tool_name):
        return None
    if is_report_intent_tool(tool_name):
        return None
    description = (
        req.get("intentionSummary")
        or req.get("intention_summary")
        or extract_description(req.get("arguments"))
    )
    invocation: dict = {
        "eventId": tool_call_id,
        "toolUseId": tool_call_id,
        "toolName": tool_name,
        "timestamp": timestamp or "",
    }
    desc = truncate(description) if description else None
    if desc:
        invocation["description"] = desc
    return invocation


def extract_tool_result_content(result: Any, error: Any) -> Optional[str]:
    """Mirror Java's extractToolResultContent: error.message > result.content/detailedContent."""
    if isinstance(error, dict):
        msg = error.get("message")
        if not is_blank(msg):
            return str(msg)
    if isinstance(result, dict):
        for key in ("content", "detailedContent"):
            value = result.get(key)
            if value is None:
                continue
            if isinstance(value, str) and not is_blank(value):
                return value
            # Some SDK shapes return content as list-of-blocks
            if isinstance(value, list):
                parts = []
                for item in value:
                    if isinstance(item, dict):
                        text = item.get("text") or item.get("content")
                        if text:
                            parts.append(str(text))
                    elif item is not None:
                        parts.append(str(item))
                if parts:
                    return "\n".join(parts)
    return None
