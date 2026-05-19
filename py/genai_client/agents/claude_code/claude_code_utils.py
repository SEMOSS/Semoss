import json
import os
from datetime import datetime, timezone
from typing import Any, Optional

from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truncate(s: Optional[str], limit: int = 200) -> str:
    if not s:
        return ""
    return s if len(s) <= limit else s[:limit] + "..."


def _extract_description(tool_input: Any) -> str:
    """Mirrors ClaudeCodeTranscriptParser.extractDescription."""
    if not isinstance(tool_input, dict):
        return ""
    if "description" in tool_input:
        return str(tool_input["description"])
    if "prompt" in tool_input:
        return _truncate(str(tool_input["prompt"]))
    if "file_path" in tool_input:
        return str(tool_input["file_path"])
    if "command" in tool_input:
        return _truncate(str(tool_input["command"]))
    return ""


def _envelope(
    event_type: str,
    data: dict,
    *,
    uuid_val: Optional[str] = None,
    parent_uuid: Optional[str] = None,
    session_id: str = "",
) -> dict:
    """Matches ClaudeCodeTranscriptParser.toEvent shape so live events and
    history-replay events share the same envelope on the frontend."""
    return {
        "event": event_type,
        "uuid": uuid_val or "",
        "parentUuid": parent_uuid,
        "sessionId": session_id or "",
        "data": data,
    }


def make_user_prompt_event(
    prompt: str,
    prompt_id: str,
    session_id: str,
    uuid_val: Optional[str] = None,
) -> dict:
    return _envelope(
        "user_prompt",
        {
            "promptId": prompt_id,
            "text": prompt,
            "timestamp": _iso_now(),
        },
        uuid_val=uuid_val,
        session_id=session_id,
    )


def make_assistant_event(msg: AssistantMessage) -> Optional[dict]:
    """One envelope per AssistantMessage, bundling all TextBlock and
    ToolUseBlock content just like the Java parser does."""
    timestamp = _iso_now()
    model = msg.model or ""
    texts: list[dict] = []
    tool_invocations: list[dict] = []

    for block in msg.content:
        if isinstance(block, TextBlock):
            texts.append(
                {
                    "text": block.text,
                    "model": model,
                    "timestamp": timestamp,
                }
            )
        elif isinstance(block, ToolUseBlock):
            tool_input = block.input if isinstance(block.input, dict) else {}
            tool_invocations.append(
                {
                    "toolUseId": block.id,
                    "toolName": block.name,
                    "description": _extract_description(tool_input),
                    "subagentType": tool_input.get("subagent_type"),
                    "timestamp": timestamp,
                }
            )

    if not texts and not tool_invocations:
        return None

    data: dict = {"model": model}
    if texts:
        data["texts"] = texts
    if tool_invocations:
        data["toolInvocations"] = tool_invocations

    return _envelope(
        "assistant",
        data,
        uuid_val=msg.uuid,
        session_id=msg.session_id or "",
    )


def make_tool_result_event(msg: UserMessage) -> Optional[dict]:
    """Extracted from a UserMessage that carries a tool_use_result.
    Leaner than the history-replay version: the SDK object doesn't expose
    the full metadata block the JSONL file has, so durationMs/stats/filePath
    fall back to the values present in msg.tool_use_result when available."""
    content_blocks = msg.content if isinstance(msg.content, list) else []
    tool_result_block: Optional[ToolResultBlock] = None
    for block in content_blocks:
        if isinstance(block, ToolResultBlock):
            tool_result_block = block
            break

    if tool_result_block is None and not msg.tool_use_result:
        return None

    timestamp = _iso_now()
    tool_use_id = tool_result_block.tool_use_id if tool_result_block else None
    content_text: Optional[str] = None
    status = "completed"
    duration_ms = 0
    file_path: Optional[str] = None
    stats: Optional[dict] = None

    if tool_result_block is not None:
        if tool_result_block.is_error:
            status = "error"
        if isinstance(tool_result_block.content, str):
            content_text = tool_result_block.content
        elif isinstance(tool_result_block.content, list):
            parts: list[str] = []
            for item in tool_result_block.content:
                if isinstance(item, dict) and item.get("type") == "text":
                    text = item.get("text", "")
                    if text:
                        parts.append(text)
            content_text = "\n".join(parts) if parts else None

    tur = msg.tool_use_result
    if isinstance(tur, dict):
        status = str(tur.get("status", status))
        duration_ms = int(tur.get("totalDurationMs", 0) or 0)
        file_path = tur.get("filePath")
        if file_path is None:
            nested_file = tur.get("file")
            if isinstance(nested_file, dict):
                file_path = nested_file.get("filePath")
        tool_stats = tur.get("toolStats")
        if isinstance(tool_stats, dict):
            stats = {
                "readCount": tool_stats.get("readCount", 0),
                "searchCount": tool_stats.get("searchCount", 0),
                "bashCount": tool_stats.get("bashCount", 0),
                "editFileCount": tool_stats.get("editFileCount", 0),
                "linesAdded": tool_stats.get("linesAdded", 0),
                "linesRemoved": tool_stats.get("linesRemoved", 0),
            }
        if content_text is None:
            content_text = tur.get("text")
        if content_text is None:
            content_field = tur.get("content")
            if isinstance(content_field, list):
                parts = []
                for item in content_field:
                    if isinstance(item, dict) and item.get("type") == "text":
                        text = item.get("text", "")
                        if text:
                            parts.append(text)
                content_text = "\n".join(parts) if parts else None
        if content_text is None:
            nested_file = tur.get("file")
            if isinstance(nested_file, dict) and nested_file.get("content"):
                content_text = nested_file["content"]
        if content_text is None:
            try:
                content_text = json.dumps(tur)
            except Exception:
                content_text = str(tur)

    data: dict = {
        "toolUseId": tool_use_id,
        "status": status,
        "durationMs": duration_ms,
        "filePath": file_path,
        "content": content_text,
        "timestamp": timestamp,
    }
    if stats is not None:
        data["stats"] = stats

    return _envelope("tool_result", data, uuid_val=msg.uuid)


def make_result_event(msg: ResultMessage) -> dict:
    """Envelope for a ResultMessage — emitted once at the end of a session.
    Surfaces subtype, error state, turn count, cost, and any error strings."""
    data: dict = {
        "subtype": msg.subtype,
        "isError": msg.is_error,
        "numTurns": msg.num_turns,
        "stopReason": msg.stop_reason,
        "totalCostUsd": msg.total_cost_usd,
        "durationMs": msg.duration_ms,
        "errors": msg.errors or [],
        "timestamp": _iso_now(),
    }
    if msg.usage:
        data["usage"] = msg.usage
    return _envelope(
        "result",
        data,
        uuid_val=msg.uuid,
        session_id=msg.session_id or "",
    )
