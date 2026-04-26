"""GitHub Copilot Python sidecar — mirrors ClaudeCodeClient.

Drives the github-copilot-sdk on behalf of a Java GitHubCopilotPyManager.
Java sends one init script and then one query script per RunAgent invocation;
streaming events are pushed back through `smss_thread_local.get_smss_stream()`
in the exact envelope shape GitHubCopilotPixelJobStreamer.java produces, so
the FE contract is identical to the in-Java path.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, field_validator

from copilot import CopilotClient, SubprocessConfig
from copilot.generated.session_events import (
    AssistantIntentData,
    AssistantMessageData,
    AssistantMessageDeltaData,
    SessionErrorData,
    SessionIdleData,
    ToolExecutionCompleteData,
    ToolExecutionPartialResultData,
    ToolExecutionProgressData,
    ToolExecutionStartData,
)
from copilot.session import PermissionHandler

from smss_thread_local import get_smss_stream
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil
from ...utils import string_to_bool
from .github_copilot_utils import (
    build_assistant_text_envelope,
    build_session_error_envelope,
    build_tool_invocation_envelope,
    build_tool_result_envelope,
    build_user_prompt_envelope,
    extract_description,
    extract_tool_result_content,
    is_report_intent_tool,
    iso_now,
    normalize_tool_request,
    truncate,
)


class _MCP(BaseModel):
    url: str
    name: str


class _GHCInitArgs(BaseModel):
    model: str
    cwd_path: str
    room_id: str
    access_key: str
    secret_key: str
    base_url: str
    permission_mode: Optional[str] = "default"
    allowed_tools: Optional[list[str]] = None
    mcps: Optional[list[_MCP]] = None
    insight_id: Optional[str] = None
    room_folder_path: Optional[str] = None
    cli_path: Optional[str] = None
    session_exists: bool = False

    @field_validator("session_exists", mode="before")
    @classmethod
    def _coerce_session_exists(cls, v: Any) -> bool:
        return string_to_bool(v)


class GitHubCopilotClient:
    """Java-callable wrapper around the github-copilot-sdk.

    One instance is held per python sidecar process, configured at init time
    by the Java manager. Each `query_copilot()` call drives one user turn and
    blocks until the session goes idle.
    """

    def __init__(self, **kwargs: Any) -> None:
        self.cfg = _GHCInitArgs(**kwargs)
        self._mcp_servers = self._build_mcp_servers()
        self._provider = self._build_provider()
        self._available_tools = self._build_available_tools()

        bearer = f"{self.cfg.access_key}:{self.cfg.secret_key}:room-{self.cfg.room_id}"
        subprocess_cfg = SubprocessConfig(
            cli_path=self.cfg.cli_path or os.environ.get("COPILOT_CLI_PATH"),
            cwd=self.cfg.cwd_path,
            github_token=bearer,
            use_logged_in_user=False,
            log_level="info",
        )
        self._client = CopilotClient(subprocess_cfg)
        self._client_started = False

    # Public sync entry point — Java calls this via PyTranslator.runDirectPy.
    def query_copilot(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        **_kwargs: Any,
    ) -> str:
        return asyncio.run(self._query_async(prompt, system_prompt))

    async def _query_async(
        self,
        prompt: str,
        system_prompt: Optional[str],
    ) -> str:
        if not self._client_started:
            await self._client.start()
            self._client_started = True

        smss_stream = get_smss_stream()
        if smss_stream:
            smss_stream(
                build_user_prompt_envelope(prompt, str(uuid4()), self.cfg.room_id),
                stream_type="content",
            )

        idle = asyncio.Event()
        final_text_parts: list[str] = []
        suppressed_tool_call_ids: set[str] = set()
        error_holder: dict[str, Any] = {}

        def on_event(ev: Any) -> None:
            try:
                self._dispatch_event(
                    ev,
                    smss_stream=smss_stream,
                    final_text_parts=final_text_parts,
                    suppressed_tool_call_ids=suppressed_tool_call_ids,
                    idle=idle,
                    error_holder=error_holder,
                )
            except Exception as exc:  # never let a handler raise out of the SDK loop
                if smss_stream:
                    smss_stream(
                        build_session_error_envelope(
                            error_type="event_handler",
                            message=f"event handler crashed: {exc!r}",
                            status_code=None,
                            timestamp=iso_now(),
                            session_id=self.cfg.room_id,
                        ),
                        stream_type="content",
                    )

        common_kwargs: dict[str, Any] = {
            "model": self.cfg.model,
            "on_permission_request": PermissionHandler.approve_all,
            "working_directory": self.cfg.cwd_path,
            "streaming": True,
            "on_event": on_event,
        }
        if self.cfg.room_folder_path:
            common_kwargs["config_dir"] = self.cfg.room_folder_path
        if self._provider is not None:
            common_kwargs["provider"] = self._provider
        if self._mcp_servers:
            common_kwargs["mcp_servers"] = self._mcp_servers
        if self._available_tools is not None:
            common_kwargs["available_tools"] = self._available_tools
        if system_prompt:
            common_kwargs["system_message"] = {
                "mode": "append",
                "content": system_prompt,
            }

        if self.cfg.session_exists:
            session = await self._client.resume_session(
                self.cfg.room_id, **common_kwargs
            )
        else:
            session = await self._client.create_session(
                session_id=self.cfg.room_id, **common_kwargs
            )
            self._mark_session_created()

        try:
            await session.send(prompt)
            await idle.wait()
        finally:
            try:
                await session.disconnect()
            except Exception:
                pass

        if error_holder.get("raised"):
            # Already streamed the session_error envelope; surface as a string for
            # the Java caller so the harness gets a non-empty return value.
            err = error_holder["raised"]
            return f"[github_copilot_py error] {err}"

        # Session is now persisted; future calls should resume.
        self.cfg.session_exists = True

        if smss_stream:
            smss_stream(
                StreamUtil.create_finish_reason_chunk("stop"),
                stream_type="content",
                interim=False,
            )

        return "".join(final_text_parts)

    def _dispatch_event(
        self,
        ev: Any,
        *,
        smss_stream: Any,
        final_text_parts: list[str],
        suppressed_tool_call_ids: set[str],
        idle: asyncio.Event,
        error_holder: dict[str, Any],
    ) -> None:
        data = getattr(ev, "data", None)
        timestamp = iso_now()
        session_id = self.cfg.room_id
        model = self.cfg.model

        if isinstance(data, AssistantMessageDeltaData):
            envelope = build_assistant_text_envelope(
                event_id=getattr(data, "message_id", "") or "",
                text=getattr(data, "delta_content", "") or "",
                is_partial=True,
                timestamp=timestamp,
                model=model,
                session_id=session_id,
                parent_tool_use_id=getattr(data, "parent_tool_call_id", None),
            )
            if envelope and smss_stream:
                smss_stream(envelope, stream_type="content")
            return

        if isinstance(data, AssistantMessageData):
            content = getattr(data, "content", "") or ""
            if content:
                final_text_parts.append(content)
            envelope = build_assistant_text_envelope(
                event_id=getattr(data, "message_id", "") or "",
                text=content,
                is_partial=False,
                timestamp=timestamp,
                model=model,
                session_id=session_id,
                parent_tool_use_id=getattr(data, "parent_tool_call_id", None),
            )
            if envelope and smss_stream:
                smss_stream(envelope, stream_type="content")

            tool_requests = getattr(data, "tool_requests", None) or []
            for req in tool_requests:
                # SDK gives dataclasses; coerce to dict via to_dict if available.
                req_dict = req.to_dict() if hasattr(req, "to_dict") else (
                    req if isinstance(req, dict) else {}
                )
                tool_name = (
                    req_dict.get("name")
                    or req_dict.get("toolName")
                )
                tool_call_id = (
                    req_dict.get("toolCallId")
                    or req_dict.get("id")
                )
                if is_report_intent_tool(tool_name) and tool_call_id:
                    suppressed_tool_call_ids.add(tool_call_id)
                    continue
                normalized = normalize_tool_request(req_dict, timestamp=timestamp)
                if normalized and smss_stream:
                    inv_envelope = build_tool_invocation_envelope(
                        tool_call_id=normalized["toolUseId"],
                        tool_name=normalized["toolName"],
                        description=normalized.get("description"),
                        timestamp=timestamp,
                        model=model,
                        session_id=session_id,
                    )
                    if inv_envelope:
                        smss_stream(inv_envelope, stream_type="content")
            return

        if isinstance(data, AssistantIntentData):
            intent = getattr(data, "intent", "") or ""
            envelope = build_assistant_text_envelope(
                event_id=str(uuid4()),
                text=intent,
                is_partial=False,
                timestamp=timestamp,
                model=model,
                session_id=session_id,
                display="intent",
            )
            if envelope and smss_stream:
                smss_stream(envelope, stream_type="content")
            return

        if isinstance(data, ToolExecutionStartData):
            tool_call_id = getattr(data, "tool_call_id", "") or ""
            tool_name = getattr(data, "tool_name", "") or ""
            if is_report_intent_tool(tool_name):
                suppressed_tool_call_ids.add(tool_call_id)
                return
            arguments = getattr(data, "arguments", None)
            envelope = build_tool_invocation_envelope(
                tool_call_id=tool_call_id,
                tool_name=tool_name,
                description=truncate(extract_description(arguments)) if arguments is not None else None,
                timestamp=timestamp,
                model=model,
                session_id=session_id,
            )
            if envelope and smss_stream:
                smss_stream(envelope, stream_type="content")
            return

        if isinstance(data, (ToolExecutionProgressData, ToolExecutionPartialResultData)):
            tool_call_id = getattr(data, "tool_call_id", "") or ""
            if tool_call_id in suppressed_tool_call_ids:
                return
            content = (
                getattr(data, "progress_message", None)
                or getattr(data, "partial_output", None)
            )
            envelope = build_tool_result_envelope(
                tool_call_id=tool_call_id,
                status="running",
                content=str(content) if content else None,
                is_partial=True,
                timestamp=timestamp,
                session_id=session_id,
            )
            if envelope and smss_stream:
                smss_stream(envelope, stream_type="content")
            return

        if isinstance(data, ToolExecutionCompleteData):
            tool_call_id = getattr(data, "tool_call_id", "") or ""
            if tool_call_id in suppressed_tool_call_ids:
                suppressed_tool_call_ids.discard(tool_call_id)
                return
            success = bool(getattr(data, "success", False))
            result = getattr(data, "result", None)
            error = getattr(data, "error", None)
            result_dict = result.to_dict() if hasattr(result, "to_dict") else result
            error_dict = error.to_dict() if hasattr(error, "to_dict") else error
            content = extract_tool_result_content(result_dict, error_dict)
            if not content and success:
                content = "(tool completed with no output)"
            envelope = build_tool_result_envelope(
                tool_call_id=tool_call_id,
                status="completed" if success else "error",
                content=content,
                is_partial=False,
                timestamp=timestamp,
                session_id=session_id,
            )
            if envelope and smss_stream:
                smss_stream(envelope, stream_type="content")
            return

        if isinstance(data, SessionErrorData):
            message = getattr(data, "message", None)
            error_type = getattr(data, "error_type", None)
            status_code = getattr(data, "status_code", None)
            error_holder["raised"] = message or "session error"
            if smss_stream:
                smss_stream(
                    build_session_error_envelope(
                        error_type=str(error_type) if error_type is not None else None,
                        message=str(message) if message is not None else None,
                        status_code=status_code,
                        timestamp=timestamp,
                        session_id=session_id,
                    ),
                    stream_type="content",
                )
            idle.set()
            return

        if isinstance(data, SessionIdleData):
            idle.set()
            return

    # Init-time builders.

    def _build_mcp_servers(self) -> dict[str, dict[str, Any]]:
        servers: dict[str, dict[str, Any]] = {}
        bearer = f"Bearer {self.cfg.access_key}:{self.cfg.secret_key}"
        for mcp in self.cfg.mcps or []:
            safe_name = mcp.name.replace(" ", "_").lower()
            servers[safe_name] = {
                "type": "http",
                "url": mcp.url,
                "headers": {"Authorization": bearer},
            }
        return servers

    def _build_provider(self) -> Optional[dict[str, Any]]:
        if not self.cfg.base_url:
            return None
        return {
            "type": "openai",
            "base_url": self.cfg.base_url,
            "api_key": f"room-{self.cfg.room_id}",
            "bearer_token": f"{self.cfg.access_key}:{self.cfg.secret_key}",
        }

    def _mark_session_created(self) -> None:
        """Drop a sentinel under the room folder so the next Java RunAgent call
        knows to call resume_session instead of create_session. Mirrors the
        ClaudeCodeManager.agentHistoryExists check pattern."""
        if not self.cfg.room_folder_path:
            return
        try:
            sentinel_dir = os.path.join(
                self.cfg.room_folder_path, "copilot-session"
            )
            os.makedirs(sentinel_dir, exist_ok=True)
            sentinel_file = os.path.join(sentinel_dir, ".created")
            with open(sentinel_file, "a", encoding="utf-8"):
                pass
        except OSError:
            pass

    def _build_available_tools(self) -> Optional[list[str]]:
        if not self.cfg.allowed_tools:
            return None
        # Empty list from Java means "no allow-list set"; keep SDK default.
        explicit = [t for t in self.cfg.allowed_tools if t and t != "*"]
        # If caller passed only "*" or nothing meaningful, defer to SDK default.
        return explicit or None
