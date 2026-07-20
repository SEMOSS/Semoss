"""GitHub Copilot Python sidecar - mirrors ClaudeCodeClient.

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
from copilot.generated.rpc import PermissionsSetApproveAllRequest
from copilot.generated import session_events as _gh_session_events
from copilot.generated.session_events import (
    AssistantIntentData,
    AssistantMessageData,
    AssistantMessageDeltaData,
    PermissionCompletedKind,
    PermissionCompletedResult,
    SessionErrorData,
    SessionIdleData,
    ToolExecutionCompleteData,
    ToolExecutionPartialResultData,
    ToolExecutionProgressData,
    ToolExecutionStartData,
)
from copilot.session import PermissionHandler


# ---------------------------------------------------------------------------
# Runtime patch for github-copilot-sdk 0.3.0 wire-format bug.
#
# The CLI emits `permission.completed` events with `result.kind="approve-once"`
# (a value from PermissionDecisionKind), but the SDK parses it against
# PermissionCompletedKind, which only accepts "approved", "approved-for-session",
# etc. Result: every tool approval crashes asyncio with
#   ValueError: 'approve-once' is not a valid PermissionCompletedKind
#
# We DO NOT edit the SDK source. Instead, replace
# PermissionCompletedResult.from_dict so unknown kinds fall back to APPROVED
# (the closest semantic match) instead of raising. This keeps the asyncio
# notification handler from crashing and lets tool-result events propagate
# through to our streaming envelopes.
# ---------------------------------------------------------------------------
def _patched_permission_completed_result_from_dict(obj: Any) -> "PermissionCompletedResult":
    assert isinstance(obj, dict)
    raw_kind = obj.get("kind")
    try:
        kind = PermissionCompletedKind(raw_kind)
    except ValueError:
        # SDK 0.3.0 sometimes echoes "approve-once" / other PermissionDecisionKind
        # values here. Treat any unknown value as APPROVED so the wire event parses.
        kind = PermissionCompletedKind.APPROVED
    return PermissionCompletedResult(kind=kind)


PermissionCompletedResult.from_dict = staticmethod(  # type: ignore[assignment]
    _patched_permission_completed_result_from_dict
)
_gh_session_events.PermissionCompletedResult.from_dict = staticmethod(  # type: ignore[assignment]
    _patched_permission_completed_result_from_dict
)

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
        # Pass --config-dir to the CLI on the subprocess argv from process
        # start, NOT just as a create_session kwarg.
        # The kwarg-only path lets the CLI bootstrap against its default
        # config dir (~/.copilot or similar) before the session is created,
        # which fails under chroot when that path isn't writable. With
        # --config-dir on argv, the CLI writes config.json / logs / telemetry
        # into the room folder (which is writable for the user under chroot).
        cli_args: list[str] = []
        if self.cfg.room_folder_path:
            cli_args.extend(["--config-dir", self.cfg.room_folder_path])

        # Run the CLI in offline mode. This disables:
        #   - the GitHub auth handshake (the "Failed to validate SDK token
        #     (401)" warning is the CLI calling api.github.com/copilot/user
        #     with our SEMOSS bearer, which is not a real GitHub PAT)
        #   - telemetry event sends (copilot-cli/cli.telemetry)
        #   - auto-update checks
        #   - the built-in github-mcp-server (we route MCPs ourselves)
        # Model calls still flow through the BYOK `provider=` SDK kwarg, which
        # offline mode requires; we also export COPILOT_PROVIDER_BASE_URL
        # explicitly so the CLI's offline-mode precondition check is satisfied
        # regardless of how it inspects the provider config.
        cli_env: dict[str, str] = {
            "COPILOT_OFFLINE": "true",
            "COPILOT_AUTO_UPDATE": "false",
        }
        if self.cfg.base_url:
            cli_env["COPILOT_PROVIDER_BASE_URL"] = self.cfg.base_url
            cli_env["COPILOT_PROVIDER_TYPE"] = "openai"
        # Offline + BYOK mode validates config at CLI start, before
        # create_session - so the model id has to be on the env / argv,
        # not just on the create_session kwarg. Otherwise the CLI exits
        # with: "BYOK providers require an explicit model."
        if self.cfg.model:
            cli_env["COPILOT_MODEL"] = self.cfg.model
            cli_env["COPILOT_PROVIDER_MODEL_ID"] = self.cfg.model
            cli_env["COPILOT_PROVIDER_WIRE_MODEL"] = self.cfg.model

        # Sandbox wrapper reads SEMOSS_SANDBOX_* from its own env, but SubprocessConfig
        # takes an explicit env dict that does not inherit os.environ - so forward them.
        for key, val in os.environ.items():
            if key.startswith("SEMOSS_SANDBOX_"):
                cli_env[key] = val

        subprocess_cfg = SubprocessConfig(
            cli_path=self.cfg.cli_path or os.environ.get("COPILOT_CLI_PATH"),
            cli_args=cli_args,
            cwd=self.cfg.cwd_path,
            env=cli_env,
            github_token=bearer,
            use_logged_in_user=False,
            log_level="info",
        )
        self._client = CopilotClient(subprocess_cfg)
        self._client_started = False

    # Public sync entry point - Java calls this via PyTranslator.runDirectPy.
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

        # Pure pass-through - Java's `sessionStateExists` checks the CLI's own
        # `events.jsonl` (mirroring ClaudeCodeManager.agentHistoryExists), so
        # the same file we're about to hand the SDK is the file Java consulted.
        # No hand-rolled sentinel, no preemptive sanitize, no recovery purge:
        # if the CLI rejects its own log we want the error to surface, not be
        # masked.
        if self.cfg.session_exists:
            session = await self._client.resume_session(
                self.cfg.room_id, **common_kwargs
            )
        else:
            session = await self._client.create_session(
                session_id=self.cfg.room_id, **common_kwargs
            )

        # Disable per-tool permission prompting at the wire level. Without
        # this, every tool invocation round-trips through
        # PermissionHandler.approve_all -> handle_pending_permission_request,
        # and the CLI then echoes a `permission.completed` event with
        # `kind: "approve-once"` that the installed github-copilot-sdk==0.3.0
        # cannot deserialize (PermissionCompletedKind enum expects
        # "approved", not "approve-once" - wire-format asymmetry between
        # PermissionDecisionKind and PermissionCompletedKind in 0.3.0).
        # The unparseable event crashes the asyncio notification handler and
        # the bash tool fails with `Unhandled permission result kind:
        # [object Object]`, which is what the model surfaces as "I'm blocked
        # by a tool permission error".
        #
        # set_approve_all tells the CLI to auto-approve every tool without
        # firing per-tool permission events, sidestepping the broken path.
        # The on_permission_request handler is still required by the SDK
        # contract but should not be invoked while approve-all is on.
        # FE-side approval routing can be wired later when the SDK is fixed.
        try:
            await session.rpc.permissions.set_approve_all(
                PermissionsSetApproveAllRequest(enabled=True)
            )
        except Exception as exc:  # pylint: disable=broad-except
            # Non-fatal - fall back to the per-tool callback path.
            if smss_stream:
                smss_stream(
                    build_session_error_envelope(
                        error_type="set_approve_all_failed",
                        message=f"set_approve_all failed; falling back to per-tool callback: {exc!r}",
                        status_code=None,
                        timestamp=iso_now(),
                        session_id=self.cfg.room_id,
                    ),
                    stream_type="content",
                )

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
        # MCPHTTPServerConfig requires `tools` - the CLI rejects servers with
        # no tools field ("No tools specified for server ... Skipping server
        # due to invalid configuration"). "*" enables every tool the server
        # advertises, matching the in-Java path that lets the CLI pull tool
        # lists at runtime.
        #
        # Bearer must use the 3-segment shape expected by SEMOSS:
        #     accessKey:secretKey:room-{roomId}
        # Monolith CodeAssistantFilter splits on ":" and only extracts the
        # roomId when split.length == 3 with the third segment starting with
        # "room-". Without that, OpenAIEndpoints / MCP endpoints create a
        # fresh SEMOSS Room every turn instead of reusing the room linked to
        # this conversation, which breaks multi-turn history reuse.
        servers: dict[str, dict[str, Any]] = {}
        bearer = (
            f"Bearer {self.cfg.access_key}:{self.cfg.secret_key}"
            f":room-{self.cfg.room_id}"
        )
        for mcp in self.cfg.mcps or []:
            safe_name = mcp.name.replace(" ", "_").lower()
            servers[safe_name] = {
                "type": "http",
                "url": mcp.url,
                "headers": {"Authorization": bearer},
                "tools": ["*"],
            }
        return servers

    def _build_provider(self) -> Optional[dict[str, Any]]:
        if not self.cfg.base_url:
            return None
        # The bearer_token must include the `:room-{roomId}` 3rd segment so
        # CodeAssistantFilter can pin the model call to the same SEMOSS
        # Room across follow-on turns. The api_key field is unused by the
        # filter (which only inspects Authorization: Bearer ...), but we
        # keep an empty value present to satisfy the SDK's BYOK precondition.
        return {
            "type": "openai",
            "base_url": self.cfg.base_url,
            "api_key": (
                f"{self.cfg.access_key}:{self.cfg.secret_key}"
                f":room-{self.cfg.room_id}"
            ),
            "bearer_token": (
                f"{self.cfg.access_key}:{self.cfg.secret_key}"
                f":room-{self.cfg.room_id}"
            ),
        }

    def _build_available_tools(self) -> Optional[list[str]]:
        if not self.cfg.allowed_tools:
            return None
        # Empty list from Java means "no allow-list set"; keep SDK default.
        explicit = [t for t in self.cfg.allowed_tools if t and t != "*"]
        # If caller passed only "*" or nothing meaningful, defer to SDK default.
        return explicit or None
