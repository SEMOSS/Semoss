"""GitHub Copilot models as a SEMOSS text-generation client.

Every ask runs as a fresh Copilot SDK session authenticated with the asking
user's GitHub OAuth token (the ``github_token`` kwarg the Java engine adds per
call), so usage is billed to that user's own Copilot subscription. One Copilot
CLI runtime is shared per python process (SEMOSS starts one process per engine).
"""

import asyncio
import queue
import shutil
import tempfile
import threading
from typing import Any, Dict, List, Optional, Tuple

from copilot import CopilotClient
from copilot.generated.session_events import (
    AssistantMessageData,
    AssistantMessageDeltaData,
    AssistantUsageData,
    SessionErrorData,
)
from copilot.session import PermissionHandler
from smss_thread_local import get_smss_stream

from ..constants import AskModelEngineResponse2
from ..message_builders.semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSTextMessagePart,
)
from ..message_builders.semoss_base.semoss_streaming_util import StreamUtil
from ..usage_ceiling import count_prompt_tokens
from .abstract_text_generation_client import AbstractTextGenerationClient
from .model_engine_exception import ErrorDetails, ModelEngineException

_DONE = object()
_TURN_TIMEOUT = 600.0
_DEFAULT_SYSTEM_PROMPT = "You are a helpful assistant."

_loop_lock = threading.Lock()
_loop: Optional[asyncio.AbstractEventLoop] = None
_client: Optional[CopilotClient] = None
_start_lock = asyncio.Lock()
_base_dir: Optional[str] = None


def _runtime_base_dir() -> str:
    """Scratch storage for the shared runtime: "empty" mode refuses to fall back
    to ~/.copilot, and every session is deleted after its ask anyway."""
    global _base_dir
    if _base_dir is None:
        _base_dir = tempfile.mkdtemp(prefix="semoss-copilot-")
    return _base_dir


def _sdk_loop() -> asyncio.AbstractEventLoop:
    """One event loop on a daemon thread. The SDK binds its RPC transport to the
    loop that started the client, so the shared client must always run there."""
    global _loop
    with _loop_lock:
        if _loop is None:
            loop = asyncio.new_event_loop()
            threading.Thread(
                target=loop.run_forever, name="copilot-sdk", daemon=True
            ).start()
            _loop = loop
        return _loop


async def _shared_client() -> CopilotClient:
    global _client
    async with _start_lock:
        if _client is None:
            # no client-level token: every session carries its own user's token.
            # "empty" mode gives a bare chat runtime with no host tools or skills.
            client = CopilotClient(
                use_logged_in_user=False,
                mode="empty",
                base_directory=_runtime_base_dir(),
            )
            try:
                await client.start()
            except Exception:
                await _stop_quietly(client)
                raise
            _client = client
        return _client


async def _stop_quietly(client: CopilotClient) -> None:
    try:
        await client.stop()
    except Exception:
        pass


async def _runtime_alive() -> bool:
    if _client is None:
        return False
    try:
        await asyncio.wait_for(_client.get_status(), 5)
        return True
    except Exception:
        return False


async def _forget_client() -> None:
    """Stop and drop a dead shared runtime so the next ask starts a fresh one."""
    global _client
    client, _client = _client, None
    if client is not None:
        await _stop_quietly(client)


def build_prompt(messages: List[SEMOSSMessage]) -> Tuple[Optional[str], str]:
    """Flatten the room branch into ``(system_prompt, prompt)`` for one send."""
    system_prompt = messages[-1].param_map.get("system_prompt") if messages else None
    turns: List[Tuple[str, str]] = []
    for message in messages:
        text = "\n".join(
            part.text
            for part in (message.parts or [])
            if isinstance(part, SEMOSSTextMessagePart)
        ) or (message.content or "")
        if text:
            turns.append(("Assistant" if message.io == "OUTPUT" else "User", text))
    if len(turns) <= 1:
        return system_prompt, turns[0][1] if turns else ""
    # shortcut: history is flattened into one prompt because each ask is a fresh
    # Copilot session; upgrade to resume_session per room if answer quality suffers
    transcript = "\n\n".join(f"{role}: {text}" for role, text in turns)
    return (
        system_prompt,
        "Conversation so far; reply to the last User message.\n\n" + transcript,
    )


async def _turn(
    model: str,
    prompt: str,
    system_prompt: Optional[str],
    github_token: str,
    deltas: "queue.Queue[Any]",
) -> Tuple[str, int, int]:
    """One Copilot session on the SDK loop. Streams text deltas into ``deltas``
    and returns ``(text, prompt_tokens, response_tokens)``."""
    chunks: List[str] = []
    final: List[str] = []
    usage = {"input": 0.0, "output": 0.0}
    errors: List[str] = []

    def on_event(event: Any) -> None:
        data = event.data
        if isinstance(data, AssistantMessageDeltaData) and data.delta_content:
            chunks.append(data.delta_content)
            deltas.put(data.delta_content)
        elif isinstance(data, AssistantMessageData) and data.content:
            final.append(data.content)
        elif isinstance(data, AssistantUsageData):
            usage["input"] += data.input_tokens or 0
            usage["output"] += data.output_tokens or 0
        elif isinstance(data, SessionErrorData):
            errors.append(data.message)

    client = None
    session = None
    try:
        client = await _shared_client()
        session = await client.create_session(
            on_permission_request=PermissionHandler.approve_all,
            model=model,
            github_token=github_token,
            available_tools=[],
            streaming=True,
            on_event=on_event,
            # always replaced: without it the runtime answers as "GitHub Copilot,
            # an AI coding agent" from its own built-in prompt
            system_message={
                "mode": "replace",
                "content": system_prompt or _DEFAULT_SYSTEM_PROMPT,
            },
        )
        await session.send_and_wait(prompt, timeout=_TURN_TIMEOUT)
        if errors:
            raise RuntimeError("; ".join(errors))
        return "".join(final) or "".join(chunks), int(usage["input"]), int(usage["output"])
    except Exception:
        # a failure before the session exists is either the user's (model id,
        # token) or a dead runtime; only the dead runtime is rebuilt
        if session is None and not await _runtime_alive():
            await _forget_client()
        raise
    finally:
        deltas.put(_DONE)
        if session is not None:
            try:
                await session.disconnect()
                await client.delete_session(session.session_id)
            except Exception:
                pass


class GitHubCopilotModelClient(AbstractTextGenerationClient):
    """Chat through GitHub Copilot with the asking user's own subscription."""

    def ask_call(
        self, prefix: str = "", **kwargs: Any
    ) -> AskModelEngineResponse2 | ErrorDetails:
        # popped first: whatever is left in kwargs lands on the last message's
        # param_map, which is persisted with the room
        github_token = kwargs.pop("github_token", None)
        kwargs.pop("_semoss_input_modalities", None)
        smss_stream = get_smss_stream()
        try:
            if not github_token:
                raise ValueError(
                    "Connect your GitHub account to use GitHub Copilot models"
                )
            messages = self.build_semoss_messages(
                model_settings=self.model_settings, **kwargs
            )
            system_prompt, prompt = build_prompt(messages)
            # Copilot has no output maximum: the ceiling can refuse the call but not cap it
            self.fit_output_to_usage_ceiling(
                None,
                lambda: count_prompt_tokens(
                    [system_prompt, prompt], message_count=len(messages)
                ),
                can_cap=False,
            )
            deltas: "queue.Queue[Any]" = queue.Queue()
            future = asyncio.run_coroutine_threadsafe(
                _turn(self.model_name, prompt, system_prompt, github_token, deltas),
                _sdk_loop(),
            )
            # relayed on this thread: smss_stream reads request state kept thread-local
            while (delta := deltas.get(timeout=_TURN_TIMEOUT + 60)) is not _DONE:
                if smss_stream:
                    smss_stream(
                        StreamUtil.create_content_chunk(delta), stream_type="content"
                    )
                print(prefix + delta, end="")
            text, prompt_tokens, response_tokens = future.result()
            if smss_stream:
                smss_stream(
                    StreamUtil.create_usage_chunk(
                        input_tokens=prompt_tokens, output_tokens=response_tokens
                    ),
                    stream_type="usage",
                )
                smss_stream(
                    StreamUtil.create_finish_reason_chunk("stop"),
                    stream_type="content",
                    interim=False,
                )
            return AskModelEngineResponse2(
                response=text,
                prompt_tokens=prompt_tokens,
                response_tokens=response_tokens,
                schemaVersion=2,
                io="OUTPUT",
                parts=[{"type": "TEXT", "text": text}] if text else [],
            )
        except Exception as e:
            return ModelEngineException(
                error=e, client="github_copilot", model=str(self.model_name)
            ).parse_error()


def list_models(github_token: str) -> List[Dict[str, Any]]:
    """Models the token's Copilot plan offers, for the Add Model dropdown.

    Runs a short-lived runtime signed in as that user, so it never shares the
    engine's runtime or its event loop."""

    async def _list() -> List[Dict[str, Any]]:
        base_dir = tempfile.mkdtemp(prefix="semoss-copilot-models-")
        client = CopilotClient(
            github_token=github_token,
            use_logged_in_user=False,
            mode="empty",
            base_directory=base_dir,
        )
        await client.start()
        try:
            auth = await client.get_auth_status()
            if not auth.isAuthenticated:
                # the runtime's own reason (no Copilot seat, blocked by an org
                # policy, token rejected) is the most useful message to show
                raise RuntimeError(
                    "GitHub Copilot did not accept the GitHub login: "
                    + (auth.statusMessage or "not authenticated")
                )
            models = await client.list_models()
        finally:
            await client.stop()
            shutil.rmtree(base_dir, ignore_errors=True)
        return sorted(
            (
                {
                    "id": model.id,
                    "name": model.name,
                    "policy": model.policy.state if model.policy else None,
                    "multiplier": model.billing.multiplier if model.billing else None,
                }
                for model in models
            ),
            key=lambda model: model["id"],
        )

    return asyncio.run(_list())
