"""GitHub Copilot models as a SEMOSS text-generation client.

Every ask runs as a fresh Copilot SDK session authenticated with the asking
user's GitHub OAuth token (the ``github_token`` kwarg the Java engine adds per
call), so usage is billed to that user's own Copilot subscription. One Copilot
CLI runtime is shared per python process (SEMOSS starts one process per engine).
"""

import asyncio
import queue
import threading
from typing import Any, List, Optional, Tuple

from copilot import CopilotClient, SubprocessConfig
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

_loop_lock = threading.Lock()
_loop: Optional[asyncio.AbstractEventLoop] = None
_client: Optional[CopilotClient] = None
_start_lock = asyncio.Lock()


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
            # no client-level token: every session carries its own user's token
            _client = CopilotClient(
                SubprocessConfig(use_logged_in_user=False), auto_start=False
            )
        # a no-op while connected; respawns the CLI after it exited
        await _client.start()
        return _client


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
            system_message=(
                {"mode": "replace", "content": system_prompt} if system_prompt else None
            ),
        )
        await session.send_and_wait(prompt, timeout=_TURN_TIMEOUT)
        if errors:
            raise RuntimeError("; ".join(errors))
        return "".join(final) or "".join(chunks), int(usage["input"]), int(usage["output"])
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
