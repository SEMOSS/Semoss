from typing import Optional
from uuid import uuid4
import asyncio, os
from pydantic import BaseModel, field_validator
from claude_agent_sdk import (
    ClaudeAgentOptions,
    AssistantMessage,
    TextBlock,
    ClaudeSDKClient,
    PermissionMode,
    UserMessage,
    ResultMessage,
)
from smss_thread_local import get_smss_stream
from .claude_code_utils import (
    make_assistant_event,
    make_result_event,
    make_tool_result_event,
    make_user_prompt_event,
)
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil
from ...utils import string_to_bool

# from ...debug_logger.debug_logger import DebugLogger

# logger = DebugLogger(
#     log_dir="/Users/rweiler/Desktop/LOG_FILES",
#     log_file_name="claude-code-client.txt",
#     class_name=__name__,
# ).logger


# def _stderr_handler(line: str):
#     logger.debug(f"Claude-Code-stderr:: {line.rstrip()}")


class MCP(BaseModel):
    url: str
    name: str


class CCInitArgs(BaseModel):
    model: str
    cwd_path: str
    room_id: str
    access_key: str
    secret_key: str
    permission_mode: PermissionMode | None = "acceptEdits"
    agent_history_exists: bool = False
    base_url: Optional[str] = ""
    allowed_tools: Optional[list[str]] = None
    mcps: Optional[list[MCP]] = None
    insight_id: Optional[str] = None
    room_folder_path: Optional[str] = None
    sandbox_cli_path: Optional[str] = None
    sandbox_env: Optional[dict[str, str]] = None

    @field_validator("agent_history_exists", mode="before")
    @classmethod
    def _coerce_agent_history_exists(cls, v):
        return string_to_bool(v)


class ClaudeCodeClient:
    def __init__(self, **kwargs):
        self.configuration = CCInitArgs(**kwargs)
        # logger.debug(self.configuration)
        mcps, allowed_tools = self._resolve_mcps(
            self.configuration.mcps or [],
            self.configuration.allowed_tools or [],
            self.configuration.access_key,
            self.configuration.secret_key,
            self.configuration.room_id,
        )

        sandbox_env = self.configuration.sandbox_env or {}
        claude_env = {
            "ANTHROPIC_BASE_URL": f"{self.configuration.base_url}",
            "ANTHROPIC_AUTH_TOKEN": f"{self.configuration.access_key}:{self.configuration.secret_key}:room-{self.configuration.room_id}",
            "ANTHROPIC_API_KEY": f"{self.configuration.access_key}:{self.configuration.secret_key}:room-{self.configuration.room_id}",
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "true",
            "ENABLE_TOOL_SEARCH": "true",
            "ANTHROPIC_DEFAULT_HAIKU_MODEL_NAME": self.configuration.model,
            "ANTHROPIC_DEFAULT_HAIKU_MODEL": self.configuration.model,
            "ANTHROPIC_DEFAULT_SONNET_MODEL_NAME": self.configuration.model,
            "ANTHROPIC_DEFAULT_SONNET_MODEL": self.configuration.model,
            "ANTHROPIC_DEFAULT_OPUS_MODEL_NAME": self.configuration.model,
            "ANTHROPIC_DEFAULT_OPUS_MODEL": self.configuration.model,
            "CLAUDE_CODE_SUBAGENT_MODEL": self.configuration.model,
            "CLAUDE_CONFIG_DIR": self.configuration.room_folder_path or "",
            "PATH": os.environ.get("PATH", ""),
            "HOME": os.environ.get("HOME", ""),
        }
        claude_env.update(sandbox_env)

        self.agent_options = ClaudeAgentOptions(
            # stderr=_stderr_handler,
            thinking={"type": "enabled", "budget_tokens": 100000},
            permission_mode="bypassPermissions",
            max_turns=50,
            setting_sources=["project"],
            model=self.configuration.model,
            cwd=self.configuration.cwd_path,
            resume=(
                self.configuration.room_id
                if self.configuration.agent_history_exists
                else None
            ),
            session_id=(
                None
                if self.configuration.agent_history_exists
                else self.configuration.room_id
            ),
            disallowed_tools=["AskUserQuestion"],
            allowed_tools=allowed_tools
            or [
                "Skill",
                "Bash",
                "BashOutput",
                "KillBash",
                "Read",
                "Write",
                "Edit",
                "MultiEdit",
                "NotebookEdit",
                "Glob",
                "Grep",
                "LS",
                "WebSearch",
                "WebFetch",
                "TaskCreate",
                "TaskUpdate",
                "TaskGet",
                "TaskList",
                "Task",
            ],
            mcp_servers=mcps,
            env=claude_env,
            **(
                {"cli_path": self.configuration.sandbox_cli_path}
                if self.configuration.sandbox_cli_path
                else {}
            ),
        )

        self.sdk_client = ClaudeSDKClient(self.agent_options)

    def query_cc(
        self, prompt: str, system_prompt: Optional[str] = None, **kwargs
    ) -> str:
        """Synchronous wrapper that bridges into the async SDK."""
        return asyncio.run(self._query_cc_async(prompt, system_prompt, **kwargs))

    async def _query_cc_async(
        self, prompt: str, system_prompt: Optional[str] = None, **kwargs
    ) -> str:
        if system_prompt:
            self.agent_options.system_prompt = {
                "type": "preset",
                "preset": "claude_code",
                "append": system_prompt,
            }
        smss_stream = get_smss_stream()
        final_message = ""

        if smss_stream:
            smss_stream(
                make_user_prompt_event(
                    prompt=prompt,
                    prompt_id=str(uuid4()),
                    session_id=self.configuration.room_id,
                ),
                stream_type="content",
            )

        async with self.sdk_client as client:
            await client.query(prompt)
            async for message in client.receive_response():
                # logger.info(f"Claude-Code-chunk:: {message}")
                if isinstance(message, AssistantMessage):
                    for block in message.content:
                        if isinstance(block, TextBlock):
                            print(f"Claude: {block.text}")
                            final_message += block.text
                    if smss_stream:
                        event = make_assistant_event(message)
                        if event is not None:
                            smss_stream(event, stream_type="content")
                elif isinstance(message, UserMessage):
                    if smss_stream:
                        event = make_tool_result_event(message)
                        if event is not None:
                            smss_stream(event, stream_type="content")
                elif isinstance(message, ResultMessage):
                    if smss_stream:
                        smss_stream(make_result_event(message), stream_type="content")

        if smss_stream:
            smss_stream(
                StreamUtil.create_finish_reason_chunk("stop"),
                stream_type="content",
                interim=False,
            )
        self.agent_options.resume = self.configuration.room_id
        self.agent_options.session_id = None
        return final_message

    def _resolve_mcps(
        self,
        mcps: list[MCP],
        allowed_tools: list[str],
        access_key: str,
        secret_key: str,
        room_id: str,
    ) -> tuple[dict, list[str]]:
        mcp_dict = {}
        bearer = f"Bearer {access_key}:{secret_key}:room-{room_id}"
        for mcp in mcps:
            safe_name = mcp.name.replace(" ", "_").lower()
            mcp_dict[safe_name] = {
                "url": mcp.url,
                "type": "http",
                "headers": {"Authorization": bearer},
            }
            allowed_tools.append(f"mcp__{safe_name}__*")
        return (mcp_dict, allowed_tools)
