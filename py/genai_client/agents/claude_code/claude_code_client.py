from typing import Optional
import asyncio
import json
from pydantic import BaseModel
from claude_agent_sdk import (
    query,
    ClaudeAgentOptions,
    AssistantMessage,
    TextBlock,
    ClaudeSDKClient,
    PermissionMode,
    HookMatcher,
)
from .claude_code_utils import _build_change_logger

from ...debug_logger.debug_logger import DebugLogger

logger = DebugLogger(
    log_dir="/Users/rweiler/Desktop/LOG_FILES",
    log_file_name="claude-code-client.txt",
    class_name=__name__,
).logger


def _stderr_handler(line: str):
    logger.debug(f"Claude-Code-stderr:: {line.rstrip()}")


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
    base_url: Optional[str] = ""
    allowed_tools: Optional[list[str]] = None
    mcps: Optional[list[MCP]] = None
    insight_id: Optional[str] = None


class ClaudeCodeClient:
    def __init__(self, **kwargs):
        self.configuration = CCInitArgs(**kwargs)
        (mcps, allowed_tools) = self._resolve_mcps(
            self.configuration.mcps or [],
            self.configuration.allowed_tools or [],
            self.configuration.access_key,
            self.configuration.secret_key,
        )

        change_logger = _build_change_logger(self.configuration.cwd_path)

        self.agent_options = ClaudeAgentOptions(
            # permission_mode=self.configuration.permission_mode,
            stderr=_stderr_handler,
            permission_mode="bypassPermissions",
            max_turns=10,
            setting_sources=["project"],
            model=self.configuration.model,
            cwd=self.configuration.cwd_path,
            disallowed_tools=[
                "AskUserQuestion",
            ],
            allowed_tools=allowed_tools
            or [
                "Agent",
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
                "TodoRead",
                "TodoWrite",
                "Task",
            ],
            mcp_servers=mcps,
            env={
                "ANTHROPIC_BASE_URL": f"{self.configuration.base_url}",
                "ANTHROPIC_AUTH_TOKEN": f"{self.configuration.access_key}:{self.configuration.secret_key}",
                # "ANTHROPIC_API_KEY": f"{self.configuration.access_key}:{self.configuration.secret_key}",
                "ANTHROPIC_API_KEY": f"room-{self.configuration.room_id}",
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "true",
                "ENABLE_TOOL_SEARCH": "true",
                "ANTHROPIC_LOG": "debug",
                "ANTHROPIC_DEFAULT_HAIKU_MODEL_NAME": self.configuration.model,
                "ANTHROPIC_DEFAULT_HAIKU_MODEL": self.configuration.model,
                "ANTHROPIC_DEFAULT_SONNET_MODEL_NAME": self.configuration.model,
                "ANTHROPIC_DEFAULT_SONNET_MODEL": self.configuration.model,
                "ANTHROPIC_DEFAULT_OPUS_MODEL_NAME": self.configuration.model,
                "ANTHROPIC_DEFAULT_OPUS_MODEL": self.configuration.model,
                "CLAUDE_CODE_SUBAGENT_MODEL": self.configuration.model,
            },
            hooks={
                "PostToolUse": [
                    HookMatcher(
                        matcher="Write|Edit|MultiEdit|NotebookEdit|Bash",
                        hooks=[change_logger],
                    )
                ],
            },
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
        # new_prompt = f"[[SEMOSS_CONTEXT:insightId={self.configuration.insight_id},roomId={self.configuration.room_id}]]\n{prompt}"
        # parent_room_tag = f"[[PARENT_ROOM_ID={self.configuration.room_id}]]"
        # if system_prompt:
        #     self.agent_options.system_prompt = f"{parent_room_tag}\n{system_prompt}"
        # else:
        #     self.agent_options.system_prompt = parent_room_tag
        final_message = ""
        async for message in query(
            prompt=prompt,
            options=self.agent_options,
        ):
            logger.info(f"Claude-Code-chunk:: {message}")
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        print(f"Claude: {block.text}")
                        final_message += block.text
        return final_message

    def _resolve_mcps(
        self,
        mcps: list[MCP],
        allowed_tools: list[str],
        access_key: str,
        secret_key: str,
    ) -> tuple[dict, list[str]]:
        mcp_dict = {}
        for mcp in mcps:
            safe_name = mcp.name.replace(" ", "_").lower()
            mcp_dict[safe_name] = {
                "url": mcp.url,
                "type": "http",
                "headers": {"Authorization": f"Bearer {access_key}:{secret_key}"},
            }
            allowed_tools.append(f"mcp__{safe_name}__*")
        return (mcp_dict, allowed_tools)
