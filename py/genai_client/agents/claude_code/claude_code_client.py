from typing import Optional
import asyncio
import os
from datetime import datetime, timezone
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


def _build_change_logger(cwd: str):
    """
    Factory that returns a PostToolUse hook callback bound to the given cwd.
    Logs every file-modifying tool call to {cwd}/.claude/logs/change_log.txt.
    """
    log_path = os.path.join(cwd, ".claude", "logs", "change_log.txt")

    async def log_change(input_data: dict, tool_use_id: str | None, context) -> dict:
        tool_name = input_data.get("tool_name", "unknown")
        tool_input = input_data.get("tool_input", {})
        timestamp = datetime.now(timezone.utc).isoformat()

        file_path = (
            tool_input.get("file_path")
            or tool_input.get("path")
            or tool_input.get("file")
            or "N/A"
        )

        if tool_name == "Bash":
            command = tool_input.get("command", "N/A")
            entry = f"[{timestamp}] TOOL={tool_name} CMD={command}\n"
        else:
            # For Write/Edit/MultiEdit, include a short description
            description = tool_input.get("description", "")
            entry = f"[{timestamp}] TOOL={tool_name} FILE={file_path}"
            if description:
                entry += f" DESC={description}"
            entry += "\n"

        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(entry)
        except OSError:
            pass

        return {}

    return log_change


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
            permission_mode=self.configuration.permission_mode,
            setting_sources=["project"],
            model=self.configuration.model,
            cwd=self.configuration.cwd_path,
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
                "TodoRead",
                "TodoWrite",
                "Task",
                "AskUserQuestion",
            ],
            mcp_servers=mcps,
            env={
                "ANTHROPIC_BASE_URL": f"{self.configuration.base_url}",
                "ANTHROPIC_AUTH_TOKEN": f"{self.configuration.access_key}:{self.configuration.secret_key}",
                "ANTHROPIC_API_KEY": f"{self.configuration.access_key}:{self.configuration.secret_key}",
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "true",
                "ENABLE_TOOL_SEARCH": "true",
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
        self.agent_options.system_prompt = system_prompt
        final_message = ""
        async for message in query(
            prompt=prompt,
            options=self.agent_options,
        ):
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
