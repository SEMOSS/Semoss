from typing import Optional
import asyncio
from pydantic import BaseModel
from claude_agent_sdk import (
    query,
    ClaudeAgentOptions,
    AssistantMessage,
    TextBlock,
    ClaudeSDKClient,
    PermissionMode,
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


class ClaudeCodeClient:
    def __init__(self, **kwargs):
        self.configuration = CCInitArgs(**kwargs)
        (mcps, allowed_tools) = self._resolve_mcps(
            self.configuration.mcps or [],
            self.configuration.allowed_tools or [],
            self.configuration.access_key,
            self.configuration.secret_key,
        )
        self.agent_options = ClaudeAgentOptions(
            permission_mode=self.configuration.permission_mode,
            model=self.configuration.model,
            cwd=self.configuration.cwd_path,
            allowed_tools=allowed_tools
            or [
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
