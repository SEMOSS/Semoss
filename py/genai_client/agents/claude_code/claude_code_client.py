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


class CCInitArgs(BaseModel):
    model: str
    cwd_path: str
    room_id: str
    access_key: str
    secret_key: str
    permission_mode: PermissionMode | None = "acceptEdits"
    base_url: Optional[str] = "http://localhost:9090/Monolith/api/model/anthropic"
    allowed_tools: Optional[list[str]] = None


class ClaudeCodeClient:
    def __init__(self, **kwargs):
        self.configuration = CCInitArgs(**kwargs)
        self.agent_options = ClaudeAgentOptions(
            permission_mode=self.configuration.permission_mode,
            model=self.configuration.model,
            cwd=self.configuration.cwd_path,
            allowed_tools=self.configuration.allowed_tools
            or [
                "Bash",
                "Glob",
                "Read",
                "Write",
                "Edit",
                "Grep",
                "WebSearch",
                "WebFetch",
                "AskUserQuestion",
            ],
            env={
                "ANTHROPIC_BASE_URL": f"{self.configuration.base_url}/{self.configuration.room_id}",
                "ANTHROPIC_AUTH_TOKEN": f"{self.configuration.access_key}:{self.configuration.secret_key}",
                "ANTHROPIC_API_KEY": f"{self.configuration.access_key}:{self.configuration.secret_key}",
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "true",
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
