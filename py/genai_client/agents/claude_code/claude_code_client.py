from typing import Optional, Any, TYPE_CHECKING
import asyncio
from pydantic import BaseModel
from claude_agent_sdk import (
    query,
    ClaudeAgentOptions,
    AssistantMessage,
    TextBlock,
    ClaudeSDKClient,
)

# if TYPE_CHECKING:
#     # injected into globals in handle_python of gaas_tcp_server_handler.py
#     def smss_stream(
#         data: Any, stream_type: str = "content", interim: bool = True
#     ) -> None: ...


class CCInitArgs(BaseModel):
    model: str
    cwd_path: str
    room_id: str
    access_key: str
    secret_key: str


class ClaudeCodeClient:
    def __init__(self, **kwargs):
        self.configuration = CCInitArgs(**kwargs)
        self.agent_options = ClaudeAgentOptions(
            permission_mode="acceptEdits",
            model=self.configuration.model,
            cwd=self.configuration.cwd_path,
            allowed_tools=[
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
                "ANTHROPIC_BASE_URL": f"http://localhost:9090/Monolith/api/model/anthropic/{self.configuration.room_id}",
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
