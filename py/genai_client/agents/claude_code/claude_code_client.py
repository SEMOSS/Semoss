from typing import Optional, Dict, Any, Union, TYPE_CHECKING, List
from ...debug_logger.debug_logger import DebugLogger
import asyncio


if TYPE_CHECKING:
    # injected into globals in handle_python of gaas_tcp_server_handler.py
    def smss_stream(
        data: Any, stream_type: str = "content", interim: bool = True
    ) -> None: ...


logger = DebugLogger(
    log_dir="C:\\Users\\rweiler\\Desktop\\LOG_FILES",
    log_file_name="claude_code.txt",
    class_name=__name__,
).logger


from smss_thread_local import get_smss_stream
import json
from pydantic import BaseModel
from claude_agent_sdk import (
    query,
    ClaudeAgentOptions,
    AssistantMessage,
    TextBlock,
    ClaudeSDKClient,
)


class CCInitArgs(BaseModel):
    model: str
    cli_path: str
    cwd_path: str
    room_id: str
    access_key: str
    secret_key: str


class ClaudeCodeClient:
    def __init__(self, **kwargs):
        self.configuration = CCInitArgs(**kwargs)
        self.agent_options = ClaudeAgentOptions(
            model=self.configuration.model,
            # cli_path=self.configuration.cli_path,
            cwd=self.configuration.cwd_path,
            env={
                # Eventually append room_id to the end of this
                "ANTHROPIC_BASE_URL": f"http://localhost:9090/Monolith/api/model/anthropic",
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
        logger.info(f"Prompt sent to query: {prompt}")
        final_message = ""
        async for message in query(
            prompt=prompt,
            options=self.agent_options,  # use the instance options, not a new bare one
        ):
            logger.info(f"Message from stream: {message}")
            if hasattr(message, "result"):
                print(message.result)
            if hasattr(message, "content") and isinstance(message.content, list):
                final_message += "".join(
                    block.text for block in message.content if hasattr(block, "text")
                )
        logger.info(f"Final message from query: {final_message}")
        return final_message
