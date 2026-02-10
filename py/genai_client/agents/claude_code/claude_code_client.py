from typing import Optional, Dict, Any, Union, TYPE_CHECKING, List

if TYPE_CHECKING:
    # injected into globals in handle_python of gaas_tcp_server_handler.py
    def smss_stream(
        data: Any, stream_type: str = "content", interim: bool = True
    ) -> None: ...


from smss_thread_local import get_smss_stream
import json
from pydantic import BaseModel
from claude_agent_sdk import query, ClaudeAgentOptions, AssistantMessage, TextBlock


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
            cli_path=self.configuration.cli_path,
            cwd=self.configuration.cwd_path,
            env={
                # Eventually append room_id to the end of this
                "ANTHROPIC_BASE_URL": f"http://localhost:9090/Monolith/api/model/anthropic",
                "ANTHROPIC_AUTH_TOKEN": f"{self.configuration.access_key}:{self.configuration.secret_key}",
                "ANTHROPIC_API_KEY": f"{self.configuration.access_key}:{self.configuration.secret_key}",
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "true",
            },
        )

    def query_cc(self, prompt: str, **kwargs) -> AssistantMessage:
        my_query = query()
