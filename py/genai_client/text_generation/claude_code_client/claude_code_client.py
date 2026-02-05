from typing import Optional, Dict, Any, Union, TYPE_CHECKING, List

if TYPE_CHECKING:
    # injected into globals in handle_python of gaas_tcp_server_handler.py
    def smss_stream(
        data: Any, stream_type: str = "content", interim: bool = True
    ) -> None: ...


from ..model_engine_exception import (
    ModelEngineException,
    AnthropicRefusalError,
)
from smss_thread_local import get_smss_stream
import json
from pydantic import BaseModel
from ..abstract_text_generation_client import AbstractTextGenerationClient
from claude_agent_sdk import query, ClaudeAgentOptions, AssistantMessage, TextBlock


class ClaudeCodeClient(AbstractTextGenerationClient):
    def __init__(self, model: str, cli_path: str, cwd_path: str, **kwargs):
        self.model = model
        self.cli_path = cli_path
        self.cwd_path = cwd_path
        self.client = self._get_client()

    def _get_client(self) -> ClaudeAgentOptions:
        return ClaudeAgentOptions(
            model=self.model, cli_path=self.cli_path, cwd=self.cwd_path
        )

    def ask_call(self, prefix: str = "", **kwargs):
        if not self.client:
            raise ValueError("Client is not initialized")
        try:
            if (
                hasattr(self.model_settings, "global_param_override")
                and self.model_settings.global_param_override
            ):
                kwargs.update(self.model_settings.global_param_override)

            semoss_messages = self.build_semoss_messages(
                model_settings=self.model_settings, **kwargs
            )

        except Exception as e:
            return ModelEngineException(
                error=e, client="anthropic", model=self.model_name
            ).parse_error()
