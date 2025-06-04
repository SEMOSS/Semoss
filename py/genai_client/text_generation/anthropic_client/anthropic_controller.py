from typing import List, Dict
from ...constants import CHAT_TYPE


class AnthropicClientController:

    def __init__(self, **kwargs):

        chat_type = kwargs.pop(CHAT_TYPE, "chat-completion")

        if chat_type == "chat-completion":
            from .anthropic_vertex_chat_client import AnthropicVertexClient

            self.anthropic_class = AnthropicVertexClient(**kwargs)
        else:
            raise ValueError(f"Chat type '{chat_type}' has not been defined.")

    def ask(self, **kwargs) -> Dict:
        return self.anthropic_class.ask(**kwargs)

    def embeddings(self, **kwargs) -> List[float]:
        return self.anthropic_class.embeddings(**kwargs)
