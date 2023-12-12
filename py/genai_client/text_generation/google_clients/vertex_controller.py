from typing import List, Dict
from ...constants import (
    CHAT_TYPE
)

class VertexAiClientController():
    
    def __init__(self, **kwargs):
        
        chat_type = kwargs.pop(
            CHAT_TYPE, 
            'text'
        )
        
        if (chat_type == 'text'):
            from .vertex_text_client import VertexTextClient
            self.vertex_class = VertexTextClient(**kwargs)
        elif (chat_type == 'chat'):
            from .vertext_chat_client import VertexChatClient
            self.vertex_class = VertexChatClient(**kwargs)
        elif (chat_type == 'codechat'):
            from .vertex_code_chat_client import VertexCodeChatClient
            self.vertex_class = VertexCodeChatClient(**kwargs)
        elif (chat_type == 'code'):
            from  .vertext_code_completion_client import VertexCodeCompletionClient
            self.vertex_class = VertexCodeCompletionClient(**kwargs)
        else:
            raise ValueError(f"Chat type '{chat_type}' has not been defined.")

        
    def ask(self, **kwargs) -> Dict:
        return self.vertex_class.ask(**kwargs)

    def embeddings(self, **kwargs) -> List[float]:
        return self.vertex_class.embeddings(**kwargs)