from typing import List, Dict
from .vertex_text_client import VertexTextClient
from  .vertext_code_completion_client import VertexCodeCompletionClient
from .vertex_code_chat_client import VertexCodeChatClient
from .vertext_chat_client import VertexChatClient
from ...constants import (
    CHAT_TYPE
)


class VertexAiClientController():
    
    def __init__(self, **kwargs):
        
        self.chat_type = kwargs.pop(
            CHAT_TYPE, 
            'text'
        )
        
        if (self.chat_type == 'text'):
            self.vertex_class = VertexTextClient(**kwargs)
        elif (self.chat_type == 'chat'):
            self.vertex_class = VertexChatClient(**kwargs)
        elif (self.chat_type == 'codechat'):
            self.vertex_class = VertexCodeChatClient(**kwargs)
        elif (self.chat_type == 'code'):
            self.vertex_class = VertexCodeCompletionClient(**kwargs)

        
    def ask(self, **kwargs) -> Dict:
        return self.vertex_class.ask(**kwargs)

    def embeddings(self, **kwargs) -> List[float]:
        return self.vertex_class.embeddings(**kwargs)