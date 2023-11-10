from typing import Optional, Union, List, Dict, Any, Tuple
from .openai_chat_completion_client import OpenAiChatCompletion

class OpenAiClientController():
    
    def __init__(self, **kwargs):
    
        self.chat_type = kwargs.pop(
            'chat_type', 
            'chat-completion'
        )
        
        if (self.chat_type == 'chat-completion'):
            self.openai_class = OpenAiChatCompletion(**kwargs)
        else:
            self.openai_class = OpenAiChatCompletion

        
    def ask(self, **kwargs) -> Dict:
        return self.openai_class.ask(**kwargs)

    def embeddings(self, **kwargs) -> List[float]:
        return self.openai_class.embeddings(**kwargs)
