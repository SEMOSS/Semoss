from typing import Dict, Optional, List
from vertexai.language_models import CodeChatModel, ChatMessage

from .abstract_vertex_textgen_client import AbstractVertextAiTextGeneration
from ...constants import (
    ModelEngineResponse,
    FULL_PROMPT
)

class VertexCodeChatClient(AbstractVertextAiTextGeneration):
    '''
    The code chat API - Can power a chatbot that assists with code-related questions. For example, you can use it for help debugging code
    '''
    def _get_client(
        self
    ):
        return CodeChatModel.from_pretrained(self.model_name)
    
    def ask(
        self,
        question: str,
        context: Optional[str] = None,
        history: Optional[List] = [],
        max_new_tokens: Optional[int] = 500,
        temperature: Optional[float] = None,
        stop_sequences: Optional[List[str]] = None,
        prefix="",
        **kwargs
    ) -> Dict:
        assert self.client != None
        
        chat = None
        if FULL_PROMPT in kwargs.keys():
            full_prompt = kwargs.pop(FULL_PROMPT)

            # make sure the full prompt param is an even list
            assert len(full_prompt) % 2 != 0
            
            # pull out the last message
            last_msg = full_prompt[-1]
            if isinstance(last_msg, dict):
                question = last_msg.get('content')
                history = full_prompt[:-1]
            elif isinstance(last_msg, str):
                question = last_msg
                history = []
            else:
                raise TypeError("Unable to extract the question from full prompt list")
            
        
        # convert history to ChatMessage class 
        try:
            history = [ChatMessage(author = msg.get('author', msg['role']), content= msg['content']) for msg in history]
        except KeyError:
            raise KeyError("Unable to determine author of the message. No 'author' or 'role' provided.")
        
        # begin the convo
        chat = self.client.start_chat(
            context=context,
            message_history = history
        )
              
        # convert ask inputs to vertex ai params
        parameters = {
            "message": question,
            "temperature": temperature,  # Temperature controls the degree of randomness in token selection.
            "max_output_tokens": max_new_tokens,  # Token limit determines the maximum amount of text output.
            "stop_sequences": stop_sequences,
        }
        
        responses = chat.send_message_streaming(
            **parameters
        )
        
        final_response = ''
        for response in responses:
            final_response += response.text
            print(prefix + response.text, end ='')
            
        output = ModelEngineResponse(response=final_response)
        
        return output.to_dict()
    