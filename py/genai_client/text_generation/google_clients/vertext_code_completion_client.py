from typing import Dict, Optional, List
from vertexai.language_models import CodeGenerationModel

from .abstract_vertex_textgen_client import AbstractVertextAiTextGeneration
from ...constants import (
    ModelEngineResponse,
    FULL_PROMPT
)

class VertexCodeCompletionClient(AbstractVertextAiTextGeneration):
    
    def _get_client(
        self
    ):
        return CodeGenerationModel.from_pretrained(self.model_name)
    
    def ask(
        self,
        question: str,
        context: Optional[str] = None,
        history: Optional[List] = [],
        max_new_tokens: Optional[int] = 500,
        temperature: Optional[float] = None,
        prefix="",
        **kwargs
    ):
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
            
        # build the message chain
        prompt = ''
        author = ''
        try:
            prompt +=  context or ''
            for msg in history:
                author = msg.get('author', msg['role']) + ':\n'
                content= msg['content']
                
                prompt += author
                prompt += content
                
                prompt += '\n\n'
                
            prompt += author
            prompt += question
        except KeyError:
            raise KeyError("Unable to determine author of the message. No 'author' or 'role' provided.")
              
        # convert ask inputs to vertex ai params
        parameters = {
            "prefix": prompt,
            "temperature": temperature,  # Temperature controls the degree of randomness in token selection.
            "max_output_tokens": max_new_tokens,  # Token limit determines the maximum amount of text output.
        }
        
        print(parameters)
        
        responses = self.client.predict_streaming(
            **parameters
        )
        
        final_response = ''
        for response in responses:
            final_response += response.text
            print(prefix + response.text, end ='')
            
        output = ModelEngineResponse(response=final_response)
        
        return output.to_dict()