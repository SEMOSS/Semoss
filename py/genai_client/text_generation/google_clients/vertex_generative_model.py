from typing import Dict, Optional, List
from vertexai.preview.generative_models import GenerativeModel, Content, Part

from .abstract_vertex_textgen_client import AbstractVertextAiTextGeneration
from ...constants import (
    ModelEngineResponse,
    FULL_PROMPT
)


class VertexGenerativeModelClient(AbstractVertextAiTextGeneration):
    '''
    Vertex AI class to use GenerativeModel and more specifically Gemini
    '''
    def _get_client(
        self
    ):
        return GenerativeModel(self.model_name)
    
    def ask(
        self,
        question: str = None,
        context: Optional[str] = None,
        history: Optional[List] = [],
        max_new_tokens: Optional[int] = 500,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        top_k: Optional[int] = None,
        candidate_count: Optional[int] = None,
        stop_sequences: Optional[List[str]] = None,
        prefix="",
        stream: Optional[bool] = True,
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
            
        
        # convert history to Content class
        history = []
        if context != None:
            history.append(Content(role = 'user', parts=[Part.from_text(context)]))
            
        try:
            history.extend(
                [Content(role = msg.get('author', msg['role']), parts=[Part.from_text(msg['content'])]) for msg in history]
            )
        except KeyError:
            raise KeyError("Unable to determine author of the message. No 'author' or 'role' provided.")
        
        # begin the convo
        chat = self.client.start_chat(
            history = history
        )
              
        # convert ask inputs to vertex ai params
        generation_config = {
            "temperature": temperature,  # Temperature controls the degree of randomness in token selection.
            "max_output_tokens": max_new_tokens,  # Token limit determines the maximum amount of text output.
            "top_p": top_p,  # Tokens are selected from most probable to least until the sum of their probabilities equals the top_p value.
            "top_k": top_k,  # A top_k of 1 means the selected token is the most probable among all tokens.
            "stop_sequences": stop_sequences,
            "candidate_count": candidate_count,   
        }
        
        if stream:
            responses = chat.send_message(
                content = question,
                generation_config=generation_config,
                stream=stream,
            )
            
            model_engine_response = ModelEngineResponse(response='')
            for response in responses:
                model_engine_response.response += response.text
                print(prefix + response.text, end ='')
                
                usage_metadata = response._raw_response.usage_metadata
                if usage_metadata:
                    model_engine_response.prompt_tokens = usage_metadata.prompt_token_count
                    model_engine_response.response_tokens = usage_metadata.candidates_token_count               
            
            return model_engine_response.to_dict()
        else:
            response = chat.send_message(
                content = question,
                generation_config=generation_config,
                stream=stream,
            )
            
            model_engine_response = ModelEngineResponse(
                response=response.text,
                prompt_tokens=response._raw_response.usage_metadata.prompt_token_count,
                response_tokens=response._raw_response.usage_metadata.candidates_token_count,
            )
            
            return model_engine_response.to_dict()