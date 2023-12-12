from typing import Optional, Union, List, Dict, Any, Tuple
import numpy as np

from .abstract_openai_client import AbstractOpenAiClient
from ...constants import (
    ModelEngineResponse
)

class OpenAiCompletion(AbstractOpenAiClient):
            
    def ask(
        self,
        question:str = None, 
        context:str = None, 
        template_name:str = None, 
        history:List[Dict] = None, 
        page_size = 100,
        max_new_tokens = 1000,
        prefix="", 
        **kwargs
    ) -> Dict:
        
        if ('repetition_penalty' in kwargs.keys()):
            kwargs['frequency_penalty'] = float(kwargs.pop('repetition_penalty'))
        if ('stop_sequences' in kwargs.keys()):
            kwargs['stop'] = kwargs.pop('stop_sequences')
            
        if template_name == None:
            template_name = self.template_name
                    
        prompt = None
        mapping = {"question": question}

        # default the template name based on model
        if template_name is None and context is None:
            template_name = f"{self.model_name}.default.nocontext"
        elif context is not None:
            template_name = f"{self.model_name}.default.context" 

        # generate the prompt
        if context is not None:
            mapping = {"question": question, "context":context}
        # merge kwargs
        mapping = mapping | kwargs

        prompt = super().fill_template(template_name=template_name, **mapping)[0]
        
        
        if prompt is None:
            prompt = question

        # Add history if one is provided
        if history is not None:
            prompt = f"{prompt} {history}"
            
        print(prompt)
        # check to see if we need to adjust the prompt or max_new_tokens
        prompt, kwargs['max_tokens'], model_engine_response = self._check_token_limits(
            prompt_payload = prompt,
            max_new_tokens = max_new_tokens
        )

        print(prompt)
        model_engine_response.response = self._inference_call(
            prompt = prompt,
            prefix = prefix, 
            kwargs = kwargs
        )
    
        model_engine_response.response_tokens = self.tokenizer.count_tokens(
            model_engine_response.response
        )

        return model_engine_response.to_dict()
        
    def _inference_call(
        self,
        prompt,
        prefix:str, 
        kwargs
    ) -> str:
        final_query = prompt + " "
        finish = False

        kwargs['stream'] = kwargs.get('stream', True)
        stream = self.client.completions.create(
            model=self.model_name, 
            prompt=prompt, 
            **kwargs
        )
        if kwargs['stream']:
            print(prefix+final_query, end ='')
            for chunk in stream:
                response = chunk.choices[0].text
                if response != None:
                    final_query += response
                    print(prefix+response, end ='')
        else:
            final_query = stream.choices[0].text
            
        return final_query

    def _process_completion(self):
        pass

    def _process_full_prompt(
        self, 
        full_prompt: List
    ) -> List[Dict]:
        if isinstance(full_prompt, list):
            listOfDicts = set([isinstance(x, dict) for x in full_prompt]) == {True}
            if (listOfDicts == False):
                raise ValueError("The provided payload is not valid")
            
            # now we have to check the key value pairs are valid
            all_keys_set = {key for d in full_prompt for key in d.keys()}
            validOpenAiDictKey = sorted(all_keys_set) == ['content', 'role']
            if (validOpenAiDictKey == False):
                raise ValueError("There are invalid OpenAI dictionary keys")
            # add it the message payload
            return full_prompt
        else:
            raise TypeError("Please make sure the full prompt for OpenAI Chat-Completion is a list")


    def _check_token_limits(
        self, 
        prompt_payload:str, 
        max_new_tokens:int
        ) -> Tuple[str, int, ModelEngineResponse]:
        '''
        The method is used to truncate the the number of tokens in the prompt and adjust the `max_new_tokens` so that the text generation does not fail.
        Instead we rather will send back a flag indicating adjustments have
        '''
        model_engine_response = ModelEngineResponse()
        warnings = []
        # use the models tokenizer to get the number of tokens in the prompt
        prompt_tokens = self.tokenizer.get_tokens_ids(prompt_payload)
        num_token_in_prompt = len(prompt_tokens)
        max_prompt_tokens = self.tokenizer.get_max_input_token_length()
        max_tokens = self.tokenizer.get_max_token_length()
        
        if (max_prompt_tokens != None):
            # perform the checks using max_input_tokens
            if (num_token_in_prompt > max_prompt_tokens):
                # we need to make the payload smaller
                prompt_tokens = prompt_tokens[:max_prompt_tokens-1]
                num_token_in_prompt = max_prompt_tokens
                prompt_payload = self.tokenizer.decode_token_ids(prompt_tokens)
                warnings.append(f'The prompt was truncated to:\n {prompt_payload}')
        
            # make sure the combination of input tokens and max_new_tokens is less that total tokens(max_tokens)
            if (max_new_tokens > (max_tokens - max_prompt_tokens)):
                max_new_tokens = max_new_tokens + ((max_tokens - max_prompt_tokens) - max_new_tokens)
                warnings.append(f'max_new_tokens was changed to: {max_new_tokens}')

        else:
            # perform the checks using max_tokens
            if (num_token_in_prompt > max_tokens):
                prompt_tokens = prompt_tokens[:max_tokens-1]
                num_token_in_prompt = max_tokens
                prompt_payload = self.tokenizer.decode_token_ids(prompt_tokens)
                warnings.append(f'The prompt was truncated to:\n {prompt_payload}')

            # now we also need to make sure the max_new_tokens passed in is adjusted
            if (max_new_tokens > (max_tokens - num_token_in_prompt)):
                max_new_tokens = max_new_tokens + ((max_tokens - num_token_in_prompt) - max_new_tokens)
                warnings.append(f'max_new_tokens was changed to: {max_new_tokens}')

        model_engine_response.prompt_tokens = num_token_in_prompt
        if (len(warnings) > 0):
            model_engine_response.warning = '\\n\\n'.join(warnings)

        return prompt_payload, int(max_new_tokens), model_engine_response