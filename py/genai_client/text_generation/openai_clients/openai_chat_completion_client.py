from typing import Optional, Union, List, Dict, Any, Tuple
import numpy as np

from .abstract_openai_client import AbstractOpenAiClient
from ...constants import (
    FULL_PROMPT,
    ModelEngineResponse
)

class OpenAiChatCompletion(AbstractOpenAiClient):
        
    def ask(
        self,
        question:str = None, 
        context:str = None, 
        template_name:str = None, 
        history:List[Dict] = None, 
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
            
        # first we determine the type of completion, since this determines how we
        # structure the payload
        final_query = ""

        # the list to construct the payload from
        message_payload = []

        if FULL_PROMPT not in kwargs.keys():
            # if the user provided context, use that. Otherwise, try to get it from the template
            message_payload = self._process_chat_completion(
                question = question,
                context = context,
                history = history,
                template_name = template_name,
                fill_variables = kwargs
            )

        else:
            message_payload = self._process_full_prompt(
                kwargs.pop(FULL_PROMPT)
            )

        num_input_tokens = self.tokenizer.count_tokens(input = message_payload)
        #self._check_input_token_limits(num_input_tokens)

        # check to see if we need to adjust the prompt or max_new_tokens
        prompt, kwargs['max_tokens'], model_engine_response = self._check_token_limits(
            prompt_payload = message_payload,
            max_new_tokens = max_new_tokens
        )
        
        # add the message payload as a kwarg
        kwargs['messages'] = message_payload
        
        print(kwargs)
        model_engine_response.response = self._inference_call(prefix = prefix, **kwargs)
        model_engine_response.response_tokens = self.tokenizer.count_tokens(model_engine_response.response)
        
        return model_engine_response.to_dict()
        
        
    def _inference_call(
        self, 
        prefix:str, 
        **kwargs
    ) -> str:
        final_query = ""
        
        kwargs['stream'] = kwargs.get('stream', True)
        stream = self.client.chat.completions.create(
            model=self.model_name, 
            **kwargs
        )
        
        if kwargs['stream']:
            for chunk in stream:
                if chunk.choices and (len(chunk.choices) > 0):
                    response = chunk.choices[0].delta.content
                    if response != None:
                        final_query += response
                        print(prefix + response, end ='')
        else:
            final_query = stream.choices[0].message.content

        return final_query
    
    def _process_chat_completion(
        self, 
        question:str,
        context:str,
        history:List[Dict],
        template_name:str,
        fill_variables:Dict
    ) -> List[Dict]:
        # the list to construct the payload from
        message_payload = []
        
        # if the user provided context, use that. Otherwise, try to get it from the template
        mapping = {"question": question} | fill_variables
        if context is not None and template_name == None:
            if isinstance(context, str):
                context = self.fill_context(context, **mapping)[0]
                message_payload.append({"role": "system", "content": context})
        elif context != None and template_name != None:
            mapping.update({"context":context})
            context = self.fill_template(template_name=template_name, **mapping)[0]
            message_payload.append({"role": "system", "content": context})
        else:
            if template_name != None:
                possibleContent = self.fill_template(template_name=template_name, **mapping)[0]
                if possibleContent != None:
                    message_payload.append({"role": "system", "content": possibleContent})

        # if history was added, then add it to the payload. Currently history is being like OpenAI prompts
        if history is not None:
            message_payload.extend(history)

        # add the new question to the payload
        if (question != None and len(question) > 0):
            message_payload.append({"role": "user", "content": question})

        return message_payload

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
        prompt_payload:List, 
        max_new_tokens:int
    ) -> Tuple[str, int, ModelEngineResponse]:
        '''
        The method is used to truncate the the number of tokens in the prompt and adjust the `max_new_tokens` so that the text generation does not fail.
        Instead we rather will send back a flag indicating adjustments have
        '''
        model_engine_response = ModelEngineResponse()
        warnings = []

        # use the models tokenizer to get the number of tokens in the prompt
        num_token_in_prompt = self.tokenizer.count_tokens(prompt_payload)
        #num_token_in_prompt = len(prompt_tokens)
        max_prompt_tokens = self.tokenizer.get_max_input_token_length()

        if (max_prompt_tokens != None):
            max_tokens = max_prompt_tokens
        else:
            max_tokens = self.tokenizer.get_max_token_length()
        
        # perform the checks using max_tokens
        if (num_token_in_prompt > max_tokens):                
            token_counter = 0
            for i, message in enumerate(prompt_payload):
                num_message_tokens = self.tokenizer.count_tokens(message)
                token_counter += num_message_tokens
                                    
                if (token_counter > max_tokens):
                    # calculate how many tokens we can take from this message                        
                    num_tokens_to_remove = token_counter - max_tokens
                    
                    message_tokens = self.tokenizer.get_tokens(message['content'])
                    message_tokens = message_tokens[:len(message_tokens) - num_tokens_to_remove] 
                    
                    prompt_payload[i]['content'] = ''.join(message_tokens)
                    prompt_payload = prompt_payload[:i+1]
                    warnings.append(f'The prompt was truncated to:\n {prompt_payload}')
                    num_token_in_prompt = self.tokenizer.count_tokens(prompt_payload)
                    break

        # now we also need to make sure the max_new_tokens passed in is adjusted
        if (max_new_tokens > (max_tokens - num_token_in_prompt)):
            max_new_tokens = max_new_tokens + ((max_tokens - num_token_in_prompt) - max_new_tokens)
            warnings.append(f'max_new_tokens was changed to: {max_new_tokens}')

        model_engine_response.prompt_tokens = num_token_in_prompt
        if (len(warnings) > 0):
            model_engine_response.warning = '\\n\\n'.join(warnings)

        return prompt_payload, int(max_new_tokens), model_engine_response