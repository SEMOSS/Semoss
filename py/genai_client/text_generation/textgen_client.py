
from typing import Optional, Union, List, Dict, Any, Tuple
from text_generation import Client
import inspect

from .base_client import BaseClient
from ..tokenizers import HuggingfaceTokenizer
from ..constants import (
    ModelEngineResponse,
    MAX_TOKENS,
    MAX_INPUT_TOKENS,
    FULL_PROMPT
)

class TextGenClient(BaseClient):
    params = list(inspect.signature(Client.generate).parameters.keys())[1:]

    def __init__(
        self, 
        endpoint:str,
        template: Optional[Union[Dict, str]] = None, 
        model_name: Optional[str] = None,
        template_name: Optional[str] = None, 
        stop_sequences: Optional[List[str]] = [], 
        timeout: Optional[int] = 30,
        **kwargs
    ):
        assert endpoint is not None

        super().__init__(
            template = template, 
            template_name = template_name
        )

        self.kwargs = kwargs
        self.client = Client(endpoint)
        self.client.timeout = timeout
        self.model_name = model_name

        self.tokenizer = HuggingfaceTokenizer(
            encoder_name = model_name, 
            max_tokens = kwargs.pop(
                MAX_TOKENS, 
                None
            ),
            max_input_tokens = kwargs.pop(
                MAX_INPUT_TOKENS, 
                None
            )
        )

        self.stop_sequences = stop_sequences or [self.tokenizer.tokenizer.eos_token]

    
    def ask(
        self, 
        question:str = None, 
        context: Optional[str] = None,
        history: Optional[List] = [],
        template_name: Optional[str] = None,
        max_new_tokens: Optional[int] = 1000,
        prefix: Optional[str] = "",
        do_sample: bool = False,
        repetition_penalty: Optional[float] = None,
        return_full_text: bool = False,
        seed: Optional[int] = None,
        stop_sequences: Optional[List[str]] = None,
        temperature: Optional[float] = None,
        top_k: Optional[int] = None,
        top_p: Optional[float] = None,
        truncate: Optional[int] = None,
        typical_p: Optional[float] = None,
        watermark: bool = False,
        **kwargs
    ) -> str:
        # start the prompt as an empty string
        prompt = ""
        
        if FULL_PROMPT not in kwargs.keys():
            content = []
        
            if template_name == None:
                template_name = self.template_name

            ## template fill method
            content, sub_occured = self._fill_params(
                content = content,
                question = question,
                context = context,
                template_name = template_name,
                fill_arguments = kwargs,
                history = history
            )

            content = self._process_history(
                content = content,
                sub_occured = sub_occured,
                question = question,
                history = history
            )

            # join all the inputs into a single string
            prompt = "".join(content)
        else:
            prompt = self._process_full_prompt(
                kwargs.pop(FULL_PROMPT)
            )
    
        # check to see if we need to adjust the prompt or max_new_tokens
        prompt, max_new_tokens, model_engine_response = self._check_token_limits(
            prompt_payload = prompt,
            max_new_tokens = max_new_tokens
        )
    
        # convert ask inputs to text gen params
        parameters = {
            "prompt": prompt,
            "do_sample": do_sample,
            "max_new_tokens": max_new_tokens,
            'repetition_penalty': repetition_penalty,
            "return_full_text": return_full_text,
            "seed": seed,
            "stop_sequences": stop_sequences if stop_sequences is not None else (stop_sequences := self.stop_sequences),
            "temperature": temperature,
            "top_k": top_k,
            "top_p": top_p,
            "truncate":truncate,
            "typical_p": typical_p,
            "watermark": watermark
        }
        
        # check whether to include logprobs in the response
        include_logprobs = kwargs.pop(
            'include_logprobs', 
            False
        )
        
        # TODO this is ugly, NEED to updates
        # check whether to stream the response or not
        stream = kwargs.get('stream', True)
        if (not stream):
            decoder_input_details = kwargs.get('decoder_input_details', False)
            parameters['decoder_input_details'] = decoder_input_details
            # configure the generator request object
            response = self.client.generate(
                **parameters
            )
            
            model_engine_response.response = response.generated_text
            
            response_tokens = []
            response_logprobs = []
            
            if decoder_input_details:
                for token in response.details.prefill:
                    if token.logprob is None:
                        continue
                    response_tokens.append(token.text)
                    response_logprobs.append(token.logprob)
                    
            for response_token in response.details.tokens:
                token_text = response_token.text
    
                if token_text in stop_sequences:
                    break
                
                response_tokens.append(token_text)
                response_logprobs.append(response_token.logprob)
                
            model_engine_response.response_tokens = len(response_tokens)
            if include_logprobs:
                model_engine_response.tokens = response_tokens
                model_engine_response.logprobs = response_logprobs

        else:
            # configure the generator request object
            responses = self.client.generate_stream(
                **parameters
            )
            
            response_tokens = []
            response_logprobs = []
            for response in responses:
                response_token = response.token
                token_text = response_token.text
                
                if token_text in stop_sequences:
                    break
                
                # print out tokens so it can be picked up by partial endpoint
                print(prefix+token_text,end='')
                
                response_tokens.append(token_text)
                response_logprobs.append(response_token.logprob)
                
            if include_logprobs:
                model_engine_response.tokens = response_tokens
                model_engine_response.logprobs = response_logprobs

            model_engine_response.response = ''.join(response_tokens) 
            model_engine_response.response_tokens = len(response_tokens)

        return model_engine_response.to_dict()

    def _process_history(
        self, 
        content:List[str],
        sub_occured:bool,
        question:str,
        history:List[Dict]
    ) -> List[str]:
        ## Here we need to check if substitution occured, this determines how we might append history ##
        if sub_occured == False:
            # if substitution did not occur, they are only passing a context string like 'Your are a helpful Assistant'
            # Therefore, we want to append the history and and question/prompt directly after
            # Add history if one is provided
            for statement in history:
                content.append(statement['role'])
                content.append(':')
                content.append('\n\n')
                content.append(statement['content'])
                content.append('\n\n')
            
            # append the user asked question to the content
            content.append('### Instruction:\n\n')
            content.append(question)
            content.append('\n\n')
            content.append('### Response:')

        else: 
            # Currently there is no template where only the context is substituted. At that point they should pass the context in as an argument.
            # Therefore, we assume that the question has been substitued and it needs to go last in the prompt string
            # As a result, we place the history first
            histContent = []
            for statement in history:
                histContent.append(statement['role'])
                histContent.append(':')
                histContent.append(statement['content'])
                histContent.append('\n')
            content = histContent + content

        return content

    def _fill_params(
        self, 
        content:List[str],
        question:str,
        context:Union[str, None],
        template_name:Union[str, None],
        fill_arguments:Dict,
        history:List[Dict]
    ) -> List[str]:
        ## Here we need to check if substitution occured, this determines how we might append history ##

        # attempt to pull in the context
        sub_occured = False
        mapping = {"question": question} | fill_arguments
    
        if context != None and template_name == None:
            # Case 1 Description: The user provided context string in the ask method. We need to check if this string might be a place holder
            # where the user wants to fill the context the provided as a template
            if isinstance(context, str):
                context, sub_occured = self.fill_context(context, **mapping)
                content = [context,'\n']
        elif context != None and template_name != None:
            # Case 2 Description: The user provided context string in the ask method. This context is intended to be string substituted 
            # into a template.
            # String substitution occurs if either 'question' or 'context' is substituted
            mapping.update({"context":context})
            context, sub_occured = self.fill_template(template_name=template_name, **mapping)
            content = [context,'\n']
        else:
            # Case 3 Description: There was no context provided, however the user might want to fill the question into a pre-defined template
            if template_name != None:
                possibleContent, sub_occured = self.fill_template(template_name=template_name, **mapping)
                if possibleContent != None:
                    content = [possibleContent,'\n']

        return content, sub_occured

    def _process_full_prompt(
        self, 
        full_prompt: Union[List, str]
    ) -> str:
        prompt = ""
        if isinstance(full_prompt, str):
            prompt = full_prompt
        elif isinstance(full_prompt, list):
            listOfDicts = set([isinstance(x, dict) for x in full_prompt]) == {True}
            if (listOfDicts == False):
                raise ValueError("The provided payload is not valid")
            # now we have to check the key value pairs are valid
            all_keys_set = {key for d in full_prompt for key in d.keys()}
            validOpenAiDictKey = sorted(all_keys_set) == ['content', 'role']
            if (validOpenAiDictKey == False):
                # this is because we are mimicing the OpenAI message payload structure
                raise ValueError("There are invalid dictionary keys")
            # add it the message payload
            for roleContent in full_prompt:
                role = roleContent['role'] if roleContent['role'].endswith('\n') else roleContent['role'] + '\n\n'
                
                prompt += role
                
                message_content = roleContent['content'] if roleContent['content'].endswith('\n') else roleContent['content'] + '\n\n'
                
                prompt += message_content
        else:
            raise ValueError("Please either pass a string containing the full prompt or a sorted list that contains dictionaries with only 'role' and 'content' keys.\nPlease note, the values associated with 'role' and 'content' should contain the appropriate character to build a prompt string.")
        
        return prompt

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