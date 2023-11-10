
import openai
from .openai_chat_completion_client import OpenAiChatCompletion
import tiktoken

class AzureOpenAiChatCompletion(OpenAiChatCompletion):
  
    def __init__(
        self, 
        endpoint:str, 
        model_name:str = None, 
        api_key:str = "EMPTY", 
        api_version='2023-07-01-preview', 
        **kwargs
    ):
        assert endpoint != None

        super().__init__(
            api_key = api_key,
            model_name = model_name,
            **kwargs
        )

        openai.api_base = endpoint
        openai.api_type ='azure'
        openai.api_version = api_version

    def _inference_call(self, prefix:str, kwargs):
        final_query = ""
        responses = openai.ChatCompletion.create(
            engine=self.model_name, 
            stream = True, 
            **kwargs
        )
        for chunk in responses:
            if chunk.choices:
                if len(chunk.choices) > 0:
                    response = chunk.choices[0].get('delta', {}).get('content')
                    if response != None:
                        final_query += response
                        print(prefix+response, end ='')

        return final_query

    def _get_tokenizer(self, init_args):
        try:
            tiktoken.encoding_for_model(self.model_name)
            return super()._get_tokenizer(init_args)
        except:
            init_args['tokenizer_name'] = init_args.pop(
                'openai_model_name', 
                "gpt-3.5-turbo"
            )
            return super()._get_tokenizer(init_args)
