from typing import Union, List, Dict, Any
import openai
from ..tokenizers import OpenAiTokenizer

class OpenAiEmbedder():
    
    def __init__(
        self,
        model_name:str, 
        api_key:str,
        **kwargs
    ):
        self.model_name = model_name
        openai.api_key = api_key
        
        self.tokenizer = OpenAiTokenizer(
            encoder_name = model_name, 
            max_tokens = kwargs.pop(
                'max_tokens', 
                None
            )
        )
        
    def ask(
        self, 
        *args: Any, 
        **kwargs
    ) -> str:
        response = 'This model does not support text generation.'
        output_payload = {
            'response':response,
            'numberOfTokensInPrompt': 0,
            'numberOfTokensInResponse': self.tokenizer.count_tokens(response)
        }
        return output_payload
    
    def embeddings(
        self,
        list_to_embed:List[str], 
        prefix:str = ""
    ) -> Dict[str, Union[str, int]]:

        # Make sure a list was passed in so we can proceed with the logic below
        assert isinstance(list_to_embed, list)
        
        total_num_of_tokens = self.tokenizer.count_tokens(''.join(list_to_embed))

        embedded_list = [] # This is the final list that will be sent back
        if (self.tokenizer.get_max_token_length() != None):
            if (total_num_of_tokens <= self.tokenizer.get_max_token_length()):
                # The entire list can be sent as a single batch
                print(prefix + "Waiting for OpenAI to process all chunks")
                single_batch_results = self._make_openai_embedding_call(
                    list_to_embed
                )

                embedded_list = [vector['embedding'] for vector in single_batch_results['data']]

            else:
                # Split the list into batches
                current_batch = []
                current_token_count = 0
                batches = []

                for chunk in list_to_embed:
                    chunk_token_count = self.tokenizer.count_tokens(chunk)

                    if current_token_count + chunk_token_count <= self.tokenizer.get_max_token_length():
                        current_batch.append(chunk)
                        current_token_count += chunk_token_count
                    else:
                        # Start a new batch
                        batches.append(current_batch)
                        current_batch = [chunk]
                        current_token_count = chunk_token_count

                if len(current_batch) > 0:
                    batches.append(current_batch)

                print(prefix + "Multiple batches have to be sent to OpenAI")
                number_of_batches = len(batches)
                for i in range(number_of_batches):
                    batch_results = self._make_openai_embedding_call(
                        batches[i]
                    )
                    print(prefix + "Completed Embedding " + str(i+1) + "/" + str(number_of_batches) + " Batches")
                    embedded_list.extend([vector['embedding'] for vector in batch_results['data']])
            
        else:
            # We have no choice but to try send the entire thing
            print(prefix + "Waiting for OpenAI to process all chunks")
            single_batch_results = self._make_openai_embedding_call(
                list_to_embed
            )

            embedded_list = [vector['embedding'] for vector in single_batch_results['data']]
            
        print(prefix + "Sending Embeddings back from Model Engine")
        
        return {
            'response': embedded_list,
            'numberOfTokensInPrompt': total_num_of_tokens,
            'numberOfTokensInResponse': 0
        }
        
    def _make_openai_embedding_call(self, list_of_text:List[str]):
        '''this method is responsible for making the openai embeddings call. it takes in a single'''
        return openai.Embedding.create(input = list_of_text, model=self.model_name)