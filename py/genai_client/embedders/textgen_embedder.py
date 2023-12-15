from typing import List
import requests

from .local_embedder import LocalEmbedder
from ..tokenizers import HuggingfaceTokenizer
from ..constants import (
    MAX_TOKENS,
    MAX_INPUT_TOKENS
)

class TextEmbeddingsInference(LocalEmbedder):
    
    BATCH_SIZE = 'batch_size'
    
    def __init__(
        self,
        endpoint:str,
        model_name:str,
        **kwargs
    ) -> None:
        self.model_name = model_name
        assert self.model_name != None

        self.batch_size = kwargs.pop(
            TextEmbeddingsInference.BATCH_SIZE, 
            32
        )
        
        self.embedder = self.get_embedder(
            endpoint = endpoint
        )

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
                
    def get_embedder(
        self,
        endpoint:str
    ):
        class TextGenEmbedder():
            '''
            This is a wrapper class to make api calls using SentenceTransformers method
            '''
            def __init__(self, endpoint, batch_size):
                self.endpoint = endpoint
                self.batch_size = batch_size
            
            def encode(self, sentences: List[str], **kwargs):                
                
                # Split a list into sublists of a specified size.
                sentence_sublists = [sentences[i:i + self.batch_size] for i in range(0, len(sentences), self.batch_size)]
                
                # hold the cumulative embeddings from batches
                embeddings_list = []
                for sublist in sentence_sublists:
                    
                    response = requests.post(self.endpoint, json={"inputs": sublist, "truncate":True})
                    if response.status_code != 200:
                        raise RuntimeError(
                            f"Error {response.status_code}: {response.text}"
                        )
                        
                    result = response.json()
                    if isinstance(result, list):
                        embeddings_list.extend(result)
                    elif list(result.keys())[0] == "error":
                        raise RuntimeError(
                            "Unable to get vector embeddings."
                        )
                        
                return embeddings_list

        return TextGenEmbedder(endpoint = endpoint, batch_size = self.batch_size)
