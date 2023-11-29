import requests
from .local_embedder import LocalEmbedder
from ..tokenizers import HuggingfaceTokenizer

class TextEmbeddingsInference(LocalEmbedder):
    
    def __init__(
        self,
        endpoint:str,
        model_name:str,
        **kwargs
    ) -> None:
        self.model_name = model_name
        assert self.model_name != None

        self.embedder = self.get_embedder(
            endpoint = endpoint
        )

        self.tokenizer = HuggingfaceTokenizer(
            encoder_name = model_name, 
            max_tokens = kwargs.pop(
                'max_tokens', 
                None
            ),
            max_input_tokens = kwargs.pop(
                'max_input_tokens', 
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
            def __init__(self, endpoint):
                self.endpoint = endpoint
            
            def encode(self, sentences, **kwargs):
                response = requests.post(self.endpoint, json={"inputs": sentences})
                result = response.json()
                if isinstance(result, list):
                    return result
                elif list(result.keys())[0] == "error":
                    raise RuntimeError(
                        "The model is currently loading, please re-run the query."
                    )

        return TextGenEmbedder(endpoint = endpoint)
