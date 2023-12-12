from typing import (
    Union, 
    List, 
    Dict, 
    Generator,
    Optional,
    Tuple
)

import math
import functools
import time
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from tqdm.auto import tqdm

# CFG classes/functions
from ..clients.client_initializer import google_initializer
from .abstract_embedder import AbstractEmbedder
from ..constants import (
    ModelEngineResponseKeys
)

# Load the "Vertex AI Embeddings for Text" model
from vertexai.preview.language_models import TextEmbeddingModel



class VertexAiEmbedder(AbstractEmbedder):
    
    def __init__(
        self,
        model_name:str,
        region:str,
        service_account_credentials:Dict = None,
        service_account_key_file:str = None,
        project:str = None,
        **kwargs
    ):
        # initialize the google aiplatform connection
        google_initializer(
            region=region,
            service_account_credentials=service_account_credentials,
            service_account_key_file=service_account_key_file,
            project=project
        )
        
        # register the some of the model details with the abstract embedder class
        super().__init__(
            model_name=model_name,
            **kwargs,
        )
        
        self.client = TextEmbeddingModel.from_pretrained(self.model_name)
        self.tokenizer = self.client
        
    def embeddings(
        self,
        list_to_embed:List[str], 
        prefix:str = ""
    ) -> Dict[str, Union[str, int]]:

        # make the call
        is_successful, embedded_list = self._encode_text_to_embedding_batched(
            sentences=list_to_embed
        )
        
        #if is_successful
        
        if not isinstance(embedded_list, list):
            embedded_list = embedded_list.tolist()
            
        if len(list_to_embed) == 1:
            embedded_list = [embedded_list]
        
        return {
            ModelEngineResponseKeys.RESPONSE: embedded_list,
            ModelEngineResponseKeys.NUMBER_OF_TOKENS_IN_PROMPT: self.tokenizer.count_tokens(list_to_embed).total_tokens,
            ModelEngineResponseKeys.NUMBER_OF_TOKENS_IN_RESPONSE: 0
        }
        
    def _encode_texts_to_embeddings(self, sentences: List[str]) -> List[Optional[List[float]]]:
        # https://colab.research.google.com/github/GoogleCloudPlatform/vertex-ai-samples/blob/main/notebooks/official/matching_engine/sdk_matching_engine_create_stack_overflow_embeddings_vertex.ipynb#scrollTo=a0370bd840d2
        try:
            embeddings = self.client.get_embeddings(sentences)
            return [embedding.values for embedding in embeddings]
        except Exception:
            return [None for _ in range(len(sentences))]
        
    def _generate_batches(
        sentences: List[str], batch_size: int
    ) -> Generator[List[str], None, None]:
        '''
        According to the documentation, each request can handle up to 5 text instances. 
        Therefore, this method splits sentences into batches of 5 before sending to the embedding API.
        '''
        for i in range(0, len(sentences), batch_size):
            yield sentences[i : i + batch_size]
            
    def _encode_text_to_embedding_batched(
        self, 
        sentences: List[str], 
        api_calls_per_second: int = 10, 
        batch_size: int = 5
    ) -> Tuple[List[bool], np.ndarray]:
        '''
        This method calls generate_batches to handle batching and then calls the embedding API via encode_texts_to_embeddings. 
        It also handles rate-limiting using time.sleep.
        For production use cases, you would want a more sophisticated rate-limiting mechanism that takes retries into account.
        '''
        embeddings_list: List[List[float]] = []

        # Prepare the batches using a generator
        batches = VertexAiEmbedder._generate_batches(sentences, batch_size)

        seconds_per_job = 1 / api_calls_per_second

        with ThreadPoolExecutor() as executor:
            futures = []
            for batch in tqdm(
                batches, total=math.ceil(len(sentences) / batch_size), position=0
            ):
                futures.append(
                    executor.submit(functools.partial(self._encode_texts_to_embeddings), batch)
                )
                time.sleep(seconds_per_job)

            for future in futures:
                embeddings_list.extend(future.result())

        is_successful = [
            embedding is not None for sentence, embedding in zip(sentences, embeddings_list)
        ]
        embeddings_list_successful = np.squeeze(
            np.stack([embedding for embedding in embeddings_list if embedding is not None])
        )
        return is_successful, embeddings_list_successful
        
    def _get_tokenizer(self, init_args):
        return None
    
    def ask(
        self, 
        *args, 
        **kwargs
    ) -> Dict:
        response = 'This model does not support text generation.'
        output_payload = {
            ModelEngineResponseKeys.RESPONSE:response,
            ModelEngineResponseKeys.NUMBER_OF_TOKENS_IN_PROMPT: 0,
            ModelEngineResponseKeys.NUMBER_OF_TOKENS_IN_RESPONSE: self.tokenizer.count_tokens([response]).total_tokens
        }
        return output_payload