from typing import List
import boto3
import json
import requests

from .abstract_embedder import AbstractEmbedder
from ..constants import (    
    EmbeddingsModelEngineResponse
)

class awsTitanTextEmbedder(AbstractEmbedder):
    def __init__(
        self,    
        model_name:str,       
        access_key=None,
        secret_key=None,
        region=None,       
        **kwargs,
    )-> None:        
        self.kwargs = kwargs        
        self.model_name = model_name      
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.modelId = model_name
        self.service_name = "bedrock-runtime"  

        super().__init__(
            model_name=self.model_name,
            **kwargs, 
        )    
     
        
    def embeddings_call(self, strings_to_embed:List[str], prefix:str = "") -> EmbeddingsModelEngineResponse:  
        embeddings_list = []
        embeddings = []       

        for text in strings_to_embed:
            json_obj = {"inputText": text}
            request = json.dumps(json_obj)
        
            try:   
                client = boto3.client(
                        service_name="bedrock-runtime",
                        aws_access_key_id=self.access_key,
                        aws_secret_access_key=self.secret_key,
                        region_name=self.region,
                    )                
                
                response = client.invoke_model(
                    modelId=self.modelId, body=request
                )
                response_body = json.loads(response['body'].read()) 
                embedding_array = response_body.get("embedding")

                if embedding_array:
                    embeddings_list = [float(value) for value in embedding_array]
                    embeddings.append(embeddings_list)
                
                model_engine_response = EmbeddingsModelEngineResponse(
                response=embeddings,
                prompt_tokens=response_body.get("inputTextTokenCount"),
                response_tokens=0
            )

            except requests.RequestException as e:
                print(f"An error occurredv in aws titan: {e}")                 
        
        return model_engine_response
    
    def _get_tokenizer(self, init_args):
        return None

   