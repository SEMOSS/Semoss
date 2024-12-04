"""
This module provides the HFEndpointEncoder class to embeddings models using Huggingface's endpoint.

The HFEndpointEncoder class is a subclass of BaseEncoder and utilizes a specified Huggingface 
endpoint to generate embeddings for given documents. It requires the URL of the Huggingface 
API endpoint and an API key for authentication. The class supports customization of the score 
threshold for filtering or processing the embeddings.

Example usage:

    from semantic_router.encoders.hfendpointencoder import HFEndpointEncoder

    encoder = HFEndpointEncoder(
        huggingface_url="https://api-inference.huggingface.co/models/BAAI/bge-large-en-v1.5",
        huggingface_api_key="your-hugging-face-api-key"
    )
    embeddings = encoder(["document1", "document2"])

Classes:
    HFEndpointEncoder: A class for generating embeddings using a Huggingface endpoint.
"""

"""
this class has to be packaged as a function engine

"""

import requests
import time
import os
from typing import Any, List, Optional, Dict


from pydantic.v1 import PrivateAttr

from semantic_router.encoders import BaseEncoder
from semantic_router.utils.logger import logger


class GovConnectAIEncoder(BaseEncoder):
    name: str = "e4449559-bcff-4941-ae72-0e3f18e06660"
    score_threshold: float = 0.5
    tokenizer_kwargs: Dict = {}
    model_kwargs: Dict = {}
    device: Optional[str] = None
    _tokenizer: Any = PrivateAttr()
    _model: Any = PrivateAttr()
    _torch: Any = PrivateAttr()

    def __init__(self, **data):
        super().__init__(**data)
        #self._tokenizer, self._model = self._initialize_hf_model()
        self._model = _initialize_gca_model()

    def _initialize_gca_model(self):
        from gaas_gpt_model import ModelEngine
        model = ModelEngine(engine_id = self.name)
        return model

    def __call__(
        self,
        docs: List[str],
        batch_size: int = 1,
        normalize_embeddings: bool = True,
        pooling_strategy: str = "mean",
    ) -> List[List[float]]:
    
        all_embeddings = []
        for i in range(0, len(docs), batch_size):
            batch_docs = docs[i : i + batch_size]
            embeddings = self._model.embeddings(batch_docs)
            embeddings = embeddings[0]['response']
            all_embeddings.extend(embeddings)
        return all_embeddings



class GovConnectAIRESTEncoder(BaseEncoder):
    """
    A class to encode documents using a Hugging Face transformer model endpoint.

    Attributes:
        huggingface_url (str): The URL of the Hugging Face API endpoint.
        huggingface_api_key (str): The API key for authenticating with the Hugging Face API.
        score_threshold (float): A threshold value used for filtering or processing the embeddings.
    """
    score_threshold: float = 0.5
    _sc: Any = PrivateAttr()
    _model:Any = PrivateAttr()



    def __init__(
        self,
        name: Optional[str] = "https://workshop.cfg.deloitte.com/cfg-ai-dev/Monolith/api",
        secret_key: str = None,
        access_key: str = None,
        score_threshold: float = 0.8,
        engine_id = "e4449559-bcff-4941-ae72-0e3f18e06660"
    ):
        """
        Initializes the HFEndpointEncoder with the specified parameters.

        Args:
            name (str, optional): The name of the encoder. Defaults to
                "hugging_face_custom_endpoint".
            huggingface_url (str, optional): The URL of the Hugging Face API endpoint.
                Cannot be None.
            huggingface_api_key (str, optional): The API key for the Hugging Face API.
                Cannot be None.
            score_threshold (float, optional): A threshold for processing the embeddings.
                Defaults to 0.8.

        Raises:
            ValueError: If either `huggingface_url` or `huggingface_api_key` is None.
        """
        super().__init__(name=name, score_threshold=score_threshold)
        import ai_server
        from ai_server import ModelEngine
        self._sc = ai_server.RESTServer(access_key=access_key, secret_key=secret_key, base=name)
        self._model = ModelEngine(engine_id=engine_id, insight_id=self._sc.cur_insight)
        self._model.embeddings(["yo baby"])
        


    def __call__(self, docs: List[str]) -> List[List[float]]:
        """
        Encodes a list of documents into embeddings using the Hugging Face API.

        Args:
            docs (List[str]): A list of documents to encode.

        Returns:
            List[List[float]]: A list of embeddings for the given documents.

        Raises:
            ValueError: If no embeddings are returned for a document.
        """
        all_embeddings = []
        batch_size=1
        for i in range(0, len(docs), batch_size):
            batch_docs = docs[i : i + batch_size]
            print(batch_docs)
            embeddings = self._model.embeddings(batch_docs)[0]['response']
            all_embeddings.extend(embeddings)
        return all_embeddings

