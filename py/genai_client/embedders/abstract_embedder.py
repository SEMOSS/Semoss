from typing import Dict, Any, List

import numpy as np

from abc import ABC, abstractmethod
from keybert.backend import BaseEmbedder

from ..constants import (
    MODEL_NAME,
    AskModelEngineResponse,
    EmbeddingsModelEngineResponse,
)
from ..tokenizers.abstract_tokenizer import AbstractTokenizer


class AbstractEmbedder(ABC):

    def __init__(self, **kwargs):
        self.model_name = kwargs[MODEL_NAME]
        self.tokenizer = self._get_tokenizer(kwargs)

    @abstractmethod
    def _get_tokenizer(self, init_args: Dict) -> AbstractTokenizer:
        pass

    def embeddings(self, strings_to_embed: List[str], **kwargs: Any) -> Dict:
        return self.embeddings_call(strings_to_embed, **kwargs).to_dict()

    @abstractmethod
    def embeddings_call(
        self, strings_to_embed, **kwargs: Any
    ) -> EmbeddingsModelEngineResponse:
        pass

    def ask(self, *args, **kwargs) -> Dict:
        response = "This model does not support text generation."
        model_engine_response = AskModelEngineResponse(
            response=response,
            prompt_tokens=0,
            response_tokens=self.tokenizer.count_tokens(response),
        )

        return model_engine_response.to_dict()

    def to_keybert_embedder(self) -> BaseEmbedder:

        class CfgEmbedderBackend(BaseEmbedder):

            def __init__(self, embedding_model: AbstractEmbedder):
                super().__init__()

                self.embedding_model = embedding_model

            def embed(self, documents: List[str], verbose: bool = False) -> np.ndarray:
                """
                Embed a list of n documents/words into an n-dimensional matrix of embeddings

                Arguments:
                    documents: A list of documents or words to be embedded
                    verbose: Controls the verbosity of the process

                Returns:
                    Document/words embeddings with shape (n, m) with `n` documents/words
                    that each have an embeddings size of `m`
                """
                # even though the BaseEmbedder class says the input is List[str], they send arrays sometimes
                if not isinstance(documents, List):
                    documents = documents.tolist()

                embeddings = self.embedding_model.embeddings_call(
                    strings_to_embed=documents
                )

                return np.array(embeddings.response, dtype="float32")

        return CfgEmbedderBackend(embedding_model=self)
