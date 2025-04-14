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

    def image_embeddings(self, images_to_embed: List[str], **kwargs: Any) -> Dict:
        return self.image_embeddings_call(images_to_embed, **kwargs).to_dict()

    @abstractmethod
    def image_embeddings_call(
        self, images_to_embed, **kwargs: Any
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

    def keyword_extraction(
        self, input: List[str], percentile: int = 0, max_keywords: int = 12
    ) -> List[str]:
        from keybert import KeyBERT

        kw_embedder = self.to_keybert_embedder()
        kw_model = KeyBERT(model=kw_embedder)

        list_of_chunks = input

        keywords = self.get_text_keywords(
            kw_model=kw_model,
            list_of_chunks=list_of_chunks,
            percentile=percentile,
            max_keywords=max_keywords,
        )

        return keywords

    def get_text_keywords(
        self, kw_model, list_of_chunks: List[str], percentile: int, max_keywords: int
    ) -> List[str]:

        import numpy as np
        from keyphrase_vectorizers import KeyphraseCountVectorizer

        if len(list_of_chunks) == 1:
            master_keywords_list = [
                kw_model.extract_keywords(
                    list_of_chunks,
                    top_n=max_keywords,
                    vectorizer=KeyphraseCountVectorizer(),
                    use_mmr=True,
                )
            ]
        else:
            master_keywords_list = kw_model.extract_keywords(
                list_of_chunks,
                top_n=max_keywords,
                vectorizer=KeyphraseCountVectorizer(),
                use_mmr=True,
            )

        for i, keywords in enumerate(master_keywords_list):
            if len(keywords) > 0:
                keywords = keywords
            else:
                keywords = [("", 1.0)]

            prob = [item[1] for item in keywords]
            threshold = np.percentile(prob, percentile)
            filtered_data = [word for word, score in keywords if score >= threshold]
            master_keywords_list[i] = " ".join(filtered_data)

        return master_keywords_list

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
