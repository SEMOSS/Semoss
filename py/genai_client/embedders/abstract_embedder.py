from abc import ABC, abstractmethod

from typing import Dict, Any, List, Optional, TYPE_CHECKING
import numpy as np
from ..constants import (
    MODEL_NAME,
    AskModelEngineResponse,
    EmbeddingsModelEngineResponse,
    EmbeddingsModelEngineResponse2,
)
from ..tokenizers.abstract_tokenizer import AbstractTokenizer
from ..text_generation.model_engine_exception import (
    ErrorDetails,
    ModelEngineException,
)
from ..usage_ceiling import (
    IMAGE_TOKENS_ESTIMATE,
    USAGE_CEILING,
    PromptCount,
    UsageCeiling,
    UsageLimitDoesNotFitError,
    count_texts_with_tokenizer,
)

if TYPE_CHECKING:
    from keybert.backend import BaseEmbedder


class AbstractEmbedder(ABC):

    def __init__(self, **kwargs):
        self.model_name = kwargs[MODEL_NAME]
        self.tokenizer = self._get_tokenizer(kwargs)

    @abstractmethod
    def _get_tokenizer(self, init_args: Dict) -> AbstractTokenizer:
        pass

    def embeddings(self, strings_to_embed: List[str], **kwargs: Any) -> Dict:
        # an embedding has no output to lower, so the whole input must fit, at
        # the input rate; one that does not is refused before the provider is
        # called, so nothing is billed
        ceiling = UsageCeiling.from_value(kwargs.pop(USAGE_CEILING, None))
        if ceiling is not None:
            try:
                ceiling.require_input_fits(
                    self.count_embedding_tokens(strings_to_embed)
                )
            except UsageLimitDoesNotFitError as e:
                return self._usage_limit_error(e)
        response = self.embeddings_call(strings_to_embed, **kwargs)
        if isinstance(response, EmbeddingsModelEngineResponse2) or isinstance(
            response, ErrorDetails
        ):
            return response.model_dump()
        elif isinstance(response, EmbeddingsModelEngineResponse):
            return response.to_dict()
        else:
            raise Exception("Could not perform embeddings")

    @abstractmethod
    def embeddings_call(
        self, strings_to_embed, **kwargs: Any
    ) -> EmbeddingsModelEngineResponse:
        pass

    def count_embedding_tokens(self, strings_to_embed: List[str]) -> PromptCount:
        """Count the input for the usage ceiling: the embedder's tokenizer when
        it has one, a conservative estimate otherwise."""
        return count_texts_with_tokenizer(
            getattr(self, "tokenizer", None), strings_to_embed or []
        )

    def count_multi_modal_embedding_tokens(
        self,
        text: List[str] = None,
        image: List[str] = None,
        video: List[str] = None,
    ) -> PromptCount:
        """Count a multi modal input for the usage ceiling: the text as
        ``count_embedding_tokens`` does, and each image and video as an
        image's worth of tokens, as the Java engine estimates them."""
        counted = self.count_embedding_tokens(text or [])
        media = len(image or []) + len(video or [])
        if not media:
            return counted
        return PromptCount(
            tokens=counted.tokens + media * IMAGE_TOKENS_ESTIMATE, estimated=True
        )

    def multi_modal_input_error(
        self,
        ceiling_value: Any,
        text: List[str] = None,
        image: List[str] = None,
        video: List[str] = None,
    ) -> Optional[Dict]:
        """The USAGE_LIMIT_DOES_NOT_FIT error response when a multi modal input does
        not fit its ``usage_ceiling``, or None when it fits or has none.

        An embedder that overrides ``multi_modal_embeddings``, such as the
        Google GenAI one, calls this first, as the input arrives in its own
        arguments rather than through ``embeddings``."""
        ceiling = UsageCeiling.from_value(ceiling_value)
        if ceiling is None:
            return None
        try:
            ceiling.require_input_fits(
                self.count_multi_modal_embedding_tokens(text, image, video)
            )
        except UsageLimitDoesNotFitError as e:
            return self._usage_limit_error(e)
        return None

    def _usage_limit_error(self, error: UsageLimitDoesNotFitError) -> Dict:
        """The USAGE_LIMIT_DOES_NOT_FIT error response. ``response`` repeats the
        message, so a caller that reads only ``response`` still shows it."""
        details = ModelEngineException(
            error=error, client=type(self).__name__, model=str(self.model_name)
        ).parse_error()
        return {**details.model_dump(), "response": details.message}

    def multi_modal_embeddings(
        self,
        text: List[str] = None,
        image: List[str] = None,
        video: List[str] = None,
        **kwargs: Any,
    ) -> Dict:
        """Embed text/image/video together, broken out by modality.

        Optional capability: embedders that support it override this method.
        The default reports that the operation is not implemented rather than
        raising, so callers get a graceful response.
        """
        # nothing is embedded, so the ceiling is dropped unused
        kwargs.pop(USAGE_CEILING, None)
        return {
            "response": "This model does not support multi modal embeddings.",
            "implemented": False,
        }

    def ask(self, *args, **kwargs) -> Dict:
        response = "This model does not support text generation."
        model_engine_response = AskModelEngineResponse(
            response=response,
            prompt_tokens=0,
            response_tokens=self.tokenizer.count_tokens(response),
        )

        return model_engine_response.to_dict()

    def to_keybert_embedder(self) -> "BaseEmbedder":
        from keybert.backend import BaseEmbedder

        class KeyBertEmbedderBackend(BaseEmbedder):

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

        return KeyBertEmbedderBackend(embedding_model=self)
