from typing import List, Dict
from openai import OpenAI

from genai_client.tokenizers.abstract_tokenizer import AbstractTokenizer

from ..tokenizers.openai_tokenizer import OpenAiTokenizer

from ..constants import MAX_TOKENS, EmbeddingsModelEngineResponse

from .abstract_embedder import AbstractEmbedder

from logging_config import get_logger

class_logger = get_logger(__name__)


class OpenAiEmbedder(AbstractEmbedder):

    def __init__(self, model_name: str, api_key: str, **kwargs):

        # register the some of the model details with the abstract embedder class
        super().__init__(
            model_name=model_name,
            max_tokens=kwargs.pop(MAX_TOKENS, None),
            **kwargs,
        )

        self.client = self._get_client(api_key=api_key, **kwargs)

    def _get_tokenizer(self, init_args: Dict) -> AbstractTokenizer:
        tokenizer = OpenAiTokenizer(
            encoder_name=self.model_name, max_tokens=init_args.pop(MAX_TOKENS, None)
        )

        return tokenizer

    def _get_client(self, api_key, **kwargs):
        from openai import OpenAI

        return OpenAI(api_key=api_key, **kwargs)

    def embeddings_call(
        self, strings_to_embed: List[str], prefix: str = ""
    ) -> EmbeddingsModelEngineResponse:
        # Make sure a list was passed in so we can proceed with the logic below
        assert isinstance(strings_to_embed, list)

        total_num_of_tokens = self.tokenizer.count_tokens("".join(strings_to_embed))

        embedded_list = []  # This is the final list that will be sent back
        if self.tokenizer.get_max_token_length() != None:
            if total_num_of_tokens <= self.tokenizer.get_max_token_length():
                # The entire list can be sent as a single batch
                class_logger.debug(
                    "Waiting for OpenAI to process all chunks",
                    extra={"stack": "BACKEND"},
                )

                single_batch_results = self._make_openai_embedding_call(
                    strings_to_embed
                )

                embedded_list = [
                    vector.embedding for vector in single_batch_results.data
                ]

            else:
                # Split the list into batches
                current_batch = []
                current_token_count = 0
                batches = []

                for chunk in strings_to_embed:
                    chunk_token_count = self.tokenizer.count_tokens(chunk)

                    if (
                        current_token_count + chunk_token_count
                        <= self.tokenizer.get_max_token_length()
                    ):
                        current_batch.append(chunk)
                        current_token_count += chunk_token_count
                    else:
                        # Start a new batch
                        batches.append(current_batch)
                        current_batch = [chunk]
                        current_token_count = chunk_token_count

                if len(current_batch) > 0:
                    batches.append(current_batch)

                class_logger.debug(
                    "Multiple batches have to be sent to OpenAI",
                    extra={"stack": "BACKEND"},
                )
                number_of_batches = len(batches)
                for i in range(number_of_batches):
                    batch_results = self._make_openai_embedding_call(batches[i])
                    class_logger.debug(
                        "Completed Embedding "
                        + str(i + 1)
                        + "/"
                        + str(number_of_batches)
                        + " Batches",
                        extra={"stack": "BACKEND"},
                    )
                    embedded_list.extend(
                        [vector.embedding for vector in batch_results.data]
                    )
        else:
            # We have no choice but to try send the entire thing
            class_logger.debug(
                "Waiting for OpenAI to process all chunks", extra={"stack": "BACKEND"}
            )
            single_batch_results = self._make_openai_embedding_call(strings_to_embed)

            embedded_list = [vector.embedding for vector in single_batch_results.data]

        class_logger.debug(
            "Sending Embeddings back from Model Engine", extra={"stack": "BACKEND"}
        )

        model_engine_response = EmbeddingsModelEngineResponse(
            response=embedded_list, prompt_tokens=total_num_of_tokens, response_tokens=0
        )

        return model_engine_response

    def _make_openai_embedding_call(self, list_of_text: List[str]):
        """this method is responsible for making the openai embeddings call. it takes in a single"""
        return self.client.embeddings.create(model=self.model_name, input=list_of_text)
