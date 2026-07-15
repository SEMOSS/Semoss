from typing import List, Dict
from ..tokenizers.openai_tokenizer import OpenAiTokenizer
from ..constants import MAX_TOKENS, EmbeddingsModelEngineResponse
from .abstract_embedder import AbstractEmbedder


class OpenAiEmbedder(AbstractEmbedder):

    def __init__(self, model_name: str, api_key: str, **kwargs):
        super().__init__(
            model_name=model_name,
            max_tokens=kwargs.pop(MAX_TOKENS, None),
            **kwargs,
        )
        self.client = self._get_client(api_key=api_key, **kwargs)

    def embeddings_call(
        self, strings_to_embed: List[str], prefix: str = "", **kwargs
    ) -> EmbeddingsModelEngineResponse:
        assert isinstance(strings_to_embed, list)

        if "base_url" in kwargs:
            self.client.base_url = kwargs["base_url"]

        total_num_of_tokens = self.tokenizer.count_tokens("".join(strings_to_embed))

        # If no max token length or within limits, process all at once
        if (
            not self.tokenizer.get_max_token_length()
            or total_num_of_tokens <= self.tokenizer.get_max_token_length()
        ):
            embedded_list = self._process_single_batch(strings_to_embed)
        else:
            embedded_list = self._process_multiple_batches(strings_to_embed)

        return EmbeddingsModelEngineResponse(
            response=embedded_list, prompt_tokens=total_num_of_tokens, response_tokens=0
        )

    def image_embeddings_call(
        self, images_to_embed: List[str], **kwargs
    ) -> EmbeddingsModelEngineResponse:
        assert isinstance(images_to_embed, list)

        if "base_url" in kwargs:
            self.client.base_url = kwargs["base_url"]

        response = self._make_openai_embedding_call(images_to_embed)

        embedded_list = []
        for data in response.data:
            embedded_list.append(data.embedding)

        return EmbeddingsModelEngineResponse(
            response=embedded_list, prompt_tokens=0, response_tokens=0
        )

    def _make_openai_embedding_call(self, list_of_text: List[str]):
        """Executes the embedding call via the OpenAI API"""
        return self.client.embeddings.create(model=self.model_name, input=list_of_text)

    def _get_tokenizer(self, init_args: Dict) -> OpenAiTokenizer:
        return OpenAiTokenizer(
            encoder_name=self.model_name, max_tokens=init_args.pop(MAX_TOKENS, None)
        )

    def _get_client(self, api_key, **kwargs):
        from openai import OpenAI

        return OpenAI(api_key=api_key, **kwargs)

    def _process_single_batch(self, strings_to_embed: List[str]) -> List:
        results = self._make_openai_embedding_call(strings_to_embed)
        return [vector.embedding for vector in results.data]

    def _process_multiple_batches(self, strings_to_embed: List[str]) -> List:
        batches = self._create_batches(strings_to_embed)
        embedded_list = []

        for batch in batches:
            batch_results = self._make_openai_embedding_call(batch)
            embedded_list.extend([vector.embedding for vector in batch_results.data])

        return embedded_list

    def _create_batches(self, strings_to_embed: List[str]) -> List[List[str]]:
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
                batches.append(current_batch)
                current_batch = [chunk]
                current_token_count = chunk_token_count

        if current_batch:
            batches.append(current_batch)

        return batches
