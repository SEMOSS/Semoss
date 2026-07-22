from typing import List, Optional, Dict, Any, Union

from genai_client.constants import EmbeddingsModelEngineResponse
from genai_client.tokenizers.abstract_tokenizer import AbstractTokenizer
from ..constants import MAX_TOKENS, EmbeddingsModelEngineResponse2
from .abstract_embedder import AbstractEmbedder
from google.genai import types
from google.genai.types import EmbedContentConfig
from google.genai import Client as GoogleGenAIClient
from ..clients.google_clients import (
    GoogleClient,
    GoogleClientConfig,
    GoogleClientType,
)
from ..text_generation.model_engine_exception import ModelEngineException, ErrorDetails


class GoogleGenAiEmbedder(AbstractEmbedder):
    def __init__(
        self,
        model_name: str,
        service_account_credentials: Optional[Dict] = None,
        service_account_key_file: Optional[str] = None,
        region: Optional[str] = None,
        project: Optional[str] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            model_name=model_name,
            max_tokens=kwargs.pop(MAX_TOKENS, None),
            **kwargs,
        )
        self.client_config = GoogleClientConfig(
            type=GoogleClientType.GOOGLE,
            service_account_credentials=service_account_credentials,
            service_account_key_file=service_account_key_file,
            region=region,
            project=project,
            api_key=api_key,
            base_url=base_url,
        )
        self.client = GoogleClient(config=self.client_config).genai_client
        self.model = model_name

    def embeddings_call(
        self, strings_to_embed: List[str], **kwargs
    ) -> Union[EmbeddingsModelEngineResponse2, ErrorDetails]:
        embedding_config = EmbedContentConfig(
            task_type=kwargs.get("task_type"),
            output_dimensionality=kwargs.get("output_dimensionality"),
            title=kwargs.get("title"),
        )
        try:
            response = self.client.models.embed_content(
                model=self.model,
                contents=[string for string in strings_to_embed],
                config=embedding_config,
            )
            embedding_results = response.embeddings or []
            prompt_tokens = 0
            embeddings = []
            truncation_meta = []
            billable_character_count_meta = (
                response.metadata.billable_character_count
                if response.metadata and response.metadata.billable_character_count
                else 0
            )

            for e in embedding_results:
                embeddings.append(e.values) if e.values else embeddings.append([])
                if e.statistics:
                    prompt_tokens += (
                        e.statistics.token_count if e.statistics.token_count else 0
                    )
                    truncation_meta.append(e.statistics.truncated)

            return EmbeddingsModelEngineResponse2(
                response=embeddings,
                prompt_tokens=int(prompt_tokens),
                metadata={
                    "truncated": truncation_meta,
                    "billable_character_count": billable_character_count_meta,
                },
            )
        except Exception as e:
            return ModelEngineException(
                error=e, client="google", model=self.model_name
            ).parse_error()

    def _get_tokenizer(self, init_args: Dict) -> AbstractTokenizer:
        return super()._get_tokenizer(init_args)

    def image_embeddings_call(
        self, images_to_embed, **kwargs: Any
    ) -> EmbeddingsModelEngineResponse:
        return super().image_embeddings_call(images_to_embed, **kwargs)
