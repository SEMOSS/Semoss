from typing import List
import boto3
import json
import logging
import requests

from .abstract_embedder import AbstractEmbedder
from ..constants import EmbeddingsModelEngineResponse

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BedrockEmbedder(AbstractEmbedder):
    def __init__(
        self,
        model_name: str = None,
        modelId: str = None,
        access_key=None,
        secret_key=None,
        region=None,
        cohere_input_type: str = None,
        **kwargs,
    ) -> None:
        self.kwargs = kwargs
        if model_name:
            self.model_name = model_name
        elif modelId:
            self.model_name = modelId
        else:
            raise ValueError("Either model_name or modelId must be provided")

        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service_name = "bedrock-runtime"
        self.cohere_input_type = cohere_input_type

        # Create the client once during initialization
        self.client = boto3.client(
            service_name=self.service_name,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
        )

        super().__init__(
            model_name=self.model_name,
            **kwargs,
        )

    def embeddings_call(
        self, strings_to_embed: List[str], prefix: str = ""
    ) -> EmbeddingsModelEngineResponse:
        embeddings_list = []
        embeddings = []

        for text in strings_to_embed:
            json_obj = self.createJsonObjForModel(text)
            request = json.dumps(json_obj)

            try:
                response = self.client.invoke_model(
                    modelId=self.model_name, body=request
                )
                response_body = json.loads(response["body"].read())

                # Determine the correct key for embeddings
                if "amazon.titan-embed-text" in self.model_name:
                    embedding_array = response_body.get("embedding")
                elif "cohere.embed" in self.model_name:
                    embedding_array = response_body.get("embeddings")[
                        0
                    ]  # we are only sending in 1 at a time in the for loop
                else:
                    raise ValueError(f"Unsupported model name: {self.model_name}")

                if embedding_array:
                    embeddings_list = [float(value) for value in embedding_array]
                    embeddings.append(embeddings_list)

                model_engine_response = EmbeddingsModelEngineResponse(
                    response=embeddings,
                    prompt_tokens=response_body.get("inputTextTokenCount"),
                    response_tokens=0,
                )

            except requests.RequestException as e:
                logger.error(f"An error occurred in bedrock embedding: {e}")

        return model_engine_response

    def image_embeddings_call(
        self, images_to_embed: List[str], **kwargs
    ) -> EmbeddingsModelEngineResponse:
        raise NotImplementedError("This model does not support image embeddings.")

    def createJsonObjForModel(self, text):
        if "amazon.titan-embed-text" in self.model_name:
            return {"inputText": text}
        elif "cohere.embed" in self.model_name:
            json_obj = {"texts": [text]}
            json_obj["input_type"] = self.cohere_input_type or "search_document"
            return json_obj
        else:
            raise ValueError(f"Unsupported model name: {self.model_name}")
        
    def keyword_extraction(
        self, input: List[str], percentile: int = 0, max_keywords: int = 12
    ) -> List[str]:
        from keybert import KeyBERT

        kw_embedder = self.to_keybert_embedder()
        kw_model = KeyBERT(model=kw_embedder)
        #kw_model.extract_keywords(docs=input)

        list_of_chunks = input

        keywords = BedrockEmbedder.get_text_keywords(
            kw_model=kw_model,
            list_of_chunks=list_of_chunks,
            percentile=percentile,
            max_keywords=max_keywords,
        )

        return keywords
    
    def get_text_keywords(
        kw_model, list_of_chunks: List[str], percentile: int, max_keywords: int
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

    def _get_tokenizer(self, init_args):
        return None
