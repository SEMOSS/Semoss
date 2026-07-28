from typing import List, Optional
import boto3.session
import json, requests

from .abstract_embedder import AbstractEmbedder
from ..constants import EmbeddingsModelEngineResponse


class BedrockEmbedder(AbstractEmbedder):
    def __init__(
        self,
        model_name: Optional[str] = None,
        modelId: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: Optional[str] = None,
        cohere_input_type: Optional[str] = None,
        **kwargs,
    ) -> None:
        if model_name:
            self.model_name = model_name
        elif modelId:
            self.model_name = modelId
        else:
            raise ValueError("Either model_name or modelId must be provided")

        super().__init__(
            model_name=self.model_name,
            **kwargs,
        )

        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service_name = "bedrock-runtime"
        self.cohere_input_type = cohere_input_type

        session = boto3.session.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
        )

        self.client = session.client(self.service_name)

    def embeddings_call(
        self, strings_to_embed: List[str], prefix: str = ""
    ) -> EmbeddingsModelEngineResponse:
        embeddings = []
        total_prompt_tokens = 0

        for text in strings_to_embed:
            json_obj = self.createJsonObjForModel(text)
            request = json.dumps(json_obj, ensure_ascii=False)

            try:
                response = self.client.invoke_model(
                    modelId=self.model_name, body=request
                )
                response_body = json.loads(response["body"].read())

                # Determine the correct key for embeddings and token count
                if "amazon.titan-embed-text" in self.model_name:
                    embedding_array = response_body.get("embedding")
                    total_prompt_tokens += response_body.get("inputTextTokenCount", 0)
                elif "cohere.embed" in self.model_name:
                    embedding_array = response_body.get("embeddings")[0]
                    # Cohere on Bedrock does not return token counts
                else:
                    raise ValueError(f"Unsupported model name: {self.model_name}")

                if embedding_array:
                    embeddings.append([float(value) for value in embedding_array])

            except requests.RequestException as e:
                raise Exception(f"An error occurred in bedrock embedding: {e}")

        return EmbeddingsModelEngineResponse(
            response=embeddings,
            prompt_tokens=total_prompt_tokens,
            response_tokens=0,
        )

    def createJsonObjForModel(self, text):
        if "amazon.titan-embed-text" in self.model_name:
            return {"inputText": text}
        elif "cohere.embed" in self.model_name:
            json_obj = {"texts": [text]}
            json_obj["input_type"] = self.cohere_input_type or "search_document"
            return json_obj
        else:
            raise ValueError(f"Unsupported model name: {self.model_name}")

    def _get_tokenizer(self, init_args):
        return None
