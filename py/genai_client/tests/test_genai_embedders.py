### ATTENTION
### To run this you need to be in the Semoss_Dev/py directory
### Then run the following command
### py -3.10 -m unittest genai_client.tests.test_genai_embedders

import unittest

from typing import Dict

from genai_client import (
    LocalEmbedder,
    OpenAiEmbedder,
    AzureOpenAiEmbedder,
    TextEmbeddingsInference,
    VertexAiEmbedder,
)

from genai_client.constants import EmbeddingsModelEngineResponse


class EmbeddingsModelTests(unittest.TestCase):

    def test_local_embedder(self):
        # declare the model
        embedder = LocalEmbedder(model_name="BAAI/bge-large-en-v1.5")

        # make sure the ask expected
        ask_response = embedder.ask(question="what is the capital of france ey?")
        self.assertIsInstance(ask_response, Dict)
        self.assertEqual(
            ask_response["response"], "This model does not support text generation."
        )

        embeddings_response = embedder.embeddings_call(strings_to_embed=["hello"])
        self.assertIsInstance(embeddings_response, EmbeddingsModelEngineResponse)
        self.assertEqual(len(embeddings_response.response), 1)
        self.assertEqual(len(embeddings_response.response[0]), 1024)

    def test_text_generation_embeddings(self):
        # declare the model
        embedder = TextEmbeddingsInference(
            endpoint="***REMOVED***", model_name="BAAI/bge-large-en-v1.5"
        )

        # make sure the ask expected
        ask_response = embedder.ask(question="what is the capital of france ey?")
        self.assertIsInstance(ask_response, Dict)
        self.assertEqual(
            ask_response["response"], "This model does not support text generation."
        )

        embeddings_response = embedder.embeddings_call(strings_to_embed=["hello"])
        self.assertIsInstance(embeddings_response, EmbeddingsModelEngineResponse)
        self.assertEqual(len(embeddings_response.response), 1)
        self.assertEqual(len(embeddings_response.response[0]), 1024)

    def test_openai_text_davinci(self):
        import os

        openai_key = os.environ.get("OPENAI_API_KEY")

        # declare the model
        embedder = OpenAiEmbedder(
            model_name="text-embedding-ada-002",
            # api_key = openai_key
            api_key=openai_key,
        )

        # make sure the ask expected
        ask_response = embedder.ask(question="what is the capital of france ey?")
        self.assertIsInstance(ask_response, Dict)
        self.assertEqual(
            ask_response["response"], "This model does not support text generation."
        )

        embeddings_response = embedder.embeddings_call(strings_to_embed=["hello"])
        self.assertIsInstance(embeddings_response, EmbeddingsModelEngineResponse)
        self.assertEqual(len(embeddings_response.response), 1)
        self.assertEqual(len(embeddings_response.response[0]), 1536)

    def test_azure_embedder(self):
        import os

        api_key = os.environ.get("AZURE_OPENAI_API_KEY")

        # declare the model
        embedder = AzureOpenAiEmbedder(
            model_name="embedding-model",
            api_key=api_key,
            endpoint="***REMOVED***",
            api_version="2023-05-15",
        )

        # make sure the ask expected
        ask_response = embedder.ask(question="what is the capital of france ey?")
        self.assertIsInstance(ask_response, Dict)
        self.assertEqual(
            ask_response["response"], "This model does not support text generation."
        )

        embeddings_response = embedder.embeddings_call(strings_to_embed=["hello"])
        self.assertIsInstance(embeddings_response, EmbeddingsModelEngineResponse)
        # self.assertEqual(len(embeddings_response.response), 8192)

    def test_vertex_embedder(self):
        import os

        vertex_sercive_key_path = os.environ.get("GOOGLE_SERVIVE_ACCOUNT_FILE_PATH")

        # declare the model
        embedder = VertexAiEmbedder(
            model_name="textembedding-gecko@001",
            service_account_key_file=vertex_sercive_key_path,
            region="us-central1",
        )

        # make sure the ask expected
        ask_response = embedder.ask(question="what is the capital of france ey?")
        self.assertIsInstance(ask_response, Dict)
        self.assertEqual(
            ask_response["response"], "This model does not support text generation."
        )

        embeddings_response = embedder.embeddings_call(strings_to_embed=["hello"])
        self.assertIsInstance(embeddings_response, EmbeddingsModelEngineResponse)
        # self.assertEqual(len(embeddings_response.response), 8192)
