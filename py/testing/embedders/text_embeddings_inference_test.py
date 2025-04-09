"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest -v testing/embedders/text_embeddings_inference_test.py
    pytest -s testing/embedders/text_embeddings_inference_test.py -> For enabling the print statements

Ensure you have a valid `.env` file with the below required keys:
    - EMBEDDER_TEXT_EMBEDDING_ENDPOINT

Install pytest if not already installed:
    pip install pytest
"""

import pytest
import os
from typing import Dict
from genai_client import TextEmbeddingsInference
from genai_client.constants import EmbeddingsModelEngineResponse
from dotenv import load_dotenv

# Load environment variables from .env.example fil
load_dotenv("testing/.env.example")


@pytest.fixture
def inference_embedder():
    endpoint = os.getenv("EMBEDDER_TEXT_EMBEDDING_ENDPOINT")
    return TextEmbeddingsInference(
        endpoint=endpoint, model_name="BAAI/bge-large-en-v1.5"
    )


def test_inference_embedder_ask(inference_embedder):
    """
    Test the TextEmbeddingsInference ask method.
    - Ensures a response dictionary is returned.
    - Validates unsupported generation message.
    """
    ask_response = inference_embedder.ask(question="what is the capital of france?")
    assert isinstance(ask_response, Dict)
    assert ask_response["response"] == "This model does not support text generation."


def test_inference_embedder_embeddings(inference_embedder):
    """
    Test the embeddings_call method.
    - Ensures a proper embedding response.
    - Checks for correct embedding length (1024).
    """
    embeddings_response = inference_embedder.embeddings_call(
        strings_to_embed=["What is the capital of France?"]
    )
    assert isinstance(embeddings_response, EmbeddingsModelEngineResponse)
    assert len(embeddings_response.response) == 1
    assert len(embeddings_response.response[0]) == 1024
