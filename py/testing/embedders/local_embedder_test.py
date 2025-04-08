"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest -v testing/embedders/local_embedder_test.py
    pytest -s testing/embedders/local_embedder_test.py -> For enabling the print statements

Install pytest if not already installed:
    pip install pytest
"""

import pytest
from typing import Dict
from genai_client import LocalEmbedder
from genai_client.constants import EmbeddingsModelEngineResponse

@pytest.fixture
def local_embedder():
    return LocalEmbedder(model_name="BAAI/bge-large-en-v1.5")

def test_local_embedder_ask(local_embedder):
    """
    Test the LocalEmbedder's ask method.
    - Ensures the response is a dictionary.
    - Validates the fallback message for unsupported generation.
    """
    ask_response = local_embedder.ask(question="what is the capital of france?")
    assert isinstance(ask_response, Dict)
    assert ask_response["response"] == "This model does not support text generation."

def test_local_embedder_embeddings(local_embedder):
    """
    Tests the embeddings_call method.
    - Checks if embeddings are returned in the expected structure.
    - Verifies the embedding size is correct (1024).
    """
    embeddings_response = local_embedder.embeddings_call(strings_to_embed=["What is the capital of France?"])
    assert isinstance(embeddings_response, EmbeddingsModelEngineResponse)
    assert len(embeddings_response.response) == 1
    assert len(embeddings_response.response[0]) == 1024
 