"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest -v testing/embedders/bedrock_embedder_test.py
    pytest -s testing/embedders/bedrock_embedder_test.py -> For enabling the print statements

Ensure you have a valid `.env` file with the below required keys:
    - EMBEDDER_BEDROCK_MODEL_ID
    - EMBEDDER_BEDROCK_ACCESS_KEY
    - EMBEDDER_BEDROCK_SECRET_KEY
    - EMBEDDER_BEDROCK_REGION

Install pytest if not already installed:
    pip install pytest
"""

import pytest
import os
from genai_client import BedrockEmbedder
from genai_client.constants import EmbeddingsModelEngineResponse
from dotenv import load_dotenv

# Load environment variables from .env.example file
load_dotenv("testing/.env")


# Bedrock Embedder
@pytest.fixture
def bedrock_embedder():
    model_id = os.getenv("EMBEDDER_BEDROCK_MODEL_ID")
    access_key = os.getenv("EMBEDDER_BEDROCK_ACCESS_KEY")
    secret_key = os.getenv("EMBEDDER_BEDROCK_SECRET_KEY")
    region = os.getenv("EMBEDDER_BEDROCK_REGION")
    return BedrockEmbedder(
        modelId=model_id,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
    )


def test_bedrock_embeddings_call_success(bedrock_embedder):
    """
    Tests the embeddings_call method of BedrockEmbedder.
    - Ensures a valid EmbeddingsModelEngineResponse is returned.
    - Validates the embeddings length and type.
    """
    response = bedrock_embedder.embeddings_call(
        strings_to_embed=["What is the capital of France?"]
    )
    assert isinstance(response, EmbeddingsModelEngineResponse)
    assert isinstance(response.response, list)
    assert len(response.response) == 1
    assert isinstance(response.response[0], list)
    assert isinstance(response.response[0][0], float)
