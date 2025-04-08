"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest -v testing/embedders/azure_openai_embedder_test.py
    pytest -s testing/embedders/azure_openai_embedder_test.py -> For enabling the print statements

Ensure you have a valid `.env` file with the below required keys:
    - AZURE_OPENAI_API_KEY

Install pytest if not already installed:
    pip install pytest
"""


import pytest
import os
from typing import Dict
from genai_client import AzureOpenAiEmbedder
from genai_client.constants import EmbeddingsModelEngineResponse
from dotenv import load_dotenv

# Load environment variables from .env.example file
load_dotenv("testing/.env")

# Azure OpenAI Embedder
@pytest.fixture
def azure_embedder():
    api_key = os.getenv("EMBEDDER_AZURE_API_KEY")
    endpoint = os.getenv("EMBEDDER_AZURE_ENDPOINT")
    model_name = os.getenv("EMBEDDER_AZURE_MODEL_NAME")
    api_version = os.getenv("EMBEDDER_AZURE_API_VERSION")

    return AzureOpenAiEmbedder(
        model_name=model_name,
        api_key=api_key,
        endpoint=endpoint,
        api_version=api_version,
    )

def test_azure_embedder_ask(azure_embedder):
    """
    Test the Azure OpenAI Embedder ask method.
    - Ensures it returns a dictionary.
    - Validates fallback message for unsupported generation.
    """
    ask_response = azure_embedder.ask(question="what is the capital of france?")
    assert isinstance(ask_response, Dict)
    assert ask_response["response"] == "This model does not support text generation."

def test_azure_embedder_embeddings(azure_embedder):
    """
    Test the embeddings_call method of Azure Embedder.
    - Ensures valid response structure.
    """
    embeddings_response = azure_embedder.embeddings_call(strings_to_embed=["What is the capital of France?"])
    assert isinstance(embeddings_response, EmbeddingsModelEngineResponse)

 