import pytest
import os
from typing import Dict
from genai_client import OpenAiEmbedder
from genai_client.constants import EmbeddingsModelEngineResponse
from dotenv import load_dotenv

# Load environment variables from .env.example fil
load_dotenv("testing/.env")

@pytest.fixture
def openai_embedder():
    api_key = os.getenv("EMBEDDER_OPENAI_API_KEY")
    model_name = os.getenv("EMBEDDER_OPENAI_MODEL_NAME")
    return OpenAiEmbedder(model_name=model_name, api_key=api_key)

def test_openai_embedder_ask(openai_embedder):
    """
    Test the OpenAIEmbedder ask method.
    - Ensures it returns a dictionary.
    - Verifies fallback response for unsupported generation.
    """
    ask_response = openai_embedder.ask(question="what is the capital of france?")
    assert isinstance(ask_response, Dict)
    assert ask_response["response"] == "This model does not support text generation."

def test_openai_embedder_embeddings(openai_embedder):
    """
    Test the embeddings_call method for OpenAI.
    - Checks if embedding vector is returned.
    - Validates the expected embedding length (1536).
    """
    embeddings_response = openai_embedder.embeddings_call(strings_to_embed=["What is the capital of France?"])
    assert isinstance(embeddings_response, EmbeddingsModelEngineResponse)
    assert len(embeddings_response.response) == 1
    assert len(embeddings_response.response[0]) == 1536
 