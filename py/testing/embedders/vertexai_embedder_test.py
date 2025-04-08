import pytest
import os
from typing import Dict
from genai_client import VertexAiEmbedder
from genai_client.constants import EmbeddingsModelEngineResponse
from dotenv import load_dotenv

# Load environment variables from .env.example file
load_dotenv("testing/.env")

# VertexAI Embedder
@pytest.fixture
def vertex_embedder():
    key_path = os.getenv("EMBEDDER_VERTEX_SERVICE_ACCOUNT_FILE_PATH")
    model_name = os.getenv("EMBEDDER_VERTEX_MODEL_NAME")
    region = os.getenv("EMBEDDER_VERTEX_REGION")

    return VertexAiEmbedder(
        model_name=model_name,
        service_account_key_file=key_path,
        region=region,
    )
def test_vertex_embedder_ask(vertex_embedder):
    """
    Test the Vertex AI Embedder's ask method.
    - Ensures proper return type.
    - Validates fallback generation message.
    """
    ask_response = vertex_embedder.ask(question="what is the capital of france?")
    assert isinstance(ask_response, Dict)
    assert ask_response["response"] == "This model does not support text generation."

def test_vertex_embedder_embeddings(vertex_embedder):
    """
    Test the embeddings_call method for Vertex AI Embedder.
    - Ensures the embeddings are in correct format.
    """
    embeddings_response = vertex_embedder.embeddings_call(strings_to_embed=["What is the capital of France?"])
    assert isinstance(embeddings_response, EmbeddingsModelEngineResponse)
 