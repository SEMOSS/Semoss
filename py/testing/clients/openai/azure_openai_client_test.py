"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest testing/clients/openai/azure_openai_client_test.py
    pytest -s testing/clients/openai/azure_openai_client_test.py -> For enabling the print statements

Ensure you have a valid `.env` file with the below required keys:
    - AZURE_OPENAI_API_KEY
    - AZURE_OPENAI_GPT_4O_ENDPOINT
    - AZURE_OPENAI_O1_MINI_ENDPOINT

Install pytest if not already installed:
    pip install pytest
"""

import os
import pytest
from dotenv import load_dotenv
from typing import Dict
from genai_client import AzureOpenAiClient

# Load environment variables from .env file
load_dotenv()

AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_GPT_4O_ENDPOINT = os.getenv("AZURE_OPENAI_GPT_4O_ENDPOINT")
AZURE_OPENAI_O1_MINI_ENDPOINT = os.getenv("AZURE_OPENAI_O1_MINI_ENDPOINT")

MAX_TOKENS = 4097
SAMPLE_QUESTION = "What is the capital of France?"


@pytest.fixture(
    params=[
        ("gpt-4o", AZURE_OPENAI_GPT_4O_ENDPOINT),
        ("o1-mini", AZURE_OPENAI_O1_MINI_ENDPOINT),
    ]
)
def azure_openai_client(request):
    """
    Fixture to create an Azure OpenAI client for "gpt-4o" & "o1-mini" Chat Completion models.
    This will run like a loop from the params.
        1. "gpt-4o", AZURE_OPENAI_GPT_4O_ENDPOINT
        2. "o1-mini", AZURE_OPENAI_O1_MINI_ENDPOINT
    """
    model_name, endpoint = request.param
    return AzureOpenAiClient(
        model_name=model_name,
        api_key=AZURE_OPENAI_API_KEY,
        endpoint=endpoint,
        max_tokens=MAX_TOKENS,
    )


def test_azure_openai_ask(azure_openai_client):
    """Test Azure OpenAI client's ask method for "gpt-4o" & "o1-mini" chat completion models"""
    ask_response = azure_openai_client.ask(question=SAMPLE_QUESTION)
    print("ask_response -", ask_response)

    # Checking response type, paramaters and token limits
    assert isinstance(ask_response, Dict), "Response Should be a Dictionary"
    assert set(ask_response.keys()) == {
        "response",
        "numberOfTokensInPrompt",
        "numberOfTokensInResponse",
        "messageType",
    }, "Response Keys Mismatch"

    total_tokens = (
        ask_response["numberOfTokensInPrompt"]
        + ask_response["numberOfTokensInResponse"]
    )
    assert total_tokens <= MAX_TOKENS, "Exceeds token limit"


def test_azure_openai_embeddings(azure_openai_client):
    """Test Azure OpenAI client's embeddings method for "gpt-4o" & "o1-mini" chat completion models"""
    embeddings_response = azure_openai_client.embeddings(
        strings_to_embed=[SAMPLE_QUESTION]
    )
    print("embeddings_response -", embeddings_response)

    # Checking embeddings response type and paramaters
    assert isinstance(embeddings_response, Dict), "Response should be a dictionary"
    assert set(embeddings_response.keys()) == {
        "response",
        "numberOfTokensInPrompt",
        "numberOfTokensInResponse",
    }, "Response Keys Mismatch"
