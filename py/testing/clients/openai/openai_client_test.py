"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest -s testing/clients/openai/openai_client_test.py
    -s => For enabling the print statements

Ensure you have a valid `.env` file with the below required keys:
    - OPENAI_API_KEY

Install pytest if not already installed:
    pip install pytest
"""

import os
import pytest
from dotenv import load_dotenv
from typing import Dict
from genai_client import OpenAiClient

# Load environment variables from .env file
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MAX_TOKENS = 4097
SAMPLE_QUESTION = "What is the capital of India?"


@pytest.fixture
def openai_chat_completion_client():
    """Fixture to create an OpenAI chat completion client instance."""
    return OpenAiClient(
        model_name="gpt-3.5-turbo",
        api_key=OPENAI_API_KEY,
        max_tokens=MAX_TOKENS,
        chat_type="chat-completion",
    )


@pytest.fixture
def openai_completion_client():
    """Fixture to create an OpenAI completion client instance."""
    return OpenAiClient(
        model_name="babbage-002",
        api_key=OPENAI_API_KEY,
        max_tokens=MAX_TOKENS,
        chat_type="completions",
    )


def test_openai_chat_completions(openai_chat_completion_client):
    """Test the OpenAI chat completion client."""
    ask_response = openai_chat_completion_client.ask(question=SAMPLE_QUESTION)
    print("openai chat completion ask_response - ", ask_response)

    embeddings_response = openai_chat_completion_client.embeddings(
        strings_to_embed=[SAMPLE_QUESTION]
    )
    print("openai chat completion embeddings_response - ", embeddings_response)

    # Checking the ask response
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

    # Checking the embeddings response
    assert isinstance(embeddings_response, Dict)
    assert set(embeddings_response.keys()) == {
        "response",
        "numberOfTokensInPrompt",
        "numberOfTokensInResponse",
    }


def test_openai_completions(openai_completion_client):
    """Test the OpenAI completion client."""
    ask_response = openai_completion_client.ask(question=SAMPLE_QUESTION)
    print("openai completion ask_response - ", ask_response)

    embeddings_response = openai_completion_client.embeddings(
        strings_to_embed=[SAMPLE_QUESTION]
    )
    print("openai completion embeddings_response - ", embeddings_response)

    # Checking the ask response
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

    # Checking the embeddings response
    assert isinstance(embeddings_response, Dict)
    assert set(embeddings_response.keys()) == {
        "response",
        "numberOfTokensInPrompt",
        "numberOfTokensInResponse",
    }
