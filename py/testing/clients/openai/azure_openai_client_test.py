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


@pytest.fixture
def azure_openai_gpt_4o_client():
    """Fixture to create an Azure OpenAI client for "gpt-4o" Chat Completion model."""
    return AzureOpenAiClient(
        model_name="gpt-4o",
        api_key=AZURE_OPENAI_API_KEY,
        endpoint=AZURE_OPENAI_GPT_4O_ENDPOINT,
        max_tokens=MAX_TOKENS,
    )


@pytest.fixture
def azure_openai_o1_mini_client():
    """Fixture to create an Azure OpenAI client for "o1-mini" Chat Completion model."""
    return AzureOpenAiClient(
        model_name="o1-mini",
        api_key=AZURE_OPENAI_API_KEY,
        endpoint=AZURE_OPENAI_O1_MINI_ENDPOINT,
        max_tokens=MAX_TOKENS,
    )


def test_azure_openai_gpt_4o_chat_completion(azure_openai_gpt_4o_client):
    """Test Azure OpenAI client's ask method for "gpt-4o" chat completion model"""
    ask_response = azure_openai_gpt_4o_client.ask(question=SAMPLE_QUESTION)
    print("\n gpt-4o ask_response -", ask_response)

    """Test Azure OpenAI client's embeddings method for "gpt-4o" chat completion model"""
    embeddings_response = azure_openai_gpt_4o_client.embeddings(
        strings_to_embed=[SAMPLE_QUESTION]
    )
    print("\n gpt-4o embeddings_response -", embeddings_response)

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

    # Checking embeddings response type and paramaters
    assert isinstance(embeddings_response, Dict), "Response should be a dictionary"
    assert set(embeddings_response.keys()) == {
        "response",
        "numberOfTokensInPrompt",
        "numberOfTokensInResponse",
    }, "Response Keys Mismatch"


def test_azure_openai_o1_mini_chat_completion(azure_openai_o1_mini_client):
    """Test Azure OpenAI client's ask method for "o1-mini" chat completion model"""
    ask_response = azure_openai_o1_mini_client.ask(question=SAMPLE_QUESTION)
    print("\n o1-mini ask_response -", ask_response)

    """Test Azure OpenAI client's embeddings method for "o1-mini" chat completion model"""
    embeddings_response = azure_openai_o1_mini_client.embeddings(
        strings_to_embed=[SAMPLE_QUESTION]
    )
    print("\n o1-mini embeddings_response -", embeddings_response)

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

    # Checking embeddings response type and paramaters
    assert isinstance(embeddings_response, Dict), "Response should be a dictionary"
    assert set(embeddings_response.keys()) == {
        "response",
        "numberOfTokensInPrompt",
        "numberOfTokensInResponse",
    }, "Response Keys Mismatch"
