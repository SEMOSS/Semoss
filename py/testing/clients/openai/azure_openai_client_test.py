"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest -s testing/clients/openai/azure_openai_client_test.py
    -s => For enabling the print statements

Ensure you have a valid `.env` file with the below required keys:
    - AZURE_OPENAI_API_KEY
    - AZURE_OPENAI_ENDPOINT

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
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
MAX_TOKENS = 4097
SAMPLE_QUESTION = "What is the capital of Germany?"


@pytest.fixture
def azure_chat_completion_client():
    """Fixture to create an Azure OpenAI client instance."""
    return AzureOpenAiClient(
        model_name="gpt-4o",
        api_key=AZURE_OPENAI_API_KEY,
        endpoint=AZURE_OPENAI_ENDPOINT,
        max_tokens=MAX_TOKENS,
    )


def test_azure_openai_chat_completions(azure_chat_completion_client):
    """Test the Azure OpenAI chat completion client"""
    ask_response = azure_chat_completion_client.ask(question=SAMPLE_QUESTION)
    print("azure chat completion ask_response - ", ask_response)

    assert isinstance(ask_response, Dict), "Response Should be a Dictionary"
    assert set(ask_response.keys()) == {
        "response",
        "numberOfTokensInPrompt",
        "numberOfTokensInResponse",
    }, "Response Keys Mismatch"

    total_tokens = (
        ask_response["numberOfTokensInPrompt"]
        + ask_response["numberOfTokensInResponse"]
    )
    assert total_tokens <= MAX_TOKENS, "Exceeds token limit"
