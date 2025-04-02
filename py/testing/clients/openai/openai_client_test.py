"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest testing/clients/openai/openai_client_test.py
    pytest -s testing/clients/openai/openai_client_test.py -> For enabling the print statements

Ensure you have a valid `.env` file with the below required keys:
    - OPENAI_API_KEY

Install pytest if not already installed:
    pip install pytest
"""

import os, json
import pytest
from dotenv import load_dotenv
from typing import Dict
from genai_client import OpenAiClient

# Load environment variables from .env.example file
load_dotenv("testing/.env.example")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MAX_TOKENS = 4097
SAMPLE_QUESTION = "What is the capital of France?"


@pytest.fixture
def openai_chat_completion_client():
    """Fixture to create an OpenAI chat completion client instance."""
    return OpenAiClient(
        model_name="gpt-4o",
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


def _get_json_schema():
    """Defining the json schema as we want in the JSON structured response."""
    schema = {
        "type": "object",
        "properties": {
            "players": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "position": {"type": "string"},
                        "country": {"type": "string"},
                        "skill": {"type": "integer"},
                    },
                    "required": ["name", "position", "country", "skill"],
                },
            }
        },
        "required": ["players"],
    }
    json_schema = {"name": "response_schema", "schema": schema}

    return json_schema


def test_openai_chat_completions_json_structured_response(
    openai_chat_completion_client,
):
    """
    Test the ask response of OpenAI chat completion client for the JSON structured response using assertions.
        - Checking the type of response
        - Checking the required keys of response
        - Checking the JSON structures for the required keys in response
        - Checking the token limit of prompt and response against model token limit
    """
    SAMPLE_QUESTION = "Name a few Manchester United players you know with their positions, countries, and skill ratings."
    # Add "stream=True" or "stream=False" in the ask call if required
    ask_response = openai_chat_completion_client.ask(
        question=SAMPLE_QUESTION,
        response_format={
            "type": "json_schema",
            "json_schema": _get_json_schema(),
        },
    )
    print(
        "\n openai_chat_completions_json_structured_response ask_response - ",
        ask_response,
    )

    # Assertions for the structure of the ask response
    assert isinstance(ask_response, Dict), "Response Should be a Dictionary"
    assert set(ask_response.keys()) == {
        "response",
        "numberOfTokensInPrompt",
        "numberOfTokensInResponse",
        "messageType",
    }, "Response Keys Mismatch"

    structured_response = json.loads(
        ask_response["response"]
    )  # converting into json format

    assert "players" in structured_response
    for player in structured_response["players"]:
        assert "name" in player
        assert "position" in player
        assert "country" in player
        assert "skill" in player

    total_tokens = (
        ask_response["numberOfTokensInPrompt"]
        + ask_response["numberOfTokensInResponse"]
    )
    assert total_tokens <= MAX_TOKENS, "Exceeds token limit"


def test_openai_chat_completions_text_response(openai_chat_completion_client):
    """
    Test the ask response of OpenAI chat completion client for the normal text response using assertions.
        - Checking the type of response
        - Checking the required keys of response
        - Checking the token limit of prompt and response against model token limit
    """
    # Add "stream=True" or "stream=False" in the ask call if required
    ask_response = openai_chat_completion_client.ask(question=SAMPLE_QUESTION)
    print("\n openai_chat_completions_text_response ask_response - ", ask_response)

    # Assertions for the text response
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


def test_openai_chat_completions_embeddings_response(openai_chat_completion_client):
    """
    Test the embeddings response of OpenAI chat completion client for the normal text using assertions.
        - Checking the type of response
        - Checking the required keys of response
    """
    embeddings_response = openai_chat_completion_client.embeddings(
        strings_to_embed=[SAMPLE_QUESTION]
    )
    print(
        "\n openai_chat_completions_text_response embeddings_response - ",
        embeddings_response,
    )

    # Assertions for the embeddings response
    assert isinstance(embeddings_response, Dict)
    assert set(embeddings_response.keys()) == {
        "response",
        "numberOfTokensInPrompt",
        "numberOfTokensInResponse",
    }


def test_openai_completions_for_ask(openai_completion_client):
    """
    Test the ask response of OpenAI completion client using assertions.
        - Checking the type of response
        - Checking the required keys of response
        - Checking the token limit of prompt and response against model token limit
    """
    # Add "stream=True" or "stream=False" in the ask call if required
    ask_response = openai_completion_client.ask(question=SAMPLE_QUESTION)
    print("\n openai completion ask_response - ", ask_response)

    # Assertions for the ask response
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


def test_openai_completions_for_embeddings(openai_completion_client):
    """
    Test the embeddings response of OpenAI completion client using assertions.
        - Checking the type of response
        - Checking the required keys of response
    """
    embeddings_response = openai_completion_client.embeddings(
        strings_to_embed=[SAMPLE_QUESTION]
    )
    print("\n openai completion embeddings_response - ", embeddings_response)

    # Assertions for the embeddings response
    assert isinstance(embeddings_response, Dict)
    assert set(embeddings_response.keys()) == {
        "response",
        "numberOfTokensInPrompt",
        "numberOfTokensInResponse",
    }
