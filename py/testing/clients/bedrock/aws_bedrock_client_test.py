"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest testing/clients/bedrock/aws_bedrock_client_test.py
    pytest -s testing/clients/bedrock/aws_bedrock_client_test.py => For enabling the print statements

Ensure you have a valid `.env` file with the below required keys:
    - AWS_SECRET_KEY
    - AWS_ACCESS_KEY

Install pytest if not already installed:
    pip install pytest
"""

import os
import pytest
from dotenv import load_dotenv
from typing import Dict
from genai_client import BedrockClient
from genai_client.constants import AskModelEngineResponse

# Load environment variables from .env.example file
load_dotenv("testing/.env")

AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
SAMPLE_QUESTION = "What is the capital of France?"


@pytest.fixture
def aws_bedrock_client():
    """Fixture to create an AWS bedrock client instance."""
    return BedrockClient(
        modelId="anthropic.claude-instant-v1",
        secret_key=AWS_SECRET_KEY,
        access_key=AWS_ACCESS_KEY,
        region="us-east-1",
    )


def test_aws_bedrock_claude(aws_bedrock_client):
    """
    Test the ask call of AWS Bedrock Claude client using assertions.
        - Checking the type of response
        - Checking the required key parameters of response
    Test the embeddings call of AWS Bedrock Claude client using assertions.
        - Checking the type of response
        - Checking the required key parameters of response
    """
    ask_response = aws_bedrock_client.ask_call(question=SAMPLE_QUESTION)
    print("\n bedrock ask_response - ", ask_response)

    embeddings_response = aws_bedrock_client.embeddings(
        strings_to_embed=[SAMPLE_QUESTION]
    )
    print("\n bedrock embeddings_response - ", embeddings_response)

    # Assertions for the ask response
    assert isinstance(
        ask_response, AskModelEngineResponse
    ), "Response Should be an instance of AskModelEngineResponse"
    assert set(ask_response.to_dict().keys()) == {
        "response",
        "numberOfTokensInPrompt",
        "numberOfTokensInResponse",
        "messageType",
    }, "Response Keys Mismatch"

    # Assertions for the embeddings response
    assert isinstance(embeddings_response, Dict)
    assert set(embeddings_response.keys()) == {
        "response",
        "numberOfTokensInPrompt",
        "numberOfTokensInResponse",
    }
