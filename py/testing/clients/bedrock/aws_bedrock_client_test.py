"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest -s testing/clients/bedrock/aws_bedrock_client_test.py
    -s => For enabling the print statements

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

# Load environment variables from .env file
load_dotenv()

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
    """Test the AWS Bedrock Claude chat completion."""
    ask_response = aws_bedrock_client.ask_call(question=SAMPLE_QUESTION)
    print("bedrock ask_response - ", ask_response)

    assert isinstance(
        ask_response, AskModelEngineResponse
    ), "Response Shoule be an instance of AskModelEngineResponse"
    assert set(ask_response.to_dict().keys()) == {
        "response",
        "numberOfTokensInPrompt",
        "numberOfTokensInResponse",
    }, "Response Keys Mismatch"
