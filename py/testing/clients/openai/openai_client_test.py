import pytest
import os
import sys
import json
from typing import Dict, List
from pydantic import BaseModel, Field
from unittest.mock import patch, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from genai_client.text_generation.openai_clients.openai_chat_completion_client import (
    OpenAiChatCompletion,
)
from genai_client.constants import AskModelEngineResponse


class TestOpenAiChatCompletion:
    """Tests for the OpenAiChatCompletion class"""

    def test_initialization(self, openai_config):
        client = OpenAiChatCompletion(
            api_key=openai_config["api_key"],
            model_name=openai_config["model_name"],
            max_tokens=openai_config["max_tokens"],
        )

        assert client is not None
        assert client.model_name == openai_config["model_name"]
        assert hasattr(client, "instruct_operation")
        assert hasattr(client, "chat_operation")

    def test_ask_call(self, openai_config, sample_question):
        """Test the ask_call method returns the expected response structure."""
        client = OpenAiChatCompletion(
            api_key=openai_config["api_key"],
            model_name=openai_config["model_name"],
            max_tokens=openai_config["max_tokens"],
        )

        response = client.ask_call(question=sample_question)

        assert isinstance(response, AskModelEngineResponse)
        assert hasattr(response, "response")
        assert hasattr(response, "prompt_tokens")
        assert hasattr(response, "response_tokens")
        assert isinstance(response.response, str)
        assert isinstance(response.prompt_tokens, int)
        assert isinstance(response.response_tokens, int)

    def test_streaming_vs_non_streaming(self, openai_config, sample_question):
        """
        Test the difference between streaming and non-streaming responses.
        Verifies that streaming outputs to console and both methods return responses.
        use pytest -s flag if you want to see the output of the streaming response printing properly
        """
        client = OpenAiChatCompletion(
            api_key=openai_config["api_key"],
            model_name=openai_config["model_name"],
            max_tokens=openai_config["max_tokens"],
        )

        streaming_kwargs = {
            "messages": [
                {
                    "role": "user",
                    "content": sample_question
                    + " Also think about who had the greatest career in the preimier league and explain in detail please.",
                }
            ],
            "stream": True,
        }
        print("\n STREAMING RESPONSE BELOW: \n")
        streaming_response = client.inference_call(prefix="", **streaming_kwargs)

        non_streaming_kwargs = {
            "messages": [{"role": "user", "content": sample_question}],
            "stream": False,
        }
        non_streaming_response = client.inference_call(
            prefix="", **non_streaming_kwargs
        )

        assert isinstance(streaming_response, str)
        assert isinstance(non_streaming_response, str)
        assert len(streaming_response) > 0
        assert len(non_streaming_response) > 0

    def test_token_limits(self, openai_config, long_text):
        """
        Test that token limits are enforced and warnings are generated.
        Verifies that prompts exceeding token limits are truncated properly.
        """
        client = OpenAiChatCompletion(
            api_key=openai_config["api_key"],
            model_name=openai_config["model_name"],
            max_tokens=openai_config["max_tokens"],
        )

        prompt_payload = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "use", "content": long_text},
        ]

        truncated_prompt, max_tokens, response = client.check_token_limits(
            prompt_payload
        )

        print("TRUNCATED VS PROMPT:", len(truncated_prompt), len(prompt_payload))

        assert len(truncated_prompt) <= len(prompt_payload)
        assert max_tokens >= 0
        assert isinstance(response, AskModelEngineResponse)
        if (
            len(truncated_prompt) < len(prompt_payload)
            or truncated_prompt[1]["content"] != long_text
        ):
            assert hasattr(response, "warning")
            assert "truncated" in response.warning.lower()
