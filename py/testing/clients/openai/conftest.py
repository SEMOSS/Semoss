import os
import pytest
from dotenv import load_dotenv
import json

load_dotenv()


@pytest.fixture(scope="session")
def openai_config():
    """Fixture for OpenAI configuration from environment variables"""
    return {
        "api_key": os.environ.get("OPENAI_API_KEY"),
        "model_name": os.environ.get("OPENAI_MODEL_NAME", "gpt-3.5-turbo"),
        "max_tokens": int(os.environ.get("openAI_MAX_TOKENS", "4097")),
    }


@pytest.fixture
def sample_question():
    return "Who is the all time leading scorer in the Premier League?"


@pytest.fixture
def sample_schema():
    return {
        "type": "object",
        "properties": {
            "player": {"type": "string"},
            "total_goals": {"type": "integer"},
        },
        "required": ["player", "total_goals"],
    }


@pytest.fixture
def sample_chat_history():
    return [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Hello, how are you?"},
        {
            "role": "assistant",
            "content": "I'm doing well, thank you for asking. How can I help you today?",
        },
    ]


@pytest.fixture
def long_text():
    """Provides a long text to test token limit"""
    return " ".join(["This is  atetst sentance for token limit testing."] * 1000)
