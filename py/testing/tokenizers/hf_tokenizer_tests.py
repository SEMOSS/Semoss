"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest -v testing/tokenizers/hf_tokenizer_tests.py
    pytest -s  testing/tokenizers/hf_tokenizer_tests.py -> For enabling the print statements

Install pytest if not already installed:
    pip install pytest
"""

import pytest
from genai_client.tokenizers.huggingface_tokenizer import HuggingfaceTokenizer

ENCODER_NAME = "gpt-4-turbo"
MAX_TOKENS = 512
MAX_INPUT_TOKENS = 256
CONTEXT_WINDOW = 1280
MAX_COMPLETION_TOKENS = 128

SAMPLE_INPUT_TEXT = "This is a sample input text for testing the HuggingfaceTokenizer."


@pytest.fixture
def tokenizer():
    """Fixture to initialize the HuggingfaceTokenizer instance."""
    return HuggingfaceTokenizer(
        encoder_name=ENCODER_NAME,
        max_tokens=MAX_TOKENS,
        max_input_tokens=MAX_INPUT_TOKENS,
        context_window=CONTEXT_WINDOW,
        max_completion_tokens=MAX_COMPLETION_TOKENS,
    )


@pytest.mark.parametrize(
    "input_text", ["This is a test sentence to check token count."]
)
def test_count_tokens(tokenizer, input_text):
    """
    Tests the count_tokens method.
    - Ensures that token counting is accurate.
    - Checks if token count is greater than zero.
    - Ensures the output is an integer.
    """
    token_count = tokenizer.count_tokens(input_text)
    assert isinstance(token_count, int), "Token count should be an integer"
    assert token_count > 0, "Token count should be greater than zero"


def test_get_tokens(tokenizer):
    """
    Tests the get_tokens method.
    - Verifies that the tokenizer returns a valid list of tokens.
    - Ensures the output is a list.
    - Confirms that the list is not empty.
    """
    tokens = tokenizer.get_tokens(SAMPLE_INPUT_TEXT)
    assert isinstance(tokens, list), "Output should be a list of tokens"
    assert len(tokens) > 0, "Token list should not be empty"


def test_get_token_ids(tokenizer):
    """
    Tests the get_tokens_ids method.
    - Checks if the token IDs are returned correctly.
    - Ensures the output is a list.
    """
    token_ids = tokenizer.get_tokens_ids(SAMPLE_INPUT_TEXT)
    assert isinstance(token_ids, list), "Output should be a list of token IDs"


def test_decode_tokens(tokenizer):
    """
    Tests the decode_token_ids method.
    - Verifies that the decoding of token IDs works correctly.
    - Ensures the decoded output is a string.
    - Confirms that the decoded string is not empty.
    """
    token_ids = tokenizer.get_tokens_ids(SAMPLE_INPUT_TEXT)
    decoded_text = tokenizer.decode_token_ids(token_ids)
    assert isinstance(decoded_text, str), "Decoded output should be a string"
    assert len(decoded_text) > 0, "Decoded string should not be empty"


def test_model_limits(tokenizer):
    """
    Tests the get_model_limits method.
    - Verifies correct retrieval of model limits.
    - Ensures the output is a dictionary.
    - Confirms the presence of specific keys: context_window, max_completion_tokens.
    """
    limits = tokenizer.get_model_limits(model_name=ENCODER_NAME)
    assert isinstance(limits, dict), "Limits should be a dictionary"
    assert "context_window" in limits, "Dictionary should contain context_window"
    assert (
        "max_completion_tokens" in limits
    ), "Dictionary should contain max_completion_tokens"
