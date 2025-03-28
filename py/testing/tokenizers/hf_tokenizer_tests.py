"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest testing/tokenizers/hf_tokenizer_tests.py
    pytest -s  testing/tokenizers/hf_tokenizer_tests.py -> For enabling the print statements

Install pytest if not already installed:
    pip install pytest
"""



import pytest
from genai_client.tokenizers.huggingface_tokenizer import HuggingfaceTokenizer

# test parameters
ENCODER_NAME = "bert-base-uncased"
MAX_TOKENS =  512
MAX_INPUT_TOKENS = 256
CONTEXT_WINDOW = 1024
MAX_COMPLETION_TOKENS = 128

#sample input text for testing
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

@pytest.mark.parametrize("input_text", [
    "",  # Empty input
    "This is a test sentence to check token count."
])

def test_count_tokens(tokenizer, input_text):
    """Test the count_tokens method to ensure tokens are counted correctly."""
    token_count = tokenizer.count_tokens(input_text)
    print(f"Input: {input_text}, Token Count: {token_count}")
    assert isinstance(token_count, int), "Token count should be an integer"


def test_get_tokens(tokenizer):
    """Test the get_tokens method to verify tokenization returns a valid list."""
    tokens = tokenizer.get_tokens(SAMPLE_INPUT_TEXT)
    assert isinstance(tokens, list), "Output should be a list of tokens"
    assert len(tokens) > 0, "Token list should not be empty"

def test_get_token_ids(tokenizer):
    """Test the get_tokens_ids method to check if token IDs are returned correctly."""
    token_ids = tokenizer.get_tokens_ids(SAMPLE_INPUT_TEXT)
    assert isinstance(token_ids, list), "Output should be a list of token IDs"
    assert all(isinstance(i, int) for i in token_ids), "All elements should be integers"

def test_decode_tokens(tokenizer):
    """Test the decode_token_ids method to verify proper decoding."""
    token_ids = tokenizer.get_tokens_ids(SAMPLE_INPUT_TEXT)
    decoded_text = tokenizer.decode_token_ids(token_ids)
    assert isinstance(decoded_text, str), "Decoded output should be a string"
    assert len(decoded_text) > 0, "Decoded string should not be empty"

def test_model_limits(tokenizer):
    """Test the get_model_limits method to verify correct model limits retrieval."""
    limits = tokenizer.get_model_limits(model_name=ENCODER_NAME)
    print(f"Model Limits for {ENCODER_NAME}: {limits}")
    assert isinstance(limits, dict), "Limits should be a dictionary"
    assert "context_window" in limits, "Dictionary should contain context_window"
    assert "max_completion_tokens" in limits, "Dictionary should contain max_completion_tokens"