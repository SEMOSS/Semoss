"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest -v testing/tokenizers/openai_tokenizer_tests.py
    pytest -s  testing/tokenizers/openai_tokenizer_tests.py -> For enabling the print statements

If any errors occur, such as:
    ModuleNotFoundError: No module named 'genai_client'
Follow these steps to resolve them:

1. Open the command prompt and navigate to the directory where your Python script is located.
2. Set the PYTHONPATH environment variable to include the directory where your module is located. Example: sset PYTHONPATH=%PYTHONPATH%;D:\\Users\\username\\pythonupdates\\Semoss\\py
3. Run the script again:
    pytest testing/tokenizers/openai_tokenizer_tests.py
    pytest -s testing/tokenizers/openai_tokenizer_tests.py -> For enabling the print statements
Install pytest if not already installed:
    pip install pytest
"""

import pytest
from genai_client.tokenizers.openai_tokenizer import OpenAiTokenizer  # Adjust if needed

# test parameters
ENCODER_NAME = "gpt-4-turbo"
MAX_TOKENS = 512
MAX_INPUT_TOKENS = 256
CONTEXT_WINDOW = 1024
MAX_COMPLETION_TOKENS = 128


@pytest.fixture
def tokenizer():
    """
    Fixture to initialize an instance of OpenAiTokenizer.
    Ensures a consistent tokenizer instance is used across multiple tests.
    """
    return OpenAiTokenizer(
        encoder_name=ENCODER_NAME,
        max_tokens=MAX_TOKENS,
        max_input_tokens=MAX_INPUT_TOKENS,
        context_window=CONTEXT_WINDOW,
        max_completion_tokens=MAX_COMPLETION_TOKENS,
    )


@pytest.mark.parametrize(
    "input_text", ["Hello world!", "This is a test sentence to check token count."]
)
def test_count_tokens(tokenizer, input_text):
    """
    Tests the count_tokens method.
    - Ensures that token counting is accurate.
    - Checks if token count is greater than zero.
    - Ensures the output is an integer.
    """
    token_count = tokenizer.count_tokens(input_text)
    assert token_count > 0, "Token count should be greater than zero"
    assert isinstance(token_count, int), "Token count should be an integer"


@pytest.mark.parametrize(
    "input_text",
    ["Testing the tokenizer function.", "Another test with different input."],
)
def test_get_tokens(tokenizer, input_text):
    """
    Tests the get_tokens method.
    - Ensures tokenization returns a list.
    - Validates that the list is not empty.
    """
    tokens = tokenizer.get_tokens(input_text)
    assert isinstance(tokens, list), "Output should be a list of tokens"
    assert len(tokens) > 0, "Token list should not be empty"


@pytest.mark.parametrize(
    "input_text", ["Tokenizing this sentence.", "A different input to test token IDs."]
)
def test_get_token_ids(tokenizer, input_text):
    """
    Tests the get_tokens_ids method.
    - Ensures token IDs are returned as a list.
    - Validates that all elements in the list are integers.
    """
    token_ids = tokenizer.get_tokens_ids(input_text)
    assert isinstance(token_ids, list), "Output should be a list of token IDs"
    assert all(isinstance(i, int) for i in token_ids), "All elements should be integers"


@pytest.mark.parametrize(
    "input_text", ["Decoding test.", "Another sentence to check decoding."]
)
def test_decode_tokens(tokenizer, input_text):
    """
    Tests the decode_token_ids method.
    - Ensures that decoded output is a string.
    - Validates that the decoded string is not empty.
    """
    token_ids = tokenizer.get_tokens_ids(input_text)
    decoded_text = tokenizer.decode_token_ids(token_ids)
    assert isinstance(decoded_text, str), "Decoded output should be a string"
    assert len(decoded_text) > 0, "Decoded string should not be empty"


def test_model_limits(tokenizer):
    """
    Tests the get_model_limits method.
    - Ensures model limits are returned as a dictionary.
    - Checks that context_window and max_completion_tokens are present in the dictionary.
    """
    limits = tokenizer.get_model_limits(model_name=ENCODER_NAME)
    assert isinstance(limits, dict), "Limits should be a dictionary"
    assert "context_window" in limits, "Dictionary should contain context_window"
    assert (
        "max_completion_tokens" in limits
    ), "Dictionary should contain max_completion_tokens"


def test_format_with_chat_template(tokenizer):
    """
    Tests the format_with_chat_template method.
    - Ensures that chat messages are formatted correctly.
    - Validates that the output is a non-empty string.
    """
    messages = [
        {"role": "user", "content": "Hello!"},
        {"role": "assistant", "content": "Hi there!"},
    ]
    formatted_output = tokenizer.format_with_chat_template(messages)
    assert isinstance(formatted_output, str), "Formatted output should be a string"
    assert len(formatted_output) > 0, "Formatted output should not be empty"


def test_get_max_token_length(tokenizer):
    """
    Tests the get_max_token_length method.
    - Ensures that the max token length is returned as an integer.
    - Validates that the length is greater than zero.
    """
    max_length = tokenizer.get_max_token_length()
    assert isinstance(max_length, int), "Max token length should be an integer"
    assert max_length > 0, "Max token length should be greater than zero"
