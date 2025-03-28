import os
import pytest
import dotenv
from transformers import AutoTokenizer
from genai_client.tokenizers.huggingface_tokenizer import HuggingfaceTokenizer
# Load environment variables from .env file
dotenv.load_dotenv()


# Load test parameters from .env
ENCODER_NAME = os.getenv("ENCODER_NAME", "bert-base-uncased")
MAX_TOKENS = int(os.getenv("MAX_TOKENS", 512))
MAX_INPUT_TOKENS = int(os.getenv("MAX_INPUT_TOKENS", 256))
CONTEXT_WINDOW = int(os.getenv("CONTEXT_WINDOW", 1024))
MAX_COMPLETION_TOKENS = int(os.getenv("MAX_COMPLETION_TOKENS", 128))

# Initialize tokenizer instance
tokenizer = HuggingfaceTokenizer(
    encoder_name=ENCODER_NAME,
    max_tokens=MAX_TOKENS,
    max_input_tokens=MAX_INPUT_TOKENS,
    context_window=CONTEXT_WINDOW,
    max_completion_tokens=MAX_COMPLETION_TOKENS,
)

@pytest.mark.parametrize(
    "input_text", [
        "",
        "This is a test sentence to check token count."
    ],
)


def test_count_tokens(input_text):
    token_count = tokenizer.count_tokens(input_text)
    print(f"Input: {input_text}, Token Count: {token_count}")
    assert token_count > 0, "Token count should be greater than zero"
    assert isinstance(token_count, int), "Token count should be an integer"


def test_get_tokens():
    input_text = "Testing the tokenizer function."
    tokens = tokenizer.get_tokens(input_text)
    assert isinstance(tokens, list), "Output should be a list of tokens"
    assert len(tokens) > 0, "Token list should not be empty"


def test_get_token_ids():
    input_text = "Tokenizing this sentence."
    token_ids = tokenizer.get_tokens_ids(input_text)
    assert isinstance(token_ids, list), "Output should be a list of token IDs"
    assert all(isinstance(i, int) for i in token_ids), "All elements should be integers"

def test_decode_tokens():
    input_text = "Decoding test."
    token_ids = tokenizer.get_tokens_ids(input_text)
    decoded_text = tokenizer.decode_token_ids(token_ids)
    assert isinstance(decoded_text, str), "Decoded output should be a string"
    assert len(decoded_text) > 0, "Decoded string should not be empty"

def test_model_limits():
    limits = tokenizer.get_model_limits(model_name=ENCODER_NAME)
    print(f"Model Limits for {ENCODER_NAME}: {limits}")
    assert isinstance(limits, dict), "Limits should be a dictionary"
    assert "context_window" in limits, "Dictionary should contain context_window"
    assert "max_completion_tokens" in limits, "Dictionary should contain max_completion_tokens"