from transformers import PreTrainedTokenizer


class WordCountTokenizer(PreTrainedTokenizer):
    """
    This tokenizer does nothing more than split sentences by spaces.

    The `encode` method does not return actual IDs so it can break if an input is expecting integers
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _tokenize(self, text, **kwargs):
        if isinstance(text, list):
            text = " ".join(
                msg["content"]
                for msg in text
                if isinstance(msg, dict) and "content" in msg
            )
        elif isinstance(text, dict) and "content" in text:
            text = text["content"]
        return text.split()

    # need this so the object can be initialized
    def _add_tokens(self, *args, **kwargs):
        pass

    def encode(self, input_data, **kwargs):
        return self._tokenize(input_data)

    def decode(self, tokens, **kwargs):
        return " ".join(tokens)
