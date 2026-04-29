"""
Lightweight word-count tokenizer with no external dependencies.

This tokenizer provides a simple fallback for API-based models (OpenAI, BEDROCK, VERTEX)
that don't need HuggingFace transformers compatibility. It splits text by whitespace
and provides both AbstractTokenizer and basic HuggingFace-compatible interfaces.

Unlike WordCountTokenizer, this does NOT import transformers, avoiding torch dependency.
"""

from typing import Union, List, Dict


class LocalWordCountTokenizer:
    """
    Lightweight tokenizer that counts words (space-delimited tokens).

    Provides compatibility with:
    1. AbstractTokenizer interface (count_tokens method)
    2. Basic HuggingFace interface (encode/decode methods)
    3. LangChain text splitters (.tokenizer attribute access)

    Does NOT import transformers or torch.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize tokenizer. Accepts args for compatibility but doesn't use them.
        """
        self.model_max_length = 999999
        self._vocab_size = 50000  # Arbitrary large number

    def _tokenize(self, text: str, **kwargs) -> List[str]:
        """
        Tokenize text by splitting on whitespace.

        Args:
            text: String to tokenize
            **kwargs: Ignored, for compatibility

        Returns:
            List of tokens (words)
        """
        # Handle different input formats
        if isinstance(text, list):
            # Handle list of message dicts
            text = " ".join(
                msg["content"]
                for msg in text
                if isinstance(msg, dict) and "content" in msg
            )
        elif isinstance(text, dict) and "content" in text:
            # Handle single message dict
            text = text["content"]

        # Split on whitespace
        return text.split()

    def encode(self, input_data: Union[str, List[Dict], Dict], **kwargs) -> List[str]:
        """
        Encode text to tokens (for HuggingFace compatibility).

        Note: Returns token strings, not integer IDs, unlike real tokenizers.
        This is sufficient for counting purposes.

        Args:
            input_data: Text string or message list/dict
            **kwargs: Ignored, for compatibility

        Returns:
            List of tokens (words)
        """
        return self._tokenize(input_data)

    def decode(self, tokens: List[str], **kwargs) -> str:
        """
        Decode tokens back to text (for HuggingFace compatibility).

        Args:
            tokens: List of token strings
            **kwargs: Ignored, for compatibility

        Returns:
            Joined text string
        """
        return " ".join(tokens)

    def count_tokens(self, input_data: Union[str, List[Dict], Dict]) -> int:
        """
        Count tokens in text (for AbstractTokenizer compatibility).

        This is the main method used by text_splitting.py and other SEMOSS code.

        Args:
            input_data: Text string or message list/dict

        Returns:
            Number of tokens (words)
        """
        return len(self.encode(input_data))

    def get_tokens_ids(self, input_data: Union[str, List[Dict], Dict]) -> List[str]:
        """
        Get token IDs (for AbstractTokenizer compatibility).

        Note: Returns token strings, not integer IDs. This is sufficient for
        OpenAiTokenizer's count_tokens() which just needs len(get_tokens_ids()).

        Args:
            input_data: Text string or message list/dict

        Returns:
            List of tokens (words)
        """
        return self.encode(input_data)

    @property
    def tokenizer(self):
        """
        Return self for .tokenizer attribute access.

        This allows code like:
            text_splitter = TokenTextSplitter.from_huggingface_tokenizer(
                cfg_tokenizer.tokenizer, ...
            )

        Returns:
            Self
        """
        return self

    @property
    def vocab_size(self) -> int:
        """Vocabulary size (for compatibility)."""
        return self._vocab_size

    def __len__(self) -> int:
        """Return model max length (for compatibility)."""
        return self.model_max_length

    # Additional compatibility methods
    def _add_tokens(self, *args, **kwargs):
        """No-op for compatibility with PreTrainedTokenizer interface."""
        pass
