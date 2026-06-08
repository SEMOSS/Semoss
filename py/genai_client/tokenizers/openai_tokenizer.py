import logging
from typing import Union, List, Dict
import tiktoken
from .abstract_tokenizer import AbstractTokenizer

logger = logging.getLogger(__name__)


class OpenAiTokenizer(AbstractTokenizer):

    def __init__(
        self,
        encoder_name: str,
        max_tokens: int,
        max_input_tokens: int = None,
        context_window: int = None,
        max_completion_tokens: int = None,
    ):
        super().__init__(
            encoder_name=encoder_name,
            max_tokens=max_tokens,
            max_input_tokens=max_input_tokens,
            context_window=context_window,
            max_completion_tokens=max_completion_tokens,
        )

        # https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
        self.tokens_per_message, self.tokens_per_name = self._set_token_adjustments(
            encoder_name
        )

    def _set_token_adjustments(self, encoder_name: str):
        """Sets token adjustment values based on the OpenAI model."""
        if ("gpt-4" in encoder_name) or ("gpt-3.5-turbo" in encoder_name):
            return 3, 1
        elif encoder_name == "gpt-3.5-turbo-0301":
            return 4, -1
        return 0, 0

    def _get_tokenizer(self, encoder_name: str):
        """
        Returns the appropriate encoding based on the given encoding type (either an encoding string or a model name).
        """
        if "k_base" in encoder_name:
            return tiktoken.get_encoding(encoder_name)
        else:
            try:
                return tiktoken.encoding_for_model(encoder_name)
            except KeyError:
                # Handle gpt-4o model explicitly if not recognized in older tiktoken versions
                if "gpt-4o" in encoder_name:
                    return tiktoken.get_encoding("o200k_base")
                logger.warning("Model not found. Using WordCountTokenizer fallback.")
                # Standard model
                from .word_count_tokenizer import WordCountTokenizer
                return WordCountTokenizer()

    def format_with_chat_template(self, messages: List[Dict]) -> str:
        """
        Applies the appropriate chat template for OpenAI models.
        Handles both regular messages and messages with image content.
        """
        # Check if any message contains image content
        has_image_content = any(
            isinstance(msg.get("content"), list) for msg in messages
        )

        # We can't use the chat template for messages with image content
        if has_image_content:
            return ""  # Return empty since we can't be using this for token counting

        if hasattr(self.tokenizer, "apply_chat_template"):
            if self.tokenizer.chat_template is None:
                self.tokenizer.chat_template = "chatml"
            return self.tokenizer.apply_chat_template(messages, tokenize=False)
        return "\n".join(f"{msg['role']}: {msg['content']}" for msg in messages)

    def count_tokens(self, input_data: Union[List[Dict], str]) -> int:
        num_tokens = 0
        if isinstance(input_data, list):
            for message in input_data:
                num_tokens += self.tokens_per_message
                for key, value in message.items():
                    if key == "content":
                        if isinstance(value, list):
                            # Handle structured content with images
                            for content_item in value:
                                if content_item["type"] == "text":
                                    num_tokens += len(
                                        self.get_tokens_ids(content_item["text"])
                                    )
                                # Skip token counting for image_url content
                        else:
                            num_tokens += len(self.get_tokens_ids(str(value)))
                    elif key == "name":
                        num_tokens += self.tokens_per_name
            num_tokens += 3  # every reply is primed with <|start|>assistant<|message|>
        elif isinstance(input_data, str):
            num_tokens = len(self.get_tokens_ids(input_data))

        return num_tokens

    def get_tokens_ids(self, input_data: Union[List[Dict], str]) -> List[int]:
        if isinstance(input_data, list):
            input_data = " ".join(
                [
                    msg.get("content") or msg.get("text")
                    for msg in input_data
                    if "content" in msg or "text" in msg
                ]
            )
        elif isinstance(input_data, dict):
            input_data = input_data.get("content")

        return self.tokenizer.encode(input_data)

    def get_tokens(self, input_data: Union[List[Dict], str]) -> List[str]:
        return [
            self.tokenizer.decode([token_id])
            for token_id in self.get_tokens_ids(input_data)
        ]

    def get_max_token_length(self) -> int:
        if self.max_tokens == None:
            # lets hope the third party packages are correct
            return self.tokenizer.max_token_value
        else:
            return self.max_tokens

    def get_max_input_token_length(self) -> int:
        return self.max_input_tokens

    def decode_token_ids(self, token_ids: List[int]) -> str:
        return self.tokenizer.decode(token_ids)

    def _safe_encode(self, text: str) -> List[int]:
        """
        Convert text -> list[int] without special tokens.
        """
        return self.tokenizer.encode(text)

    def _safe_decode(self, tokens: List[int]) -> str:
        """
        Convert list[int] -> text.  Just delegates to tiktoken.
        """
        return self.tokenizer.decode(tokens)
