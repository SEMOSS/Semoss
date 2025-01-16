import logging
from typing import Union, List, Dict, Optional
import tiktoken
from .abstract_tokenizer import AbstractTokenizer
from .model_limits_config import get_model_limits

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

        self.tokens_per_message = 0
        self.tokens_per_name = 0
        # https://github.com/openai/openai-cookbook/blob/main/examples/How_to_count_tokens_with_tiktoken.ipynb
        if ("gpt-4" in encoder_name) or ("gpt-3.5-turbo" in encoder_name):
            self.tokens_per_message = 3
            self.tokens_per_name = 1
        elif encoder_name == "gpt-3.5-turbo-0301":
            self.tokens_per_message = (
                4  # every message follows <|start|>{role/name}\n{content}<|end|>\n
            )
            self.tokens_per_name = -1  # if there's a name, the role is omitted

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
                logger.warning("Warning: model not found. Using cl100k_base encoding.")
                return tiktoken.get_encoding("cl100k_base")

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

    def count_tokens(self, input: Union[List[Dict], str]) -> int:
        num_tokens = 0
        if isinstance(input, list):
            for message in input:
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
        elif isinstance(input, str):
            num_tokens = len(self.get_tokens_ids(input))

        return num_tokens

    def get_tokens_ids(self, input: Union[List[Dict], str]) -> List[int]:
        if isinstance(input, list):
            input = " ".join(
                [
                    message.get("content") or message.get("text")
                    for message in input
                    if "content" in message or "text" in message
                ]
            )
        elif isinstance(input, dict):
            input = input["content"]

        return self.tokenizer.encode(input)

    def get_tokens(self, input: Union[List[Dict], str]) -> List[str]:
        return [
            self.tokenizer.decode([tokenId]) for tokenId in self.get_tokens_ids(input)
        ]

    def get_max_token_length(self) -> int:
        if self.max_tokens == None:
            # lets hope the third party packages are correct
            return self.tokenizer.max_token_value
        else:
            return self.max_tokens

    def get_max_input_token_length(self) -> int:
        return self.max_input_tokens

    def decode_token_ids(self, input: List[int]) -> str:
        return self.tokenizer.decode(input)

    def get_model_limits(self, model_name: Optional[str]) -> Dict[str, int]:
        """
        Get the context window and max completion tokens limits for a given model.
        Always checks for the new variable names first, then the old variable names, and finally the model_limits_config.
        The new variable names are context_window and max_completion_tokens.
        The old variable names are max_tokens and max_input_tokens.
        Args:
            model_name Optional[str]: The model name to get the limits for.
        Returns:
            Dict[str, int]: A dictionary containing the context window as context_window and max completion tokens limits as max_completion_tokens.
        """
        model_name = model_name or self.encoder_name
        model_limits_config = get_model_limits(model_name)
        model_limits = {
            "context_window": None,
            "max_completion_tokens": None,
        }
        # 1. We want to check if the new variable names are being used in the SMSS files first
        if self.context_window:
            model_limits["context_window"] = self.context_window
        if self.max_completion_tokens:
            model_limits["max_completion_tokens"] = self.max_completion_tokens

        # 2. If the new variable names are not being used, we want to check if the old variable names are being used
        if model_limits["context_window"] == None and self.max_tokens != None:
            model_limits["context_window"] = self.max_tokens
        if (
            model_limits["max_completion_tokens"] == None
            and self.max_input_tokens != None
        ):
            model_limits["max_completion_tokens"] = self.max_input_tokens

        # 3. Idk if we even want to use third party packages for context window anymore but this is here just in case
        if model_limits["context_window"] == None:
            model_limits["context_window"] = self.get_max_token_length()

        # 4. Finally, if either the context_window or max_completion_tokens are still None, we want to use the model_limits_config
        if model_limits["context_window"] == None:
            model_limits["context_window"] = model_limits_config["context_window"]
        if model_limits["max_completion_tokens"] == None:
            model_limits["max_completion_tokens"] = model_limits_config[
                "max_completion_tokens"
            ]

        return model_limits
