from typing import Dict, List
from transformers import AutoTokenizer
from .abstract_tokenizer import AbstractTokenizer


class HuggingfaceTokenizer(AbstractTokenizer):

    def __init__(
        self, encoder_name: str, max_tokens: int, max_input_tokens: int = None
    ):
        self.encoder_name = encoder_name
        super().__init__(
            encoder_name=encoder_name,
            max_tokens=max_tokens,
            max_input_tokens=max_input_tokens,
        )

    def _get_tokenizer(self, encoder_name: str):
        """
        Returns the appropriate encoding based on the given encoding type (either an encoding string or a model name).
        """
        try:
            return AutoTokenizer.from_pretrained(encoder_name)
        except:
            # this is the defacto default tokenizer
            from transformers import PreTrainedTokenizer

            class WordCountTokenizer(PreTrainedTokenizer):
                """
                This tokenizer does nothing more than split sentences by spaces.

                The `encode` method does not return actual IDs so it can break if an input is expecting integers
                """

                def __init__(self, *args, **kwargs):
                    super().__init__(*args, **kwargs)

                def _tokenize(self, text, **kwargs):
                    # Simple whitespace-based tokenization
                    return text.split()

                # need this so the object can be initialized
                def _add_tokens(self, *args, **kwargs):
                    pass

                def encode(self, input, **kwargs):
                    return self._tokenize(input)

                def decode(self, tokens, **kwargs):
                    return " ".join(tokens)

            return WordCountTokenizer()

    def format_with_chat_template(self, messages: List[Dict]) -> str:
        """
        Identifies and applies the appropriate chat template based on model type
        Args:
            messages: List of message dictionaries with 'role' and 'content'
        Returns:
            str: Formatted prompt string
        """
        # Checking if it's a Llama model since thse have a specific template
        if "llama" in self.encoder_name.lower():
            formatted_messages = []
            for msg in messages:
                if msg["role"] == "system":
                    formatted_messages.append(
                        f"<<SYS>>\n{msg['content']}\n<</SYS>>\n\n"
                    )
                elif msg["role"] == "user":
                    formatted_messages.append(f"[INST] {msg['content']} [/INST]\n")
                elif msg["role"] == "assistant":
                    formatted_messages.append(f"{msg['content']}\n")
            return "".join(formatted_messages)

        # For other models use this generic template
        # Eventually we can add more templates for other models we support here..
        generic_template = """{% for message in messages %}{% if message["role"] == "system" %}System: {{ message["content"] }}
                              {% elif message["role"] == "user" %}User: {{ message["content"] }}
                              {% elif message["role"] == "assistant" %}Assistant: {{ message["content"] }}
                              {% endif %}{% endfor %}"""

        if hasattr(self.tokenizer, "apply_chat_template"):
            self.tokenizer.chat_template = generic_template
            return self.tokenizer.apply_chat_template(messages, tokenize=False)

        # Fallback if the tokenizer does not have the apply_chat_template method
        return "\n".join(f"{msg['role']}: {msg['content']}" for msg in messages)

    def count_tokens(self, input: str) -> int:
        """Use the model tokenizer to get the number of tokens"""
        input_tokens_ids = self.get_tokens_ids(input=input)
        return len(input_tokens_ids)

    def get_tokens_ids(self, input: str, add_special_tokens: bool = False) -> List[int]:
        return self.tokenizer.encode(input, add_special_tokens=add_special_tokens)

    def get_tokens(self, input: str) -> List[str]:
        return self.tokenizer.tokenize(input)

    def get_max_token_length(self) -> int:
        if self.max_tokens == None:
            # lets hope the third party packages are correct
            return self.tokenizer.model_max_length
        else:
            return self.max_tokens

    def get_max_input_token_length(self) -> int:
        return self.max_input_tokens

    def decode_token_ids(self, input: List[int]) -> str:
        return self.tokenizer.decode(input)
