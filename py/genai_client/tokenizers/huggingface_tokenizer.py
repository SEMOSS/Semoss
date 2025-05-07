from typing import Dict, List, Union
from transformers import AutoTokenizer
from .abstract_tokenizer import AbstractTokenizer


class HuggingfaceTokenizer(AbstractTokenizer):

    def __init__(
        self,
        encoder_name: str,
        max_tokens: int,
        max_input_tokens: int = None,
        context_window: int = None,
        max_completion_tokens: int = None,
    ):
        self.encoder_name = encoder_name
        super().__init__(
            encoder_name=encoder_name,
            max_tokens=max_tokens,
            max_input_tokens=max_input_tokens,
            context_window=context_window,
            max_completion_tokens=max_completion_tokens,
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

    def count_tokens(self, input_data: Union[List[Dict], str]) -> int:
        """Use the model tokenizer to get the number of tokens, including image tokens"""
        token_count = len(self.get_tokens_ids(input_data=input_data))

        # Add tokens for images if present
        if isinstance(input_data, list):
            for message in input_data:
                if isinstance(message, dict):
                    content = message.get("content")
                    if isinstance(content, list):
                        for item in content:
                            if (
                                isinstance(item, dict)
                                and item.get("type") == "image_url"
                            ):
                                # This is rather arbitrary but its a conservative estimate
                                token_count += 1000

        return token_count

    def get_tokens_ids(self, input_data: Union[List[Dict], str]) -> List[int]:
        if isinstance(input_data, list):
            contents = []
            for message in input_data:
                if isinstance(message, dict):
                    content = message.get("content") or message.get("text")
                    if isinstance(content, list):
                        text_contents = []
                        for item in content:
                            if isinstance(item, dict):
                                if item.get("type") == "text":
                                    text_contents.append(item.get("text", ""))
                        content = " ".join(text_contents) if text_contents else ""
                    contents.append(content)
            input_data = " ".join([str(c) for c in contents if c is not None])
        elif isinstance(input_data, dict):
            content = input_data.get("content") or input_data.get("text")
            if isinstance(content, list):
                text_contents = []
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        text_contents.append(item.get("text", ""))
                input_data = " ".join(text_contents) if text_contents else ""
            else:
                input_data = content

        return self.tokenizer.encode(str(input_data) if input_data is not None else "")

    def get_tokens(self, input_data: Union[List[Dict], str]) -> List[str]:
        return [
            self.tokenizer.decode([token_id])
            for token_id in self.get_tokens_ids(input_data)
        ]

    def get_max_token_length(self) -> int:
        if self.max_tokens == None:
            # lets hope the third party packages are correct
            return self.tokenizer.model_max_length
        else:
            return self.max_tokens

    def get_max_input_token_length(self) -> int:
        return self.max_input_tokens

    def decode_token_ids(self, token_ids: List[int]) -> str:
        return self.tokenizer.decode(token_ids)

    def _safe_encode(self, text: str) -> List:
        """
        Encode without special tokens; works for real HF tokenizers
        and for the fallback WordCountTokenizer.
        """
        try:
            return self.tokenizer.encode(text, add_special_tokens=False)
        except TypeError:  # WordCountTokenizer accepts no kwarg
            return self.tokenizer.encode(text)

    def _safe_decode(self, tokens: List) -> str:
        # WordCountTokenizer returns str tokens; join them
        if tokens and isinstance(tokens[0], str):
            return " ".join(tokens)
        # HF tokenizer returns ints; use its native decode
        return self.tokenizer.decode(tokens)
