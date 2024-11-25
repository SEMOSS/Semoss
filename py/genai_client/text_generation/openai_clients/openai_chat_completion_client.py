from typing import List, Optional, Tuple
from .operations.instruct import Instruct
from .operations.chat import Chat
from .abstract_openai_client import AbstractOpenAiClient
from ...constants import (
    AskModelEngineResponse,
    InstructModelEngineResponse,
)


class OpenAiChatCompletion(AbstractOpenAiClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.instruct_operation = Instruct(client=self)
        self.chat_operation = Chat(client=self)

    def instruct(self, **kwargs) -> InstructModelEngineResponse:
        return self.instruct_operation.instruct(**kwargs)

    def ask_call(self, **kwargs) -> AskModelEngineResponse:
        return self.chat_operation.ask(**kwargs)

    def inference_call(self, prefix: str, **kwargs) -> str:
        final_query = ""

        kwargs["stream"] = kwargs.get("stream", True)
        openai_response = self.client.chat.completions.create(
            model=self.model_name, **kwargs
        )

        if self.model_name == "o1-preview" or self.model_name == "o1-mini":
            max_tokens = kwargs.pop("max_tokens")
            kwargs["max_completion_tokens"] = max_tokens

        if kwargs["stream"]:
            for chunk in openai_response:
                if chunk.choices and (len(chunk.choices) > 0):
                    response = chunk.choices[0].delta.content
                    if response != None:
                        final_query += response
                        print(prefix + response, end="")
        else:
            if "function_call" in kwargs.keys():
                final_query = openai_response.choices[0].message.function_call.arguments
            else:
                final_query = openai_response.choices[0].message.content

        return final_query

    def check_token_limits(
        self,
        prompt_payload: List,
        user_max_tokens: Optional[int] = None,
    ) -> Tuple[str, int, AskModelEngineResponse]:
        """
        The purpose of this method is to calculate the number of tokens in the prompt and adjust the max_completion_tokens to fit within the context window.
        Args:
            prompt_payload (List): The prompt in the form of chat history
        Returns:
            Tuple[str, int, AskModelEngineResponse]: The truncated prompt, the adjusted max_completion_tokens, and the model engine response dataclass
        """
        model_engine_response = AskModelEngineResponse()
        warnings = []

        # 1. Get our prompt token count
        num_tokens_in_prompt = len(
            self.tokenizer._get_tokenizer(self.model_name).encode(
                self.tokenizer.format_with_chat_template(prompt_payload)
            )
        )

        # 2. Get model limits
        model_limits = self.tokenizer.get_model_limits(self.model_name)
        context_window = model_limits["context_window"]
        max_completion_tokens = model_limits["max_completion_tokens"]
        # If the user provides a token limit for completions we can honor it as long as it is less than the model limit
        if user_max_tokens is not None and user_max_tokens < max_completion_tokens:
            max_completion_tokens = user_max_tokens

        # 3. Define safety margins.. I need this for discrepancy between token counts and actual text length
        SAFETY_PERCENTAGE = 0.01  # 1% for token count safety
        TRUNCATION_THRESHOLD = 0.9  # 90% for truncation decisions

        safety_margin = int(context_window * SAFETY_PERCENTAGE)
        safe_prompt_tokens = num_tokens_in_prompt + safety_margin

        # 4. Check if we need to truncate
        if safe_prompt_tokens > (context_window * TRUNCATION_THRESHOLD):
            token_counter = 0
            truncation_limit = int(context_window * TRUNCATION_THRESHOLD)

            for i, message in enumerate(prompt_payload):
                message_tokens = self.tokenizer.count_tokens(message)
                next_count = token_counter + message_tokens

                if next_count > truncation_limit:
                    # Calculate safe tokens for this message
                    available_tokens = truncation_limit - token_counter
                    if available_tokens > 0:
                        # Truncate this message
                        tokens = self.tokenizer.get_tokens(message["content"])
                        tokens = tokens[:available_tokens]
                        prompt_payload[i]["content"] = "".join(tokens)
                        prompt_payload = prompt_payload[: i + 1]
                    else:
                        # No room for this message
                        prompt_payload = prompt_payload[:i]

                    warnings.append("Prompt was truncated to fit within context window")

                    # Recalculate prompt tokens after truncation
                    num_tokens_in_prompt = len(
                        self.tokenizer._get_tokenizer(self.model_name).encode(
                            self.tokenizer.format_with_chat_template(prompt_payload)
                        )
                    )
                    safe_prompt_tokens = num_tokens_in_prompt + safety_margin
                    break

                token_counter = next_count

        # 5. Calculate available context and final tokens
        available_context = context_window - safe_prompt_tokens
        final_max_tokens = min(available_context, max_completion_tokens)
        final_max_tokens = max(0, final_max_tokens)

        model_engine_response.prompt_tokens = num_tokens_in_prompt
        if len(warnings) > 0:
            model_engine_response.warning = "\\n\\n".join(warnings)

        return prompt_payload, int(final_max_tokens), model_engine_response
