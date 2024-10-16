from typing import List, Tuple
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
        self, prompt_payload: List, max_new_tokens: int
    ) -> Tuple[str, int, AskModelEngineResponse]:
        """
        The method is used to truncate the the number of tokens in the prompt and adjust the `max_new_tokens` so that the text generation does not fail.
        Instead we rather will send back a flag indicating adjustments have
        """
        model_engine_response = AskModelEngineResponse()
        warnings = []

        specific_tokenizer = self.tokenizer._get_tokenizer(self.model_name)
        if hasattr(specific_tokenizer, "apply_chat_template"):
            # Apply the chat template to the prompt if no chat template was provided
            if specific_tokenizer.chat_template == None:
                specific_tokenizer.chat_template = "chatml"
            # there is a apply chat template available for this model - transformers tokenizer
            prompt = specific_tokenizer.apply_chat_template(
                prompt_payload, tokenize=False
            )
            # use the models tokenizer to get the number of tokens in the prompt
            prompt_tokens = self.tokenizer.get_tokens_ids(prompt)
            num_token_in_prompt = len(prompt_tokens)
        else:
            # use the models tokenizer to get the number of tokens in the prompt
            # this is likely directly openai
            num_token_in_prompt = self.tokenizer.count_tokens(prompt_payload)

        max_prompt_tokens = self.tokenizer.get_max_input_token_length()

        if max_prompt_tokens != None:
            max_tokens = max_prompt_tokens
        else:
            max_tokens = self.tokenizer.get_max_token_length()

        # perform the checks using max_tokens
        if num_token_in_prompt > max_tokens:
            token_counter = 0
            for i, message in enumerate(prompt_payload):
                num_message_tokens = self.tokenizer.count_tokens(message)
                token_counter += num_message_tokens

                if token_counter > max_tokens:
                    # calculate how many tokens we can take from this message
                    num_tokens_to_remove = token_counter - max_tokens

                    message_tokens = self.tokenizer.get_tokens(message["content"])
                    message_tokens = message_tokens[
                        : len(message_tokens) - num_tokens_to_remove
                    ]

                    prompt_payload[i]["content"] = "".join(message_tokens)
                    prompt_payload = prompt_payload[: i + 1]
                    warnings.append(f"The prompt was truncated to:\n {prompt_payload}")
                    num_token_in_prompt = self.tokenizer.count_tokens(prompt_payload)
                    break

        # now we also need to make sure the max_new_tokens passed in is adjusted
        if max_new_tokens > (max_tokens - num_token_in_prompt):
            max_new_tokens = max_new_tokens + (
                (max_tokens - num_token_in_prompt) - max_new_tokens
            )
            warnings.append(f"max_new_tokens was changed to: {max_new_tokens}")

        model_engine_response.prompt_tokens = num_token_in_prompt
        if len(warnings) > 0:
            model_engine_response.warning = "\\n\\n".join(warnings)

        return prompt_payload, int(max_new_tokens), model_engine_response
