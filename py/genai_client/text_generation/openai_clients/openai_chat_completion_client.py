import math
from typing import List, Optional, Tuple, Any
import json
from pydantic import BaseModel
from .operations.instruct import Instruct
from .operations.chat import Chat
from .abstract_openai_client import AbstractOpenAiClient
from ...constants import (
    AskModelEngineResponse,
    InstructModelEngineResponse,
)
from ...model_limits import ModelLimits


class OpenAiChatCompletion(AbstractOpenAiClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.instruct_operation = Instruct(client=self)
        self.chat_operation = Chat(client=self)

    def instruct(self, **kwargs) -> InstructModelEngineResponse:
        return self.instruct_operation.instruct(**kwargs)

    def ask_call(self, **kwargs) -> AskModelEngineResponse:
        return self.chat_operation.ask(**kwargs)

    def _validate_structured_input(self, schema) -> Tuple[str, Any]:
        """
        Validate the input schema for structured output.
        Returns a tuple with the schema type as string and the schema instance.
        Convert to Dict if JSON..
        """
        if isinstance(schema, str):
            # Attempting to parse as JSON
            try:
                return "dict", json.loads(schema)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON string provided for schema.")
        elif isinstance(schema, dict):
            # Validating that dict can be serialized to JSON
            try:
                json.dumps(schema)
                return ("dict", schema)
            except TypeError:
                raise ValueError("Schema dict contains non-serializable values.")
        elif isinstance(schema, BaseModel) or (
            isinstance(schema, type) and issubclass(schema, BaseModel)
        ):
            # checking if Pydantic model
            return ("pydantic", schema)
        else:
            raise ValueError("Schema must be a JSON string, dict, or Pydantic model.")

    def _create_structured_response_format(
        self, schema_type, schema
    ) -> Tuple[str, Any]:
        """
        Create the structure request format for structured output.
        Returns a tuple with the parameter name as string and the parameter value.
        These cases are different based on whether we are hitting OpenAI versus vLLM
        and whether the schema is a dict or Pydantic model.
        """
        if self.model_type == "OPEN_AI":
            return (
                (
                    "response_format",
                    {
                        "type": "json_schema",
                        "json_schema": {"name": "custom_schema", "schema": schema},
                    },
                )
                if schema_type == "dict"
                else ("response_format", schema)  # Pydantic model
            )
        else:
            # For vLLM it is the same for both dict and Pydantic model
            return ("extra_body", {"guided_json": schema})

    def _get_structured_output_response(self, params):
        """
        Make the structured output call to the correct endpoint based on model type.
        vLLM requires a different endpoint...
        """
        response = (
            self.client.beta.chat.completions.parse(model=self.model_name, **params)
            if self.model_type == "OPEN_AI"
            else self.client.chat.completions.create(model=self.model_name, **params)
        )
        try:
            return response.choices[0].message.content
        except Exception as e:
            raise ValueError(f"Failed to extract structured output: {e}")

    def _structured_output_call(self, **kwargs):
        """
        1. Validate the schema and identify the schema type
        2. Create the structured response format with the correct parameter name
        3. Make the structured output call to the correct endpoint based on model type
        4. Extract the structured output from the response
        """
        schema = kwargs.pop("schema")
        # Validating the schema and identifying the type
        schema_type, schema = self._validate_structured_input(schema)
        # Creating the structured response format with the correct parameter name
        structured_param_name, structured_param_value = (
            self._create_structured_response_format(schema_type, schema)
        )
        # Making new params so I can use dynamic keys
        params = {structured_param_name: structured_param_value, **kwargs}
        return self._get_structured_output_response(params)

    def _update_model_specific_kwargs(self, **kwargs) -> dict:
        """
        Update the kwargs based on the model name to ensure compatibility with the model's capabilities.
        Returns:
            dict: Updated kwargs
        """
        updated_kwargs = kwargs.copy()

        # Handle o1-mini (doesn't support system/developer roles)
        if self.model_name.startswith("o1-mini"):
            # Remove temperature - only 1.0 is supported
            if "temperature" in updated_kwargs and updated_kwargs["temperature"] != 1.0:
                del updated_kwargs["temperature"]

            updated_kwargs["stream"] = False

            # Convert system/developer messages to user messages
            if "messages" in updated_kwargs:
                messages = updated_kwargs["messages"]
                for i, msg in enumerate(messages):
                    if msg.get("role") in ["system", "developer"]:
                        original_role = msg.get("role").upper()
                        messages[i]["role"] = "user"
                        messages[i][
                            "content"
                        ] = f"{original_role}: {messages[i]['content']}"
                updated_kwargs["messages"] = messages

        # Handle regular o1 models
        elif self.model_name == "o1" or self.model_name.startswith("o1-preview"):
            # Temperature - only 1.0 is supported
            if "temperature" in updated_kwargs and updated_kwargs["temperature"] != 1.0:
                del updated_kwargs["temperature"]

            updated_kwargs["stream"] = False

        # Handle o3-mini
        elif self.model_name.startswith("o3-mini"):
            # Remove temperature - only 1.0 is supported
            if "temperature" in updated_kwargs and updated_kwargs["temperature"] != 1.0:
                del updated_kwargs["temperature"]

            updated_kwargs["stream"] = False

        return updated_kwargs

    def inference_call(self, prefix: str, **kwargs) -> Tuple[str, int, str]:
        final_query = ""
        response_tokens = None
        messageType = "CHAT"
        # For Remote Client Server Models
        if "base_url" in kwargs:
            self.client.base_url, self.client.api_key = kwargs.pop("base_url"), "EMPTY"

        # Process structured output
        has_schema = kwargs.get("schema", False)
        if has_schema:
            return self._structured_output_call(**kwargs)

        kwargs["stream"] = kwargs.get("stream", True)

        # If "tool_choice" is in kwargs, set stream to False
        if "tool_choice" in kwargs:
            kwargs["stream"] = False

        # Check if 'max_tokens' exists in kwargs and remove it, saving its value
        max_tokens = kwargs.pop("max_tokens", None)
        # If 'max_tokens' was found and 'max_completion_tokens' is not already in kwargs, set it
        if max_tokens is not None and "max_completion_tokens" not in kwargs:
            kwargs["max_completion_tokens"] = max_tokens

        # Update model specific kwargs
        kwargs = self._update_model_specific_kwargs(**kwargs)

        response = self.client.chat.completions.create(model=self.model_name, **kwargs)

        if "tool_choice" in kwargs:
            tools_call = response.choices[0].message.tool_calls
            toolResult = []
            if tools_call:  # Check if tools_call is not empty
                for tool_call in tools_call:
                    toolResult.append(
                        {
                            "id": tool_call.id,
                            "name": tool_call.function.name,
                            "arguments": tool_call.function.arguments,
                        }
                    )
                final_query = toolResult
                messageType = "TOOL"
            else:
                final_query = response.choices[0].message.content
            response_tokens = response.usage.completion_tokens
        else:
            if kwargs["stream"]:
                for chunk in response:
                    if chunk.choices and (len(chunk.choices) > 0):
                        content = chunk.choices[0].delta.content
                        if content != None:
                            final_query += content
                            print(prefix + content, end="")
            else:
                final_query = response.choices[0].message.content
                response_tokens = response.usage.completion_tokens

        return final_query, response_tokens, messageType

    def _truncate_messages(
        self,
        messages: List[dict],
        tokens_over_limit: int,
        char_multiplier: int,
        truncation_strategy: str = "standard",
    ) -> List[dict]:
        """
        Truncate messages in the ChatML format based on the number of tokens exceeding the safe context window.
        This accepts a character multiplier to determine how many characters to remove from the message content.
        The multiplier is used to estimate the number of characters to remove based on the number of tokens over the limit.
        The truncation strategy is set to "standard" by default, but can be changed to other strategies in the future.
        - The standard strategy always attempts to truncate the oldest messages first. It will remove an entire message if it is oldest and truncation would not be enough to fit the context window.
        - Otherwise it will truncate the message content from the end of the message until it is under the limit.

        ChatML Example:
        [{'role': 'system', 'content': 'You are a helpful assistant. '}, {'role': 'user', 'content': 'What weighs more, a pound of feathers or a pound of bricks?'}]

        Args:
            messages (List[dict]): ChatML history to be truncated
            tokens_over_limit (int): The number of tokens exceeding the safe context window
            char_multiplier (int): The multiplier for the number of characters to remove

        Returns:
            List[dict]: An updated list of ChatML messages with the truncated content
        """
        chars_to_remove = char_multiplier * tokens_over_limit
        updated_messages = []
        if truncation_strategy.lower() == "standard":
            # Determine if the first message is a system prompt
            maintain_sys_prompt = messages and messages[0].get("role") == "system"
            # assuming we have [message1, message2, message3, message4]
            if maintain_sys_prompt:
                # Process order: message2, message3, message1, message4
                process_order = list(range(1, len(messages) - 1)) + [
                    0,
                    len(messages) - 1,
                ]
            else:
                # Process order: message1, message2, message3, message4
                process_order = list(range(len(messages)))

            for index, value in enumerate(process_order):
                # note we use the value in this case
                # since here we might have array [1,2,0,3]
                # and we want to grab from the original message value
                message = messages[value]
                # Grabbing the content of the message
                message_content = message.get("content", "")
                # Measuring the length of the message content
                message_length = len(message_content)

                if message_length < chars_to_remove:
                    # If the message length doesn't cover the character estimation, remove it
                    # We remove the entire ChatML message by not adding it to the updated messages list
                    chars_to_remove -= message_length
                    continue
                else:
                    # If the message length is greater than the character estimation we truncate
                    truncated_message = message_content[:-chars_to_remove]
                    if message.get("role") == "system":
                        updated_messages.insert(
                            0, {"role": message["role"], "content": truncated_message}
                        )
                    else:
                        updated_messages.append(
                            {"role": message["role"], "content": truncated_message}
                        )

                    # Adding the remaining messages to the updated messages list
                    # note here we only want to continue after this position
                    # so via index, not value
                    if index + 1 < len(messages):
                        for j in range(index + 1, len(messages)):
                            if messages[j].get("role") == "system":
                                updated_messages.insert(0, messages[j])
                            else:
                                updated_messages.append(messages[j])

                    return updated_messages
        else:
            # If the truncation strategy is not standard... we can implement other strategies here
            # For now, we will just return the original messages
            # FYI: We won't hit this block
            return messages

    def check_token_limits(
        self,
        messages: List,
        max_tokens: int,
        context_window: int,
    ) -> Tuple[List, int, AskModelEngineResponse]:
        """
        Calculate tokens in the prompt and adjust max_completion_tokens to fit within context window.
        Args:
            messages (List): The prompt in the form of chat history
            max_tokens (int): The maximum tokens for completion
            context_window (int): The model's context window size
        Returns:
            Tuple[List, int, AskModelEngineResponse]: The truncated messages, adjusted max_tokens, and response object
        """
        model_engine_response = AskModelEngineResponse()
        warnings = []

        # Saving 10% of the context window for completion tokens at minimum
        # We can consider updating this in the future to something more nuanced
        safe_window = int(context_window * 0.9)

        # Get token count for all messages
        message_tokens = self.tokenizer.count_tokens(messages)

        updated_messages = messages.copy()

        # The total tokens we have to remove (if a positive number)
        tokens_over_limit = message_tokens - safe_window

        if tokens_over_limit > 0:
            # This is a truncation strategy I am using to try to optimize the number of tokens we are removing
            # We start with a conservative character multiplier of 3 and increase by 1 until we are under the limit
            for i in range(3, 6, 1):
                truncated_messages = self._truncate_messages(
                    updated_messages, tokens_over_limit, i
                )
                updated_token_count = self.tokenizer.count_tokens(truncated_messages)
                if updated_token_count <= safe_window:
                    updated_messages = truncated_messages
                    break

            updated_token_count = self.tokenizer.count_tokens(updated_messages)

            # If we get here that means a 6 character multiplier was not enough to get under the limit
            # There is likely some other problem with the messages
            if updated_token_count > safe_window:
                print(
                    f"There is a problem. The updated token count is still over the limit after truncation."
                )
            else:
                message_tokens = updated_token_count

        # Calculating the max completion tokens we have available from the context window
        # I need a buffer of 5% to be safe due to discrepancies in the tokenization process
        final_max_tokens = math.floor(
            min(context_window - message_tokens, max_tokens) * 0.95
        )  # 5% buffer

        if final_max_tokens > max_tokens:
            final_max_tokens = max_tokens

        model_engine_response.prompt_tokens = message_tokens

        if warnings:
            model_engine_response.warning = "\n\n".join(warnings)

        return updated_messages, final_max_tokens, model_engine_response
