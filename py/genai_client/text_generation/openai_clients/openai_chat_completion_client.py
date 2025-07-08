import math
from typing import List, Tuple, Any
import json
from pydantic import BaseModel
from .operations.instruct import Instruct
from .operations.chat import Chat
from .abstract_openai_client import AbstractOpenAiClient
from ...constants import (
    AskModelEngineResponse,
    InstructModelEngineResponse,
    IMAGE_ENCODED,
    IMAGE_URL,
)
from utils.util import string_to_bool


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

    def _get_structured_output_response(self, params) -> Tuple[str, int, str]:
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
            content = response.choices[0].message.content
            response_tokens = response.usage.completion_tokens
            return content, response_tokens, "CHAT"
        except Exception as e:
            raise ValueError(f"Failed to extract structured output: {e}")

    def _structured_output_call(self, **kwargs) -> Tuple[str, int, str]:
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

    def resolve_token_param_naming(self, **kwargs) -> dict:
        """
        Resolves the token parameter naming for different OpenAI-compatible APIs.
        Some APIs use max_tokens while others use max_completion_tokens.
        Set use_max_tokens=True in config to use max_tokens parameter.
        """
        use_max_tokens_param = self.use_max_tokens_param
        if isinstance(use_max_tokens_param, str):
            use_max_tokens_param = string_to_bool(use_max_tokens_param)

        max_completion_tokens = kwargs.pop("max_completion_tokens", None)
        max_tokens = kwargs.pop("max_tokens", None)

        # Determine which value to use (prefer the one that was actually set)
        token_limit = max_completion_tokens or max_tokens

        if not token_limit:
            return kwargs

        # Set the appropriate parameter based on API preference
        if use_max_tokens_param:
            kwargs["max_tokens"] = token_limit
        else:
            kwargs["max_completion_tokens"] = token_limit

        return kwargs

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

        # If tools is defined but tool_choice is not
        # but also check that tools is not None or empty
        # set tool_choice to auto
        if "tool_choice" not in kwargs and "tools" in kwargs:
            if kwargs["tools"] is not None and len(kwargs["tools"]) > 0:
                kwargs["tool_choice"] = "auto"

        # If "tool_choice" is in kwargs, set stream to False
        if "tool_choice" in kwargs:
            kwargs["stream"] = False

        # Checking if use_max_tokens was set in SMSS to support non-updated API's (e.g. nvidia nims)
        kwargs = self.resolve_token_param_naming(**kwargs)

        # Update model specific kwargs
        kwargs = self._update_model_specific_kwargs(**kwargs)

        response = self.client.chat.completions.create(model=self.model_name, **kwargs)

        if "tool_choice" in kwargs:
            tools_call = response.choices[0].message.tool_calls
            toolResult = []
            if tools_call:  # Check if tools_call is not empty
                for tool_call in tools_call:
                    # TODO: we should not create our own format
                    # TODO: we should not create our own format
                    # TODO: we should not create our own format
                    # TODO: we should not create our own format
                    # TODO: we should not create our own format
                    toolResult.append(
                        {
                            "id": tool_call.id,
                            "type": tool_call.type,
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

    def _truncate_by_tokens(
        self,
        messages: List[dict],
        safe_window: int,
        keep_system: bool = True,
    ) -> List[dict]:
        """
        Returns a ChatML history whose **total** token count
        is ≤ safe_window.
        Oldest non-system messages are dropped first; when only
        one message needs trimming we cut tokens from its *start*.
        """

        # --- Tokenise *once* ----------------------------------------
        toks_per_msg = []
        total = 0
        for m in messages:
            toks = self.tokenizer._safe_encode(m["content"])
            toks_per_msg.append(toks)
            total += len(toks)

        if total <= safe_window:
            return messages  # nothing to do

        to_cut = total - safe_window  # exact excess
        keep_flags = [True] * len(messages)

        # --- Build truncation order ---------------------------------
        # oldest->newest
        # if keep_system, then we will maintain it up until the last message
        order = list(range(len(messages)))
        if keep_system and messages and messages[0]["role"] == "system":
            # assuming we have [system_prompt, message2, message3, message4]
            # Process order: message2, message3, system_prompt, message4
            order = list(range(1, len(messages) - 1)) + [
                0,
                len(messages) - 1,
            ]

        # --- Drop or trim -------------------------------------------
        for idx in order:
            if to_cut == 0:
                break
            toks = toks_per_msg[idx]
            if len(toks) <= to_cut:
                # drop whole message
                keep_flags[idx] = False
                to_cut -= len(toks)
            else:
                # keep tail part of this message
                toks_per_msg[idx] = toks[-(len(toks) - to_cut) :]
                to_cut = 0

        # --- Re-build ChatML ----------------------------------------
        new_messages = []
        for keep, m, toks in zip(keep_flags, messages, toks_per_msg):
            if not keep:
                continue
            m = m.copy()
            m["content"] = self.tokenizer._safe_decode(toks)
            new_messages.append(m)
        return new_messages

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
            updated_messages = self._truncate_by_tokens(updated_messages, safe_window)

            updated_token_count = self.tokenizer.count_tokens(updated_messages)

            message_tokens = updated_token_count

        # Calculating the max completion tokens we have available from the context window
        # I need a buffer of 5% to be safe due to discrepancies in the tokenization process
        final_max_tokens = math.floor(
            min(context_window - message_tokens, max_tokens) * 0.95
        )  # 5% buffer
        # If the final max tokens is greater than the passed in max tokens, we set it to passed in max tokens
        # This is to ensure we are not exceeding the max tokens set by the user or config
        if final_max_tokens > max_tokens:
            final_max_tokens = max_tokens

        model_engine_response.prompt_tokens = message_tokens

        if warnings:
            model_engine_response.warning = "\n\n".join(warnings)

        return updated_messages, final_max_tokens, model_engine_response

    def _handle_image_params(
        self, question: str, fill_variables: dict, message_payload
    ):
        """
        Handle image parameters in the payload.
        """
        image_payload = [{"type": "text", "text": question}]

        key_to_pop = IMAGE_ENCODED if IMAGE_ENCODED in fill_variables else IMAGE_URL
        images = fill_variables.pop(key_to_pop)
        if isinstance(images, str):
            if key_to_pop == IMAGE_ENCODED:
                image_url = {"url": f"data:image/png;base64,{images}"}
            else:
                image_url = {"url": images}
            image_payload.append({"type": "image_url", "image_url": image_url})
            message_payload.append({"role": "user", "content": image_payload})
            return message_payload, fill_variables
        elif isinstance(images, list):
            for image in images:
                if key_to_pop == IMAGE_ENCODED:
                    image_url = {"url": f"data:image/png;base64,{image}"}
                else:
                    image_url = {"url": image}
                image_payload.append({"type": "image_url", "image_url": image_url})
            message_payload.append({"role": "user", "content": image_payload})
            return message_payload, fill_variables
        else:
            raise ValueError(
                f"Invalid type for {key_to_pop}. Expected str or list, got {type(images)}"
            )
