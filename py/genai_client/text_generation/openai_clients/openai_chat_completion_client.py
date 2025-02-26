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
                schema = json.loads(schema)
                return ("dict", schema)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON string provided for schema.")
        elif isinstance(schema, dict):
            # Validating that dict can be serialized to JSON
            try:
                json.dumps(schema)
                return ("dict", schema)
            except TypeError:
                raise ValueError("Schema dict contains non-serializable values.")
        elif isinstance(schema, BaseModel):
            # Checking if Pydantic model
            return ("pydantic", schema)
        elif isinstance(schema, type) and issubclass(schema, BaseModel):
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
            if schema_type == "dict":
                return (
                    "response_format",
                    {
                        "type": "json_schema",
                        "json_schema": {"name": "custom_schema", "schema": schema},
                    },
                )
            else:
                # Pydantic model
                return ("response_format", schema)
        else:
            # For vLLM it is the same for both dict and Pydantic model
            return ("extra_body", {"guided_json": schema})

    def _get_structured_output_response(self, params):
        """
        Make the structured output call to the correct endpoint based on model type.
        vLLM requires a different endpoint...
        """
        if self.model_type == "OPEN_AI":
            response = self.client.beta.chat.completions.parse(
                model=self.model_name, **params
            )
        else:
            response = self.client.chat.completions.create(
                model=self.model_name, **params
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
        structured_param_name, param_value = self._create_structured_response_format(
            schema_type, schema
        )
        # Making new params so I can use dynamic keys
        params = {structured_param_name: param_value, **kwargs}
        return self._get_structured_output_response(params)

    def inference_call(self, prefix: str, **kwargs) -> str:
        final_query = ""
        # For Remote Client Server Models
        if "base_url" in kwargs.keys():
            base_url = kwargs.pop("base_url")
            self.client.base_url = base_url
            self.client.api_key = "EMPTY"

        # Process structured output
        has_schema = kwargs.get("schema", False)
        if has_schema:
            return self._structured_output_call(**kwargs)

        kwargs["stream"] = kwargs.get("stream", True)

        if (
            self.model_name.startswith("o1-preview")
            or self.model_name.startswith("o1-mini")
            or self.model_name.startswith("o3-mini")
            or self.model_name == "o1"
        ):
            max_tokens = kwargs.pop("max_tokens")
            kwargs["max_completion_tokens"] = max_tokens
            # Check and remove "temperature" if it exists as its not supported
            if "temperature" in kwargs:
                del kwargs["temperature"]

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
        num_tokens_in_prompt = self.tokenizer.count_tokens(prompt_payload)

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
