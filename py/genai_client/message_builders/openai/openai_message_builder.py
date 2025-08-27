from typing import List, Dict, Any, Tuple, Union
import json
from pydantic import BaseModel
from ...utils import get_image_extension
from .openai_models import (
    OpenAIRoles,
    OpenAIMessage,
    OpenAIImageURL,
    OpenAIImageContentPart,
    OpenAITextContentPart,
    OpenAIImageDetail,
    OpenAIResponsesImageContentPart,
    OpenAIToolChatCompletionContentPart,
    OpenAIToolResponsesContentPart,
)
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSImageContent,
    SEMOSSImageType,
    ModelSettings,
)


class OpenAIMessageBuilder:

    def __init__(self, model_settings: ModelSettings, chat_type: str):
        """Initialize the OpenAI message builder with a specific model name."""
        self.model_settings = model_settings
        self.chat_type = chat_type

    def build_request(self, semoss_messages: List[SEMOSSMessage]) -> Dict[str, Any]:
        """Build complete OpenAI request with messages and parameters. This is a dictionary that can be sent directly to OpenAI"""

        messages, request_map = self.build_messages(semoss_messages)

        message_dicts = []
        for message in messages:
            msg_dict: Dict[str, Any] = {"role": message.role}

            if isinstance(message.content, str):
                msg_dict.update({"content": message.content})
            else:
                content_list = []
                for part in message.content:
                    content_list.append(part.model_dump())
                msg_dict.update({"content": content_list})

            message_dicts.append(msg_dict)

        if self.chat_type == "chat-completion":
            request_map.update({"messages": message_dicts})
        elif self.chat_type == "responses":
            request_map.update({"input": message_dicts})
        elif self.chat_type == "completions":
            raise ValueError("Completions are not supported yet")

        return request_map

    def build_messages(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Tuple[List[OpenAIMessage], Dict[str, Any]]:
        """Convert SEMOSS messages to OpenAI messages, verifying the messages and return the param map from the latest message"""
        openai_messages = []
        param_map = {}

        for i, message in enumerate(semoss_messages):
            is_last = i == len(semoss_messages) - 1
            role = self._message_type_to_role(message.type)

            content_parts = []

            # Handle text content
            if message.content:
                content_parts.append(self._build_text_content_part(message.content))

            # Handle image content
            if message.image_content:
                image_content_parts = self._build_image_content_parts(
                    message.image_content
                )
                content_parts.extend(image_content_parts)

            if len(content_parts) == 1 and isinstance(
                content_parts[0], OpenAITextContentPart
            ):
                content = content_parts[0].text
            else:
                content = content_parts

            openai_messages.append(
                OpenAIMessage(
                    role=role,
                    content=content,
                )
            )

            if is_last:
                param_map.update(message.param_map)
                if self.chat_type == "responses":
                    # Process structured json input
                    has_schema = param_map.get("schema", False)
                    if has_schema:
                        # converting string to boolean for "additionalProperties" key
                        param_map["schema"] = self.replace_string_false(
                            param_map["schema"]
                        )
                        param_map = self._get_structured_parameters_format(**param_map)

                    # convert tools into openai responses format if present
                    if param_map.get("tools"):
                        param_map["tools"] = self.convert_mcp_to_openai_responses_tools(
                            param_map["tools"]
                        )

                    openai_messages, param_map = self._clean_param_map_for_responses(
                        openai_messages, param_map
                    )
                elif self.chat_type == "chat-completion":
                    # Process structured json input
                    has_schema = param_map.get("schema", False)
                    if has_schema:
                        param_map = self._get_structured_parameters_format(**param_map)

                    # convert tools into openai chat-completion format if present
                    if param_map.get("tools"):
                        param_map["tools"] = (
                            self.convert_mcp_to_openai_chat_completions_tools(
                                param_map["tools"]
                            )
                        )

                    openai_messages, param_map = (
                        self._clean_param_map_for_chat_completions(
                            openai_messages, param_map
                        )
                    )
                elif self.chat_type == "completions":
                    raise ValueError("Completions are not supported yet")
                else:
                    raise ValueError(f"Invalid chat type: {self.chat_type}")

        return openai_messages, param_map

    def replace_string_false(self, obj):
        """
        Recursively traverse a structure and replace string booleans with actual booleans.
        """
        if isinstance(obj, dict):
            return {k: self.replace_string_false(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.replace_string_false(v) for v in obj]
        if isinstance(obj, str):
            if obj.lower() == "false":
                return False
            if obj.lower() == "true":
                return True
        return obj

    def _get_structured_parameters_format(self, **param_map) -> Tuple[str, int, str]:
        """
        1. Validate the schema and identify the schema type
        2. Create the structured response format with the correct parameter name
        3. Make the structured output call to the correct endpoint based on model type
        4. Extract the structured output from the response
        """
        schema = param_map.pop("schema")
        # Validating the schema and identifying the type
        schema_type, schema = self._validate_structured_input(schema)
        # Creating the structured response format with the correct parameter name
        structured_param_name, structured_param_value = self._create_structured_format(
            schema_type, schema
        )
        # Making new params so I can use dynamic keys
        params = {structured_param_name: structured_param_value, **param_map}

        return params

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
                json.dumps(schema, ensure_ascii=False)
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

    def _create_structured_format(self, schema_type, schema) -> Tuple[str, Any]:
        """
        Create the structure request format for structured output.
        Returns a tuple with the parameter name as string and the parameter value.
        These cases are different based on whether we are hitting OpenAI versus vLLM
        and whether the schema is a dict or Pydantic model.
        """
        if self.chat_type == "chat-completion":
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
        elif self.chat_type == "responses":
            if schema_type == "dict":
                # Ensure the schema has additionalProperties set to False for responses API
                processed_schema = self._ensure_additional_properties_false(schema)
                return (
                    "text",
                    {
                        "format": {
                            "type": "json_schema",
                            "name": "schema_name",
                            "schema": processed_schema,
                            "strict": True,
                        }
                    },
                )
            else:
                return ("text", schema)  # Pydantic model

    def _ensure_additional_properties_false(self, schema: dict) -> dict:
        """
        Recursively ensure that all objects in the schema have additionalProperties set to False.
        This is required for OpenAI's responses API strict mode.
        """
        if not isinstance(schema, dict):
            return schema

        # Make a deep copy to avoid modifying the original
        import copy

        processed_schema = copy.deepcopy(schema)

        def process_object(obj):
            if isinstance(obj, dict):
                # If this is a JSON schema object definition
                if obj.get("type") == "object" or "properties" in obj:
                    # Set additionalProperties to False if not already specified
                    if "additionalProperties" not in obj:
                        obj["additionalProperties"] = False

                # Recursively process all nested objects
                for key, value in obj.items():
                    if isinstance(value, dict):
                        process_object(value)
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                process_object(item)

        process_object(processed_schema)
        return processed_schema

    def convert_mcp_to_openai_chat_completions_tools(
        self, mcp_tools: List[Dict]
    ) -> List[Dict]:
        """
        Convert MCP-formatted tools to OpenAI function calling format.
        Args:
            mcp_tools: List of tools in MCP format
        Returns:
            List of OpenAI tools for Chat Completions
        """
        openai_tools = []

        for tool in mcp_tools:
            openai_tool = {
                "name": tool["name"],
                "description": tool["description"],
                "parameters": {
                    "type": tool["inputSchema"]["type"],
                    "properties": {},
                    "required": tool["inputSchema"].get("required", []),
                },
            }

            for prop_name, prop_def in tool["inputSchema"]["properties"].items():
                openai_tool["parameters"]["properties"][prop_name] = {
                    k: v for k, v in prop_def.items() if k != "title"
                }

            openai_tools.append(
                OpenAIToolChatCompletionContentPart(
                    type="function", function=openai_tool
                )
            )

        return openai_tools

    def convert_mcp_to_openai_responses_tools(
        self, mcp_tools: List[Dict]
    ) -> List[Dict]:
        """
        Convert MCP-formatted tools to OpenAI function calling format.
        Args:
            mcp_tools: List of tools in MCP format
        Returns:
            List of OpenAI tools for Responses
        """
        openai_tools = []

        for tool in mcp_tools:
            openai_tool_parameters = {
                "type": tool["inputSchema"]["type"],
                "properties": {},
                "required": tool["inputSchema"].get("required", []),
            }

            for prop_name, prop_def in tool["inputSchema"]["properties"].items():
                openai_tool_parameters["properties"][prop_name] = {
                    k: v for k, v in prop_def.items() if k != "title"
                }

            openai_tools.append(
                OpenAIToolResponsesContentPart(
                    type="function",
                    name=tool["name"],
                    description=tool["description"],
                    parameters=openai_tool_parameters,
                )
            )

        return openai_tools

    def _clean_param_map_for_responses(
        self, openai_messages: List[OpenAIMessage], param_map: Dict[str, Any]
    ) -> Dict[str, Any]:
        if param_map.get("context"):
            param_map["instructions"] = param_map.pop("context")

        max_tokens = (
            param_map.pop("max_tokens", None)
            or param_map.pop("max_new_tokens", None)
            or param_map.pop("max_completion_tokens", None)
        )
        if max_tokens:
            param_map["max_output_tokens"] = max_tokens

        # Removing any unhanlded semoss specific params
        param_map.pop("max_completion_tokens", None)
        param_map.pop("max_tokens", None)
        param_map.pop("max_new_tokens", None)
        param_map.pop("model_name", None)
        param_map.pop("history", None)
        param_map.pop("use_history", None)
        param_map.pop("context", None)
        param_map.pop("image_url", None)
        param_map.pop("image_encoded", None)
        return (openai_messages, param_map)

    def _clean_param_map_for_chat_completions(
        self, openai_messages: List[OpenAIMessage], param_map: Dict[str, Any]
    ) -> Tuple[List[OpenAIMessage], Dict[str, Any]]:
        """
        Cleaning the param map for the specific chat type and removing any unhandled semoss specific params
        """

        if param_map.get("context"):
            openai_messages = self._create_system_message(
                param_map.pop("context"), openai_messages
            )

        max_tokens = (
            param_map.pop("max_tokens", None)
            or param_map.pop("max_new_tokens", None)
            or param_map.pop("max_output_tokens", None)
        )
        if max_tokens:
            param_map["max_completion_tokens"] = max_tokens

        # Removing any unhanlded semoss specific params
        param_map.pop("max_output_tokens", None)
        param_map.pop("max_tokens", None)
        param_map.pop("max_new_tokens", None)
        param_map.pop("model_name", None)
        param_map.pop("history", None)
        param_map.pop("use_history", None)
        param_map.pop("context", None)
        param_map.pop("image_url", None)
        param_map.pop("image_encoded", None)
        return (openai_messages, param_map)

    def _create_system_message(
        self, context: str, openai_messages: List[OpenAIMessage]
    ) -> List[OpenAIMessage]:
        """Create or update the system message at the beginning of the message list."""
        # List is not empty and starts with a system message.
        if openai_messages and openai_messages[0].role == OpenAIRoles.SYSTEM.value:
            openai_messages[0].content = context
        # List does not start with a system message.
        else:
            openai_messages.insert(
                0, OpenAIMessage(role=OpenAIRoles.SYSTEM.value, content=context)
            )
        return openai_messages

    def _message_type_to_role(self, message_type: SEMOSSMessageType) -> str:
        """Convert SEMOSS message type to OpenAI role."""
        user_message_types = [
            SEMOSSMessageType.INPUT_TEXT,
            SEMOSSMessageType.INPUT_MEDIA,
            SEMOSSMessageType.INPUT_TOOL_EXEC,
        ]
        assistant_message_types = [
            SEMOSSMessageType.RESPONSE_TEXT,
            SEMOSSMessageType.RESPONSE_MEDIA,
            SEMOSSMessageType.RESPONSE_TOOL,
        ]
        if message_type in user_message_types:
            if self.model_settings.user_role:
                return self.model_settings.user_role
            else:
                # DEFAULT USER ROLE
                return OpenAIRoles.USER.value
        elif message_type in assistant_message_types:
            if self.model_settings.ai_role:
                return self.model_settings.ai_role
            else:
                # DEFAULT ASSISTANT ROLE
                return OpenAIRoles.ASSISTANT.value
        else:
            raise ValueError(f"Unknown message type: {message_type}")

    def _build_text_content_part(self, content: str) -> OpenAITextContentPart:
        """Build OpenAI text content part"""
        if self.chat_type == "responses":
            return OpenAITextContentPart(text=content, type="input_text")
        else:
            return OpenAITextContentPart(text=content)

    def _build_image_content_parts(
        self, image_content: List[SEMOSSImageContent] = []
    ) -> List[OpenAIImageContentPart]:
        """Build OpenAI image content parts from SEMOSS image content."""
        openai_image_parts = []

        for image in image_content:
            if image.type == SEMOSSImageType.URL:
                openai_image_parts.append(self._build_url_image_content(image))
            elif image.type == SEMOSSImageType.BASE64:
                openai_image_parts.append(self._build_base64_image_content(image))
            else:
                raise ValueError(f"Unknown image type: {image.type}")

        return openai_image_parts

    def _build_url_image_content(
        self, image_content: SEMOSSImageContent
    ) -> Union[OpenAIImageContentPart, OpenAIResponsesImageContentPart]:
        """Build OpenAI image content part from URL"""
        if not image_content.url:
            raise ValueError(
                "The image type was specified as URL but no URL was provided."
            )

        if self.chat_type == "responses":
            return OpenAIResponsesImageContentPart(image_url=image_content.url)
        else:
            image_url = OpenAIImageURL(
                url=image_content.url, detail=OpenAIImageDetail.AUTO.value
            )

            return OpenAIImageContentPart(image_url=image_url)

    def _build_base64_image_content(
        self, image_content: SEMOSSImageContent
    ) -> Union[OpenAIImageContentPart, OpenAIResponsesImageContentPart]:
        """Build OpenAI image content part from base64"""
        if not image_content.data:
            raise ValueError(
                "The image type was specified as base64 but no data was provided."
            )

        if not image_content.mime_type:
            image_content.mime_type = get_image_extension(image_content.data)

        if image_content.mime_type == "image/jpg":
            image_content.mime_type = "image/jpeg"

        data_uri = f"data:{image_content.mime_type};base64,{image_content.data}"

        if self.chat_type == "responses":
            return OpenAIResponsesImageContentPart(image_url=data_uri)
        else:
            image_url = OpenAIImageURL(
                url=data_uri, detail=OpenAIImageDetail.AUTO.value
            )
            return OpenAIImageContentPart(image_url=image_url)
