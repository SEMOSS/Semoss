from typing import List, Dict, Any, Optional, Tuple, Union
import json
from pydantic import BaseModel
from ...utils import get_image_extension, string_to_bool
from ..semoss_base.builtin_tools import (
    built_in_tool_request_fields,
    normalize_built_in_tools,
)
from ..semoss_base.reasoning import normalize_reasoning
from .openai_models import (
    OpenAIResponsesToolCall,
    OpenAIRoles,
    OpenAIMessage,
    OpenAIToolFunctionPart,
    OpenAIToolCall,
    OpenAIImageURL,
    OpenAIImageContentPart,
    OpenAIFile,
    OpenAIFileContentPart,
    OpenAITextContentPart,
    OpenAIImageDetail,
    OpenAIResponsesImageContentPart,
    OpenAIResponsesFileContentPart,
    OpenAIToolChatCompletionContentPart,
    OpenAIToolResponsesContentPart,
    OpenAIResponsesToolCallOutput,
    OpenAIResponsesMessage,
)
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    SEMOSSMessagePartType,
    SEMOSSMediaContent,
    SEMOSSMediaInputType,
    ModelSettings,
)


class OpenAIMessageBuilder:

    def __init__(
        self,
        model_settings: ModelSettings,
        chat_type: str,
        simplify_messages: bool = False,
    ):
        """Initialize the OpenAI message builder with a specific model name."""
        self.model_settings = model_settings
        self.chat_type = chat_type
        self.simplify_messages = simplify_messages

    def build_request(self, semoss_messages: List[SEMOSSMessage]) -> Dict[str, Any]:
        """Build complete OpenAI request with messages and parameters. This is a dictionary that can be sent directly to OpenAI"""
        if self.chat_type == "responses":
            request = self.build_responses_request(semoss_messages)
        elif self.chat_type == "chat-completion":
            request = self.build_chat_completions_request(semoss_messages)
        elif self.chat_type == "completions":
            request = self.build_completions_messages(semoss_messages)
        else:
            raise ValueError(f"Unsupported chat type: {self.chat_type}")

        if (
            hasattr(self.model_settings, "global_param_override")
            and self.model_settings.global_param_override
        ):
            request.update(self.model_settings.global_param_override)

        if "built_in_tools" in request:
            openai_built_in = []
            for selection in normalize_built_in_tools(request.pop("built_in_tools")):
                spec = built_in_tool_request_fields(selection)
                spec.setdefault("type", selection["alias"])
                openai_built_in.append(spec)
            if openai_built_in:
                existing_tools = request.get("tools", [])
                existing_tools.extend(openai_built_in)
                request["tools"] = existing_tools

        return request

    def build_responses_request(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Dict[str, Any]:
        messages, request_map = self.build_responses_messages(semoss_messages)
        messages = [message.model_dump(exclude_none=True) for message in messages]
        request_map.update({"input": messages})
        return request_map

    def build_chat_completions_request(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Dict[str, Any]:
        messages, request_map = self.build_chat_completions_messages(semoss_messages)
        messages = [message.model_dump(exclude_none=True) for message in messages]
        request_map.update({"messages": messages})
        return request_map

    def build_completions_messages(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Dict[str, Any]:
        last_message = semoss_messages[-1]
        param_map = last_message.param_map if last_message.param_map else {}

        if last_message.type != SEMOSSMessageType.INPUT_TEXT:
            raise ValueError(
                "For completions, the last message must be of type INPUT_TEXT."
            )

        prompt = last_message.content
        param_map.update({"prompt": prompt})
        param_map.pop("tools", None)
        return param_map

    def build_responses_messages(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Tuple[List[Any], Dict[str, Any]]:
        """Convert SEMOSS messages to OpenAI Responses messages, verifying the messages and return the param map from the latest message"""
        openai_messages = []
        param_map = {}

        for i, message in enumerate(semoss_messages):
            is_last = i == len(semoss_messages) - 1

            if message.parts:
                content_parts = []
                is_assistant = message.io != "INPUT"
                assistant_media_parts = []
                for p in message.parts:
                    if p.type == SEMOSSMessagePartType.TEXT:
                        content_parts.append(
                            self._build_text_content_part(
                                p.text,
                                type=(
                                    "input_text"
                                    if message.io == "INPUT"
                                    else "output_text"
                                ),
                            )
                        )

                    elif p.type == SEMOSSMessagePartType.MEDIA:
                        if is_assistant:
                            # OpenAI does not allow image blocks in assistant turns;
                            # add a text placeholder and queue the image for a synthetic user message.
                            file_name = (
                                getattr(p.media_info, "file_name", None) or "image"
                            )
                            content_parts.append(
                                self._build_text_content_part(
                                    f"[Generated image: {file_name}]",
                                    type="output_text",
                                )
                            )
                            assistant_media_parts.append(
                                self._build_media_content_single_part(p.media_info)
                            )
                        else:
                            media_content = self._build_media_content_single_part(
                                p.media_info
                            )
                            content_parts.append(media_content)

                    elif p.type == SEMOSSMessagePartType.TOOL_CALL:
                        # other provider messages might have text with tool calls
                        # we need to append them separately to be able to convert to the correct openai format
                        if content_parts:
                            openai_messages.append(
                                OpenAIResponsesMessage(
                                    role=(
                                        OpenAIRoles.USER.value
                                        if message.io == "INPUT"
                                        else OpenAIRoles.ASSISTANT.value
                                    ),
                                    content=content_parts,
                                )
                            )
                            content_parts = []

                        openai_messages.append(
                            OpenAIResponsesToolCall(
                                call_id=p.tool_call.id,
                                name=p.tool_call.function.name,
                                arguments=p.tool_call.function.parameters or {},
                            )
                        )

                    elif p.type == SEMOSSMessagePartType.TOOL_RESULT:
                        # other provider messages might have text with tool calls
                        # we need to append them separately to be able to convert to the correct openai format
                        if content_parts:
                            openai_messages.append(
                                OpenAIResponsesMessage(
                                    role=(
                                        OpenAIRoles.USER.value
                                        if message.io == "INPUT"
                                        else OpenAIRoles.ASSISTANT.value
                                    ),
                                    content=content_parts,
                                )
                            )
                            content_parts = []

                        output = p.tool_result.output or "Tool executed successfully."
                        blocks = self._parse_tool_result_blocks(output)
                        openai_messages.append(
                            OpenAIResponsesToolCallOutput(
                                type="function_call_output",
                                call_id=p.tool_result.id,
                                output=self._build_responses_tool_output(output, blocks),
                            )
                        )

                    elif p.type == SEMOSSMessagePartType.THINKING:
                        thinking_dict = {
                            "type": "thinking",
                            "thinking": p.thinking,
                        }
                        if self.thinking_signature:
                            thinking_dict["signature"] = self.thinking_signature
                        content_parts.append(thinking_dict)

                # this message might be a tool result with no other content
                # in that case we don't want to add an additional message with empty content
                if content_parts:
                    openai_messages.append(
                        OpenAIResponsesMessage(
                            role=(
                                OpenAIRoles.USER.value
                                if message.io == "INPUT"
                                else OpenAIRoles.ASSISTANT.value
                            ),
                            content=content_parts,
                        )
                    )

                # Inject a synthetic user message with the images so the model can reference them
                if assistant_media_parts:
                    synthetic_content = [
                        self._build_text_content_part(
                            "Here is the generated image:", type="input_text"
                        )
                    ] + assistant_media_parts
                    openai_messages.append(
                        OpenAIResponsesMessage(
                            role=OpenAIRoles.USER.value,
                            content=synthetic_content,
                        )
                    )

                # handle parameters update based on last message same as w/o parts
                if is_last:
                    param_map.update(message.param_map)

            else:
                role = self._message_type_to_role(message.type)

                if message.type == "RESPONSE_TOOL" and message.tool_calls:
                    for tool_call in message.tool_calls:
                        openai_messages.append(
                            OpenAIResponsesToolCall(
                                call_id=tool_call.get("id"),
                                name=tool_call["function"]["name"],
                                arguments=tool_call["function"].get("arguments", {}),
                            )
                        )
                    continue

                if message.type == "INPUT_TOOL_EXEC" and message.tool_call_id:
                    output = message.content or "Tool executed successfully."
                    blocks = self._parse_tool_result_blocks(output)
                    openai_messages.append(
                        OpenAIResponsesToolCallOutput(
                            type="function_call_output",
                            call_id=message.tool_call_id,
                            output=self._build_responses_tool_output(output, blocks),
                        )
                    )
                    if is_last:
                        param_map.update(message.param_map)
                    continue

                # Handle regular messages (text and media content)
                content_parts = []

                # Handle text content
                if hasattr(message, "content") and message.content:
                    content_parts.append(self._build_text_content_part(message.content))

                # Handle media content
                if hasattr(message, "media_content") and message.media_content:
                    media_content_parts = self._build_media_content_parts(
                        message.media_content
                    )
                    content_parts.extend(media_content_parts)

                if len(content_parts) == 1 and isinstance(
                    content_parts[0], OpenAITextContentPart
                ):
                    content = content_parts[0].text
                else:
                    content = content_parts

                openai_messages.append(
                    OpenAIResponsesMessage(
                        role=role,
                        content=content,
                    )
                )

                if is_last:
                    param_map.update(message.param_map)

        has_schema = param_map.get("schema", False)

        try:
            reasoning = self._resolve_extended_reasoning(param_map)
            if reasoning:
                param_map["reasoning"] = reasoning
                param_map.pop("temperature", None)
        except Exception:
            pass

        if has_schema:
            # converting string to boolean for "additionalProperties" key
            param_map["schema"] = self.replace_string_false(param_map["schema"])
            param_map = self._get_structured_parameters_format(**param_map)

        # convert tools into openai responses format if present
        if param_map.get("tools"):
            tools = self._handle_responses_tools(param_map["tools"])
            param_map["tools"] = [
                tool.model_dump() if hasattr(tool, "model_dump") else tool
                for tool in tools
            ]
        else:
            param_map.pop("tools", None)

        # convert tool_choice into openai responses format if present
        if "tool_choice" in param_map and param_map.get("tools"):
            param_map["tool_choice"] = self._build_tool_choice(param_map["tool_choice"])

        openai_messages, param_map = self._clean_param_map_for_responses(
            openai_messages, param_map
        )

        return openai_messages, param_map

    def build_chat_completions_messages(
        self, semoss_messages: List[SEMOSSMessage]
    ) -> Tuple[List[OpenAIMessage], Dict[str, Any]]:
        """Convert SEMOSS messages to OpenAI messages, verifying the messages and return the param map from the latest message"""
        openai_messages = []
        param_map = {}

        for i, message in enumerate(semoss_messages):
            is_last = i == len(semoss_messages) - 1

            if message.parts:
                content_parts = []
                tool_call_parts = []
                is_assistant = message.io != "INPUT"
                assistant_media_parts = []
                for p in message.parts:
                    if p.type == SEMOSSMessagePartType.TEXT:
                        content_parts.append(self._build_text_content_part(p.text))

                    elif p.type == SEMOSSMessagePartType.MEDIA:
                        if is_assistant:
                            # OpenAI does not allow image blocks in assistant turns;
                            # add a text placeholder and queue the image for a synthetic user message.
                            file_name = (
                                getattr(p.media_info, "file_name", None) or "image"
                            )
                            content_parts.append(
                                self._build_text_content_part(
                                    f"[Generated image: {file_name}]"
                                )
                            )
                            assistant_media_parts.append(
                                self._build_media_content_single_part(p.media_info)
                            )
                        else:
                            media_content = self._build_media_content_single_part(
                                p.media_info
                            )
                            content_parts.append(media_content)

                    elif p.type == SEMOSSMessagePartType.TOOL_CALL:
                        # other provider messages might have text with tool calls
                        # we need to be able to convert to the correct openai format
                        # if content_parts:
                        #     openai_messages.append(
                        #         OpenAIResponsesMessage(
                        #             role=(
                        #                 OpenAIRoles.USER.value
                        #                 if message.io == "INPUT"
                        #                 else OpenAIRoles.ASSISTANT.value
                        #             ),
                        #             content=content_parts,
                        #         )
                        #     )
                        #     content_parts = []

                        tool_call_parts.append(
                            OpenAIToolCall(
                                id=p.tool_call.id,
                                type=p.tool_call.type,
                                function=OpenAIToolFunctionPart(
                                    name=p.tool_call.function.name,
                                    arguments=p.tool_call.function.parameters or {},
                                ),
                            )
                        )

                    elif p.type == SEMOSSMessagePartType.TOOL_RESULT:
                        # other provider messages might have text with tool calls
                        # we need to be able to convert to the correct openai format
                        if content_parts:
                            # openai_messages.append(
                            #     OpenAIResponsesMessage(
                            #         role=(
                            #             OpenAIRoles.USER.value
                            #             if message.io == "INPUT"
                            #             else OpenAIRoles.ASSISTANT.value
                            #         ),
                            #         content=content_parts,
                            #     )
                            # )
                            content_parts = []

                        openai_messages.append(
                            OpenAIMessage(
                                role="tool",
                                content=p.tool_result.output,
                                tool_call_id=p.tool_result.id,
                            )
                        )

                    elif p.type == SEMOSSMessagePartType.THINKING:
                        thinking_dict = {
                            "type": "thinking",
                            "thinking": p.thinking,
                        }
                        if self.thinking_signature:
                            thinking_dict["signature"] = self.thinking_signature
                        content_parts.append(thinking_dict)

                # openai does not allow text with tool calls
                # so if tool call we will drop the text portion
                if tool_call_parts:
                    openai_messages.append(
                        OpenAIMessage(
                            role="assistant",
                            content="",
                            tool_calls=tool_call_parts,
                        ),
                    )
                # this message might be a tool result with no other content
                # in that case we don't want to add an additional message with empty content
                elif content_parts:
                    if (
                        len(content_parts) == 1
                        and isinstance(content_parts[0], OpenAITextContentPart)
                        and self.simplify_messages
                    ):
                        content = content_parts[0].text
                    else:
                        content = content_parts

                    openai_messages.append(
                        OpenAIMessage(
                            role=(
                                OpenAIRoles.USER.value
                                if message.io == "INPUT"
                                else OpenAIRoles.ASSISTANT.value
                            ),
                            content=content,
                        )
                    )

                # Inject a synthetic user message with the images so the model can reference them
                if assistant_media_parts:
                    synthetic_content = [
                        self._build_text_content_part("Here is the generated image:")
                    ] + assistant_media_parts
                    openai_messages.append(
                        OpenAIMessage(
                            role=OpenAIRoles.USER.value,
                            content=synthetic_content,
                        )
                    )

                # handle parameters update based on last message same as w/o parts
                if is_last:
                    param_map.update(message.param_map)

            else:
                role = self._message_type_to_role(message.type)

                # Handle RESPONSE_TOOL messages (assistant messages with tool calls)
                if message.type == "RESPONSE_TOOL" and message.tool_calls:
                    tool_calls = []
                    for tool_call in message.tool_calls:
                        tool_calls.append(
                            OpenAIToolCall(
                                id=tool_call.get("id"),
                                type=tool_call.get("type", "function"),
                                function=OpenAIToolFunctionPart(
                                    name=tool_call["function"]["name"],
                                    arguments=tool_call["function"].get(
                                        "arguments", {}
                                    ),
                                ),
                            )
                        )

                    openai_messages.append(
                        OpenAIMessage(
                            role="assistant",
                            content="",
                            tool_calls=tool_calls,
                        )
                    )
                    continue

                # Handle INPUT_TOOL_EXEC messages (tool execution results)
                if message.type == "INPUT_TOOL_EXEC" and message.tool_call_id:
                    openai_messages.append(
                        OpenAIMessage(
                            role="tool",
                            content=message.content,
                            tool_call_id=message.tool_call_id,
                        )
                    )
                    if is_last:
                        param_map.update(message.param_map)
                    continue

                # Handle regular messages (text and media content)
                content_parts = []

                # Handle text content
                if message.content:
                    content_parts.append(self._build_text_content_part(message.content))

                # Handle media content
                if message.media_content:
                    media_content_parts = self._build_media_content_parts(
                        message.media_content
                    )
                    content_parts.extend(media_content_parts)

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

        try:
            reasoning_effort = self._resolve_reasoning_effort(param_map)
            if reasoning_effort:
                param_map["reasoning_effort"] = reasoning_effort
                param_map.pop("temperature", None)
        except Exception:
            pass

        has_schema = param_map.get("schema", False)
        if has_schema:
            # # converting string to boolean for "additionalProperties" key
            param_map["schema"] = self.replace_string_false(param_map["schema"])
            param_map = self._get_structured_parameters_format(**param_map)

        # convert tools into openai chat-completion format if present
        if not has_schema and param_map.get("tools"):
            tools = self.convert_mcp_to_openai_chat_completions_tools(
                param_map["tools"]
            )
            param_map["tools"] = [
                tool.model_dump() if hasattr(tool, "model_dump") else tool
                for tool in tools
            ]
        else:
            param_map.pop("tools", None)

        # convert tool_choice into openai chat-completion format if present
        if "tool_choice" in param_map and param_map.get("tools"):
            param_map["tool_choice"] = self._build_tool_choice(param_map["tool_choice"])

        openai_messages, param_map = self._clean_param_map_for_chat_completions(
            openai_messages, param_map
        )

        return openai_messages, param_map

    def _build_tool_choice(
        self, tool_choice: Dict[str, str] | str
    ) -> Dict[str, Any] | str | None:
        """
        Build the tool choice as string and dictionary for OpenAI
        SEMOSS tool_type options [auto, required, forced, none]
        OpenAI type options [auto, required, forced, none]
        OpenAI types of any and tool are not available with extended thinking
        """
        if isinstance(tool_choice, str):
            return tool_choice

        tool_type = tool_choice.get("type", "auto").lower()
        tool_name = tool_choice.get("name", None)

        if tool_type == "auto":
            return "auto"
        elif tool_type == "required":
            return "required"
        elif tool_type == "forced" and tool_name:
            if self.chat_type == "responses":
                return {"type": "function", "name": tool_name}
            elif self.chat_type == "chat-completion":
                return {"type": "function", "function": {"name": tool_name}}
        elif tool_type == "none":
            return "none"
        else:
            return None

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
            if schema_type == "dict":
                # Ensure the schema has additionalProperties set to False for chat completions API
                processed_schema = self._ensure_additional_properties_false(schema)
                return (
                    "response_format",
                    {
                        "type": "json_schema",
                        "json_schema": {
                            "name": "custom_schema",
                            "strict": True,
                            "schema": processed_schema,
                        },
                    },
                )
            else:
                return ("response_format", schema)  # Pydantic model

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
    ) -> List[Any]:
        """
        Convert MCP-formatted tools to OpenAI function calling format.
        Args:
            mcp_tools: List of tools in MCP format
        Returns:
            List of OpenAI tools for Chat Completions
        """
        openai_tools = []

        for tool in mcp_tools:
            tool_type = tool.get("type", "function")

            # built-in tools
            if (
                tool_type != "function"
                and "inputSchema" not in tool
                and "parameters" not in tool
            ):
                openai_tools.append(tool)
                continue

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
                # copy all properties except 'title'
                converted_prop = {k: v for k, v in prop_def.items() if k != "title"}

                # if type is array, change to object and remove items
                if prop_def.get("type") == "array":
                    converted_prop["type"] = "object"
                    converted_prop.pop("items", None)

                openai_tool["parameters"]["properties"][prop_name] = converted_prop

            openai_tools.append(
                OpenAIToolChatCompletionContentPart(
                    type="function", function=openai_tool
                )
            )

        return openai_tools

    def _handle_responses_tools(self, tools: List[Dict]) -> List[Any]:
        """
        I'm returning a mix of pydantic models and raw dictionaries because of OpenAI's built in tools.
        I want to be able to explictly define non-built-in tools but I'm not going to try to update or keep track
        of OpenAI's built-in tool's parameters.
        """
        openai_tools = []
        for tool in tools:
            tool_type = tool.get("type", "function")

            # Built-in tools (web_search, code_interpreter, etc.)
            if (
                tool_type != "function"
                and "inputSchema" not in tool
                and "parameters" not in tool
            ):
                openai_tools.append(tool)
                continue

            if "parameters" in tool:
                # Already in OpenAI format
                openai_tools.append(
                    OpenAIToolResponsesContentPart(
                        type=tool.get("type", "function"),
                        name=tool.get("name"),
                        description=tool.get("description"),
                        parameters=tool.get("parameters"),
                    )
                )
            else:
                # MCP format
                converted_tools = self._convert_mcp_to_openai_responses_tool(tool)
                openai_tools.append(converted_tools)

        return openai_tools

    def _convert_mcp_to_openai_responses_tool(
        self, mcp_tool: Dict[str, Any]
    ) -> OpenAIToolResponsesContentPart:
        """
        Convert MCP-formatted tools to OpenAI function calling format.
        Args:
            mcp_tool: A tool in MCP format
        Returns:
            An OpenAI tool for Responses
        """
        openai_tool_parameters = {
            "type": mcp_tool["inputSchema"]["type"],
            "properties": {},
            "required": mcp_tool["inputSchema"].get("required", []),
        }

        for prop_name, prop_def in mcp_tool["inputSchema"]["properties"].items():
            # copy all properties except 'title'
            converted_prop = {k: v for k, v in prop_def.items() if k != "title"}

            # if type is array, change to object and remove items
            if prop_def.get("type") == "array":
                converted_prop["type"] = "object"
                converted_prop.pop("items", None)

            openai_tool_parameters["properties"][prop_name] = converted_prop

        return OpenAIToolResponsesContentPart(
            type="function",
            name=mcp_tool["name"],
            description=mcp_tool["description"],
            parameters=openai_tool_parameters,
        )

    def _clean_param_map_for_responses(
        self, openai_messages: List[OpenAIMessage], param_map: Dict[str, Any]
    ) -> tuple[List[OpenAIMessage], Dict[str, Any]]:
        if param_map.get("system_prompt"):
            param_map["instructions"] = param_map.pop("system_prompt")

        max_tokens = (
            param_map.pop("max_tokens", None)
            or param_map.pop("max_new_tokens", None)
            or param_map.pop("max_completion_tokens", None)
            or param_map.pop("max_output_tokens", None)
            or self.model_settings.max_tokens
        )
        if max_tokens:
            param_map["max_output_tokens"] = max_tokens

        if "stream" not in param_map:
            param_map["stream"] = True
        else:
            streaming = param_map["stream"]
            streaming_bool = (
                string_to_bool(streaming)
                if isinstance(streaming, str)
                else bool(streaming)
            )
            param_map["stream"] = streaming_bool

        stream_options = param_map.get("stream_options")
        if isinstance(stream_options, dict):
            stream_options.pop("include_usage", None)
            if not stream_options:
                param_map.pop("stream_options", None)

        # Removing any unhandled semoss specific params
        param_map.pop("max_completion_tokens", None)
        param_map.pop("max_tokens", None)
        param_map.pop("max_new_tokens", None)
        param_map.pop("model_name", None)
        param_map.pop("history", None)
        param_map.pop("use_history", None)
        param_map.pop("image_url", None)
        param_map.pop("image_encoded", None)
        param_map.pop("chat_type", None)
        # Removing client-specific metadata not accepted by the Responses API
        param_map.pop("client_metadata", None)
        return (openai_messages, param_map)

    def _clean_param_map_for_chat_completions(
        self, openai_messages: List[OpenAIMessage], param_map: Dict[str, Any]
    ) -> Tuple[List[OpenAIMessage], Dict[str, Any]]:
        """
        Cleaning the param map for the specific chat type and removing any unhandled semoss specific params
        """

        # CODEX SPECIFIC HANDLING
        if param_map.get("instructions"):
            openai_messages = self._create_system_message(
                param_map.pop("instructions"), openai_messages
            )
        param_map.pop("include", None)
        # END CODEX SPECIFIC HANDLING

        if param_map.get("system_prompt"):
            openai_messages = self._create_system_message(
                param_map.pop("system_prompt"), openai_messages
            )

        max_tokens = (
            param_map.pop("max_tokens", None)
            or param_map.pop("max_new_tokens", None)
            or param_map.pop("max_output_tokens", None)
            or param_map.pop("max_completion_tokens", None)
            or self.model_settings.max_tokens
        )
        if max_tokens:
            param_map["max_completion_tokens"] = max_tokens

        if "stream" not in param_map:
            param_map["stream"] = True
            param_map["stream_options"] = {"include_usage": True}
        else:
            streaming = param_map["stream"]
            streaming_bool = (
                string_to_bool(streaming)
                if isinstance(streaming, str)
                else bool(streaming)
            )
            param_map["stream"] = streaming_bool
            if streaming_bool:
                param_map["stream_options"] = {"include_usage": True}
            else:
                param_map.pop("stream_options", None)

        # Removing any unhanlded semoss specific params
        param_map.pop("max_output_tokens", None)
        param_map.pop("max_tokens", None)
        param_map.pop("max_new_tokens", None)
        param_map.pop("model_name", None)
        param_map.pop("history", None)
        param_map.pop("use_history", None)
        param_map.pop("image_url", None)
        param_map.pop("image_encoded", None)
        return (openai_messages, param_map)

    def _create_system_message(
        self, system_prompt: str, openai_messages: List[OpenAIMessage]
    ) -> List[OpenAIMessage]:
        """Create or update the system message at the beginning of the message list."""
        # List is not empty and starts with a system message.
        if openai_messages and openai_messages[0].role == OpenAIRoles.SYSTEM.value:
            openai_messages[0].content = system_prompt
        # List does not start with a system message.
        else:
            openai_messages.insert(
                0, OpenAIMessage(role=OpenAIRoles.SYSTEM.value, content=system_prompt)
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

    def _parse_tool_result_blocks(self, output: str):
        """Return blocks from a SEMOSSMultimodalToolResponse envelope, or None for plain text."""
        try:
            parsed = json.loads(output)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(parsed, dict):
            return None
        blocks = parsed.get("SEMOSSMultimodalToolResponse")
        if not isinstance(blocks, list) or not blocks:
            return None
        return blocks

    def _build_responses_tool_output(self, output: str, blocks):
        """Convert MCP blocks to Responses API tool content.
        Images become input_image data-URIs; PDFs become input_file blocks."""
        if blocks is None:
            return output or "Tool executed successfully."
        result = []
        for b in blocks:
            if b["type"] == "text":
                result.append({"type": "input_text", "text": b["text"]})
            elif b["type"] == "image":
                if "data" not in b:
                    continue  # unresolved file ref — Java should have inlined this
                mime = b.get("mimeType", "image/png")
                result.append({"type": "input_image",
                               "image_url": f"data:{mime};base64,{b['data']}"})
            elif b["type"] == "document":
                if "data" not in b:
                    continue
                mime = b.get("mimeType", "application/pdf")
                result.append({"type": "input_file",
                               "filename": "document.pdf",
                               "file_data": f"data:{mime};base64,{b['data']}"})
        return result if result else (output or "Tool executed successfully.")

    def _build_text_content_part(
        self, content: str, type: Optional[str] = "input_text"
    ) -> OpenAITextContentPart:
        """Build OpenAI text content part"""
        if self.chat_type == "responses":
            return OpenAITextContentPart(text=content, type=type)
        else:
            return OpenAITextContentPart(text=content)

    def _build_media_content_parts(
        self, media_content: List[SEMOSSMediaContent] = []
    ) -> List[
        Union[
            OpenAIImageContentPart,
            OpenAIFileContentPart,
            OpenAIResponsesImageContentPart,
            OpenAIResponsesFileContentPart,
        ]
    ]:
        """Build OpenAI media content parts from SEMOSS media content."""
        openai_media_parts = []
        for media in media_content:
            openai_media_parts.append(self._build_media_content_single_part(media))

        return openai_media_parts

    def _build_media_content_single_part(
        self, media: SEMOSSMediaContent = None
    ) -> Union[
        OpenAIImageContentPart,
        OpenAIFileContentPart,
        OpenAIResponsesImageContentPart,
        OpenAIResponsesFileContentPart,
    ]:
        """Build OpenAI media content part from SEMOSS media content."""
        if media.type == SEMOSSMediaInputType.URL:
            return self._build_url_image_content(media)
        elif media.type == SEMOSSMediaInputType.BASE64:
            return self._build_base64_media_content(media)
        else:
            raise ValueError(f"Unknown media type: {media.type}")

    def _build_url_image_content(
        self, media_content: SEMOSSMediaContent
    ) -> Union[OpenAIImageContentPart, OpenAIResponsesImageContentPart]:
        """Build OpenAI media content part from URL"""
        if not media_content.url:
            raise ValueError(
                "The media type was specified as URL but no URL was provided."
            )

        if self.chat_type == "responses":
            return OpenAIResponsesImageContentPart(image_url=media_content.url)
        else:
            image_url = OpenAIImageURL(
                url=media_content.url, detail=OpenAIImageDetail.AUTO.value
            )

            return OpenAIImageContentPart(image_url=image_url)

    def _build_base64_media_content(self, media_content: SEMOSSMediaContent) -> Union[
        OpenAIImageContentPart,
        OpenAIFileContentPart,
        OpenAIResponsesImageContentPart,
        OpenAIResponsesFileContentPart,
    ]:
        """Build OpenAI media content part from base64"""
        if not media_content.data:
            raise ValueError(
                "The media type was specified as base64 but no data was provided."
            )

        if not media_content.mime_type:
            media_content.mime_type = get_image_extension(media_content.data)

        if media_content.mime_type == "image/jpg":
            media_content.mime_type = "image/jpeg"

        data_uri = f"data:{media_content.mime_type};base64,{media_content.data}"

        if self.chat_type == "responses":
            if media_content.mime_type.startswith("image"):
                return OpenAIResponsesImageContentPart(image_url=data_uri)
            else:
                return OpenAIResponsesFileContentPart(
                    filename=media_content.file_name, file_data=data_uri
                )
        else:
            if media_content.mime_type.startswith("image"):
                image_url = OpenAIImageURL(
                    url=data_uri, detail=OpenAIImageDetail.AUTO.value
                )
                return OpenAIImageContentPart(image_url=image_url)
            else:
                file_data = OpenAIFile(
                    filename=media_content.file_name, file_data=data_uri
                )
                return OpenAIFileContentPart(file=file_data)

    # Canonical effort -> OpenAI effort. OpenAI has no "max"; clamp it to "high".
    # "none"/"minimal"/"xhigh" pass through (support varies by model — e.g. only
    # gpt-5.1+ accepts "none", only gpt-5.5 accepts "xhigh" — so only emit those
    # when the caller explicitly asked for them).
    _OPENAI_EFFORT = {
        "none": "none",
        "minimal": "minimal",
        "low": "low",
        "medium": "medium",
        "high": "high",
        "xhigh": "xhigh",
        "max": "high",
    }

    def _resolve_extended_reasoning(self, param_map: Dict[str, Any]) -> Dict[str, Any]:
        """Responses API: returns {"effort": ..., "summary": "auto"} or None."""
        resolved = normalize_reasoning(param_map, self.model_settings)
        if resolved is None:
            return None
        return {
            "effort": self._OPENAI_EFFORT.get(resolved.effort, "medium"),
            "summary": "auto",
        }

    def _resolve_reasoning_effort(self, param_map: Dict[str, Any]) -> str | None:
        """Chat Completions API: returns the flat reasoning_effort string or None."""
        resolved = normalize_reasoning(param_map, self.model_settings)
        if resolved is None:
            return None
        return self._OPENAI_EFFORT.get(resolved.effort, "medium")
