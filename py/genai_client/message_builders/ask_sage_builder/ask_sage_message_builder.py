from typing import List, Optional, Union, Dict, Any
from pydantic import BaseModel
from ..semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessageType,
    ModelSettings,
)


class AskSageMessage(BaseModel):
    user: str
    message: str


class AskSageRequest(BaseModel):
    message: Union[str, List[Dict[str, str]]]
    model: Optional[str] = None
    persona: Optional[int] = None
    tools: Optional[List[Dict[str, Any]]] = None
    tool_choice: Optional[Union[Dict[str, Any], str, None]] = None
    reasoning_effort: Optional[str] = None
    system_prompt: Optional[str] = None
    dataset: Optional[str] = None
    limit_references: Optional[int] = None
    temperature: Optional[float] = None
    live: Optional[int] = None
    streaming: Optional[bool] = True


class AskSageMessageBuilder:
    def __init__(self, model_settings: ModelSettings):
        self.model_settings = model_settings

    def build_request(self, semoss_messages: List[SEMOSSMessage]) -> AskSageRequest:
        if not semoss_messages:
            raise ValueError("semoss_messages cannot be empty.")
        ask_sage_messages = []

        for msg in semoss_messages:
            message_role: str = self.map_message_role(msg.type)
            msg_content = msg.content if msg.content else ""
            ask_sage_messages.append(
                AskSageMessage(user=message_role, message=msg_content)
            )

        last_message = semoss_messages[-1]
        return self.convert_param_map(
            last_message.param_map, ask_sage_messages, last_message
        )

    def map_message_role(self, semoss_role: SEMOSSMessageType) -> str:
        """Convert SEMOSS message type to AskSage role."""
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

        if semoss_role in user_message_types:
            return "me"
        elif semoss_role in assistant_message_types:
            return "sage"
        else:
            raise ValueError(f"Unsupported SEMOSS message type: {semoss_role}")

    def _build_tool_choice(
        self, tool_choice: Dict[str, str]
    ) -> Union[Dict[str, Any], str, None]:
        """
        Build the tool choice as string and dictionary for OpenAI
        SEMOSS tool_type options [auto, required, forced, none]
        OpenAI type options [auto, required, forced, none]
        OpenAI types of any and tool are not available with extended thinking
        """
        tool_type = tool_choice.get("type", "auto").lower()
        tool_name = tool_choice.get("name", None)

        if tool_type == "auto":
            return "auto"
        elif tool_type == "required":
            return "required"
        elif tool_type == "forced" and tool_name:
            return {"type": "function", "function": {"name": tool_name}}
        elif tool_type == "none":
            return "none"
        else:
            return None

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
                # copy all properties except 'title'
                converted_prop = {k: v for k, v in prop_def.items() if k != "title"}

                # if type is array, change to object and remove items
                if prop_def.get("type") == "array":
                    converted_prop["type"] = "object"
                    converted_prop.pop("items", None)

                openai_tool["parameters"]["properties"][prop_name] = converted_prop

            openai_tools.append({"type": "function", "function": openai_tool})

        return openai_tools

    def convert_param_map(
        self,
        param_map: Dict[str, Any],
        messages: List[AskSageMessage],
        last_semoss_msg: SEMOSSMessage,
    ) -> AskSageRequest:
        """Convert SEMOSS param_map to AskSage request parameters."""
        messages_dicts: List[Dict[str, Any]] = [
            msg.model_dump(exclude_none=True) for msg in messages
        ]
        tools: List[Dict[str, Any]] = []
        tools_param = param_map.get("tools", None)
        if tools_param:
            tools = self.convert_mcp_to_openai_chat_completions_tools(tools_param)

        tool_choice: Union[Dict[str, Any], str, None] = None

        tool_choice_param = (
            param_map.get("tool_choice", None) if len(tools) > 0 else None
        )
        if tool_choice_param:
            tool_choice = self._build_tool_choice(tool_choice_param)

        return AskSageRequest(
            message=messages_dicts,
            model=self.model_settings.model_name,
            persona=param_map.get("persona", None),
            tools=tools,
            tool_choice=tool_choice,
            reasoning_effort=param_map.get(
                "reasoning_effort", None
            ),  # Do we have this param name standardized?
            system_prompt=param_map.get("instructions", None),
            dataset=param_map.get("dataset", None),
            limit_references=param_map.get("limit_references", None),
            temperature=param_map.get("temperature", None),
            live=param_map.get("live", None),
            streaming=param_map.get("streaming", True),
        )
