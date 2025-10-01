from typing import Any, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    # injected into globals in handle_python of gaas_tcp_server_handler.py
    def smss_stream(
        data: Any, stream_type: str = "content", interim: bool = True
    ) -> None: ...


import json

from ..abstract_openai_client import AbstractOpenAiClient
from ....constants import AskModelEngineResponse
from ....message_builders.openai.openai_message_builder import OpenAIMessageBuilder
from ....message_builders.semoss_base.semoss_streaming_util import StreamUtil
from smss_thread_local import get_smss_stream


class OpenAIClientV2(AbstractOpenAiClient):
    def __init__(self, client, chat_type: str):
        # I won't need to do this in the future
        self.cfg_client = client
        self.chat_type = chat_type
        self.message_builder = OpenAIMessageBuilder(
            self.cfg_client.model_settings, chat_type
        )

    def ask_call(self, prefix: str = "", **kwargs) -> AskModelEngineResponse:
        if self.cfg_client.model_type == "image":
            return self.cfg_client.image_client.ask(**kwargs)

        # until all the models are ported over
        # we are going to set openai to stream=True by default
        streaming = kwargs.pop("stream", True)
        if streaming:
            kwargs.update({"stream": True, "stream_options": {"include_usage": True}})

        self.ask_settings = self.get_ask_settings(
            self.cfg_client.model_settings, **kwargs
        )

        if self.ask_settings.semoss_messages is None:
            raise ValueError("semoss_messages is required")

        request_params = self.message_builder.build_request(
            self.ask_settings.semoss_messages
        )

        if self.chat_type == "chat-completion":
            return self.handle_chat_completion_response(request_params, prefix=prefix)
        elif self.chat_type == "responses":
            return self.handle_responses_response(request_params, prefix=prefix)
        elif self.chat_type == "completions":
            raise ValueError("Completions are not supported")
        else:
            raise ValueError("Invalid chat type")

    def handle_responses_response(
        self,
        request: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse:
        response = self.cfg_client.client.responses.create(
            model=self.cfg_client.model_settings.model_name, **request
        )
        if request.get("stream", False):
            final_query = ""
            for chunk in response:
                if "delta" in chunk.type:
                    content = chunk.delta
                    if content != None:
                        final_query += content
                        print(prefix + content, end="")
            response_tokens = 0
            input_tokens = 0
        else:
            final_query = response.output_text
            response_tokens = response.usage.output_tokens
            input_tokens = response.usage.input_tokens

        # Returning a diff type of AskModelEngineResponse if there are tool calls
        if not request.get("stream", False):
            for output in response.output:
                if output.type == "function_call":
                    return self._parse_tools_call_response(
                        response=response,
                        response_tokens=response_tokens,
                        prompt_tokens=input_tokens,
                    )

        model_engine_response = AskModelEngineResponse(
            response=final_query,
            response_tokens=response_tokens,
            prompt_tokens=input_tokens,
        )

        return model_engine_response

    def handle_chat_completion_response(
        self,
        request: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse:
        # Get the stream function for the current thread
        smss_stream = get_smss_stream()

        response = self.cfg_client.client.chat.completions.create(
            model=self.cfg_client.model_settings.model_name, **request
        )

        if request.get("stream", False):
            response_tokens = 0
            prompt_tokens = 0

            streamed_tools = {}
            finish_reason = None
            aggregated_content = ""
            for chunk in response:
                # Usage info typically comes in the final chunk
                if hasattr(chunk, "usage") and chunk.usage is not None:
                    response_tokens = chunk.usage.completion_tokens
                    prompt_tokens = chunk.usage.prompt_tokens

                if chunk.choices and (len(chunk.choices) > 0):
                    # streaming text
                    if chunk.choices[0].delta.content:
                        content = chunk.choices[0].delta.content
                        if content is not None:
                            aggregated_content += content
                            data = StreamUtil.create_content_chunk(content)
                            smss_stream(data, stream_type="content")
                            print(prefix + content, end="")

                    if chunk.choices[0].delta.tool_calls:
                        tool_calls = chunk.choices[0].delta.tool_calls
                        if tool_calls:
                            for tool_call in tool_calls:
                                idx = tool_call.index
                                if idx not in streamed_tools:
                                    streamed_tools[idx] = {
                                        "id": None,
                                        "type": None,
                                        "function": {"name": None, "arguments": ""},
                                    }

                                if (
                                    hasattr(tool_call, "id")
                                    and tool_call.id is not None
                                ):
                                    streamed_tools[idx]["id"] = tool_call.id
                                    data = StreamUtil.create_tool_id_chunk(
                                        idx, tool_call.id
                                    )
                                    smss_stream(data, stream_type="tool")
                                    print(prefix + str(data), end="")

                                if (
                                    hasattr(tool_call, "type")
                                    and tool_call.type is not None
                                ):
                                    streamed_tools[idx]["type"] = tool_call.type
                                    data = StreamUtil.create_tool_type_chunk(
                                        idx, tool_call.type
                                    )
                                    smss_stream(data, stream_type="tool")
                                    print(prefix + str(data), end="")

                                if hasattr(tool_call, "function"):
                                    fn = tool_call.function
                                    if hasattr(fn, "name") and fn.name is not None:
                                        streamed_tools[idx]["function"][
                                            "name"
                                        ] = fn.name
                                        data = StreamUtil.create_function_name_chunk(
                                            idx, fn.name
                                        )
                                        smss_stream(data, stream_type="tool")
                                        print(prefix + str(data), end="")

                                    if (
                                        hasattr(fn, "arguments")
                                        and fn.arguments is not None
                                    ):
                                        streamed_tools[idx]["function"][
                                            "arguments"
                                        ] += fn.arguments
                                        data = (
                                            StreamUtil.create_function_arguments_chunk(
                                                idx, fn.arguments
                                            )
                                        )
                                        smss_stream(data, stream_type="tool")
                                        print(prefix + str(data), end="")

                    # Check if this chunk has a finish_reason
                    if chunk.choices[0].finish_reason:
                        finish_reason = chunk.choices[0].finish_reason

            if streamed_tools:
                data = StreamUtil.create_finish_reason_chunk(finish_reason)
                smss_stream(data, stream_type="tool", interim=False)
                final_tool_calls = [
                    streamed_tools[idx] for idx in sorted(streamed_tools.keys())
                ]
                # we flatten out the tool calls
                tool_result = []
                for tool_call in final_tool_calls:
                    # tool_call is a normal dict, need to use [] to pull keys
                    tool_result.append(
                        {
                            "id": tool_call["id"],
                            "type": tool_call["type"],
                            "name": tool_call["function"]["name"],
                            "arguments": tool_call["function"]["arguments"],
                        }
                    )

                return AskModelEngineResponse(
                    response=tool_result,
                    prompt_tokens=prompt_tokens,
                    response_tokens=response_tokens,
                    messageType="TOOL",
                )
            else:
                data = StreamUtil.create_finish_reason_chunk(finish_reason)
                smss_stream(data, stream_type="content", interim=False)

                return AskModelEngineResponse(
                    response=aggregated_content,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                )
        else:
            response_tokens = response.usage.completion_tokens
            prompt_tokens = response.usage.prompt_tokens

            final_content = response.choices[0].message.content
            tool_calls = response.choices[0].message.tool_calls
            if tool_calls:
                return self._parse_tools_call_response(
                    response=response,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                )
            else:
                return AskModelEngineResponse(
                    response=final_content,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                )

    def _parse_tools_call_response(
        self,
        response: AskModelEngineResponse,
        response_tokens: int,
        prompt_tokens: int,
    ) -> AskModelEngineResponse:
        tools_result = []

        if self.chat_type == "chat-completion":  # chat-completion
            for i, tool_call in enumerate(response.choices[0].message.tool_calls):
                tools_result.append(
                    {
                        "id": tool_call.id,
                        "type": tool_call.type,
                        "name": tool_call.function.name,
                        "arguments": json.loads(tool_call.function.arguments),
                    }
                )

        elif self.chat_type == "responses":  # responses
            for i, tool_call in enumerate(response.output):
                tools_result.append(
                    {
                        "id": tool_call.id,
                        "type": tool_call.type,
                        "name": tool_call.name,
                        "arguments": json.loads(tool_call.arguments),
                    }
                )

        return AskModelEngineResponse(
            response=tools_result,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            messageType="TOOL",
        )
