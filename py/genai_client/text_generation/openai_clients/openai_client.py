from typing import Any, Dict, TYPE_CHECKING, Union

if TYPE_CHECKING:

    def smss_stream(
        data: Any, stream_type: str = "content", interim: bool = True
    ) -> None: ...


import json
from openai import OpenAI, AzureOpenAI
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...constants import AskModelEngineResponse
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil
from ...message_builders.openai.openai_message_builder import OpenAIMessageBuilder
from smss_thread_local import get_smss_stream
from .openai_image_client import OpenAiImageClient
from .openai_audio_client import OpenAiAudioClient
from ...tokenizers.vllm_tokenizer import VLLMTokenizer
from ...tokenizers.tgi_tokenizer import TGITokenizer
from ...tokenizers.openai_tokenizer import OpenAiTokenizer
from ...tokenizers.huggingface_tokenizer import HuggingfaceTokenizer
from ...utils import StringEnum


class Models(StringEnum):
    OPENAI_5_1_TEXT_IMAGE = "gpt-5.1"


class OpenAiClient(AbstractTextGenerationClient):
    PARENT_PARAMS = {
        "template",
        "template_name",
        "model_name",
        "model_type",
        "context_window",
        "max_input_tokens",
        "max_tokens",
        "max_completion_tokens",
        "ai_role",
        "user_role",
        "system_role",
        "chat_type",
        "tokens_param_name",
    }

    def __init__(
        self,
        is_azure: bool,
        api_key: str,
        **kwargs,
    ):
        self.is_azure = is_azure
        self.endpoint = kwargs.pop("endpoint", None)
        if self.endpoint:
            kwargs["base_url"] = self.endpoint
        chat_type = kwargs.pop("chat_type", "chat-completion")
        kwargs["chat_type"] = chat_type
        self.deployment_type = kwargs.pop("deployment_type", "openai").lower()

        parent_kwargs = {k: v for k, v in kwargs.items() if k in self.PARENT_PARAMS}
        client_kwargs = {k: v for k, v in kwargs.items() if k not in self.PARENT_PARAMS}

        super().__init__(**parent_kwargs)

        self.chat_type = self.model_settings.chat_type
        self.tokenizer = self._get_tokenizer(kwargs)
        self.client = self._get_client(api_key, is_azure, **client_kwargs)

        self.message_builder = OpenAIMessageBuilder(self.model_settings, self.chat_type)
        self.image_client = OpenAiImageClient(client=self)
        self.audio_client = OpenAiAudioClient(client=self)

    def _get_tokenizer(
        self, init_args: Dict = {}
    ) -> Union[VLLMTokenizer, OpenAiTokenizer]:
        if not self.is_azure and self.endpoint and self.endpoint.strip():
            if self.deployment_type == "vllm":
                return VLLMTokenizer(
                    model_name=self.model_settings.model_name,
                    endpoint=self.endpoint,
                    api_key=init_args.get("api_key", "EMPTY"),
                )
            elif self.deployment_type == "tgi":
                return TGITokenizer(
                    endpoint=self.endpoint, api_key=init_args.get("api_key", "EMPTY")
                )
            else:
                return HuggingfaceTokenizer(
                    encoder_name=init_args.get("tokenizer_name", None)
                    or self.model_settings.model_name,
                    max_tokens=self.model_settings.max_completion_tokens,
                    max_input_tokens=self.model_settings.max_input_tokens,
                    context_window=self.model_settings.context_window,
                    max_completion_tokens=self.model_settings.max_completion_tokens,
                )
        return OpenAiTokenizer(
            encoder_name=init_args.get("tokenizer_name", None)
            or self.model_settings.model_name,
            max_tokens=self.model_settings.max_completion_tokens,
            max_input_tokens=self.model_settings.max_input_tokens,
            context_window=self.model_settings.context_window,
            max_completion_tokens=self.model_settings.max_completion_tokens,
        )

    def _get_client(
        self, api_key: str, is_azure: bool, **kwargs
    ) -> Union[OpenAI, AzureOpenAI]:
        if is_azure:
            endpoint = kwargs.pop("endpoint", None)
            kwargs["azure_endpoint"] = endpoint
            return AzureOpenAI(api_key=api_key, **kwargs)
        return OpenAI(api_key=api_key, **kwargs)

    def ask_call(self, prefix: str = "", **kwargs) -> AskModelEngineResponse:
        semoss_messages = self.build_semoss_messages(
            model_settings=self.model_settings, **kwargs
        )

        if self.model_settings.model_type == "audio":
            last_message = semoss_messages[-1]
            text = last_message.content if hasattr(last_message, "content") else ""
            return self.audio_client.ask(text, **kwargs)

        streaming = kwargs.pop("stream", True)
        if self.chat_type == "chat-completion" and streaming:
            kwargs.update({"stream": True, "stream_options": {"include_usage": True}})

        elif self.chat_type == "responses" and streaming:
            kwargs.update({"stream": True})

        try:
            openai_messages = self.message_builder.build_request(semoss_messages)
        except Exception as e:
            raise ValueError(f"Error building OpenAI messages: {e}") from e

        if self.model_settings.model_type == "image":
            return self.image_client.ask(openai_messages, **kwargs)

        if self.chat_type == "chat-completion":
            return self.handle_chat_completion_response(openai_messages, prefix=prefix)
        elif self.chat_type == "responses":
            return self.handle_responses_response(openai_messages, prefix=prefix)
        elif self.chat_type == "completions":
            return self.handle_completions_response(openai_messages, prefix=prefix)
        else:
            raise ValueError("Invalid chat type")

    def handle_completions_response(
        self,
        request: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse:
        response = self.client.completions.create(
            model=self.model_settings.model_name, **request
        )
        if request.get("stream", True):
            final_query = ""
            for chunk in response:
                if "text" in chunk:
                    content = chunk.choices[0].text
                    if content != None:
                        final_query += content
                        print(prefix + content, end="")
            response_tokens = 0
            input_tokens = 0
        else:
            final_query = response.choices[0].text
            response_tokens = response.usage.completion_tokens
            input_tokens = response.usage.prompt_tokens

        model_engine_response = AskModelEngineResponse(
            response=final_query,
            response_tokens=response_tokens,
            prompt_tokens=input_tokens,
        )

        return model_engine_response

    def handle_responses_response(
        self,
        request: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse:
        smss_stream = get_smss_stream()

        if self.model_settings.model_name in Models.values():
            request["tools"] = [
                {
                    "type": "image_generation",
                }
            ]

        response = self.client.responses.create(
            model=self.model_settings.model_name, **request
        )

        if request.get("stream", False):
            response_tokens = 0
            input_tokens = 0

            streamed_tools = {}
            finish_reason = None
            aggregated_content = ""
            image_content = ""
            text_image_response = ""

            for chunk in response:
                # Usage info typically comes in the final chunk
                if "response.completed" in chunk.type:
                    response_tokens = chunk.response.usage.output_tokens
                    input_tokens = chunk.response.usage.input_tokens
                    finish_reason = chunk.response.status

                # streaming text and schema
                if "response.output_text.delta" in chunk.type:
                    content = chunk.delta
                    if content is not None:
                        aggregated_content += content
                        data = StreamUtil.create_content_chunk(content)
                        smss_stream(data, stream_type="content")
                        print(prefix + content, end="")

                if "response.image_generation_call.partial_image" in chunk.type:
                    image_content = chunk.partial_image_b64

                # streaming tool calls
                if "response.function_call_arguments.done" in chunk.type:
                    idx = chunk.output_index
                    if idx not in streamed_tools:
                        streamed_tools[idx] = {
                            "id": None,
                            "type": None,
                            "name": None,
                            "arguments": "",
                        }

                    if hasattr(chunk, "item_id") and chunk.item_id is not None:
                        streamed_tools[idx]["id"] = chunk.item_id
                        data = StreamUtil.create_tool_id_chunk(idx, chunk.item_id)
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                    if hasattr(chunk, "type") and chunk.type is not None:
                        streamed_tools[idx]["type"] = chunk.type
                        data = StreamUtil.create_tool_type_chunk(idx, chunk.type)
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                    # since name key is mandatory, so added item_id as name for now
                    if hasattr(chunk, "item_id") and chunk.item_id is not None:
                        streamed_tools[idx]["name"] = chunk.item_id
                        data = StreamUtil.create_function_name_chunk(idx, chunk.item_id)
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

                    if hasattr(chunk, "arguments") and chunk.arguments is not None:
                        streamed_tools[idx]["arguments"] += chunk.arguments
                        data = StreamUtil.create_function_arguments_chunk(
                            idx, chunk.arguments
                        )
                        smss_stream(data, stream_type="tool")
                        print(prefix + str(data), end="")

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
                    try:
                        arguments = json.loads(tool_call["arguments"])
                    except json.decoder.JSONDecodeError:
                        arguments = tool_call["arguments"]

                    tool_result.append(
                        {
                            "id": tool_call["id"],
                            "type": tool_call["type"],
                            "name": tool_call["name"],
                            "arguments": arguments,
                        }
                    )

                return AskModelEngineResponse(
                    response=tool_result,
                    prompt_tokens=input_tokens,
                    response_tokens=response_tokens,
                    messageType="TOOL",
                )
            else:
                data = StreamUtil.create_finish_reason_chunk(finish_reason)
                smss_stream(data, stream_type="content", interim=False)

                # Mixed output: text + image
                if aggregated_content and image_content:
                    text_image_list = []
                    text_image_list.append({"type": "text", "data": aggregated_content})
                    text_image_list.append(
                        {
                            "type": "image",
                            "data": image_content,
                        }
                    )

                    text_image_response = json.dumps(
                        text_image_list
                    )  # converting list into string

                    return AskModelEngineResponse(
                        response=text_image_response,
                        prompt_tokens=input_tokens,
                        response_tokens=response_tokens,
                        messageType="CHAT",
                    )

                return AskModelEngineResponse(
                    response=aggregated_content,
                    response_tokens=response_tokens,
                    prompt_tokens=input_tokens,
                )
        else:
            response_tokens = response.usage.output_tokens
            input_tokens = response.usage.input_tokens

            final_content = response.output_text

            # non-stream tool calls
            for output in response.output:
                if output.type == "function_call":
                    return self._parse_tools_call_response(
                        response=response,
                        response_tokens=response_tokens,
                        prompt_tokens=input_tokens,
                    )

            if self.model_settings.model_name in Models.values():
                return self._handle_text_image_response(
                    response=response,
                    response_tokens=response_tokens,
                    input_tokens=input_tokens,
                )

            return AskModelEngineResponse(
                response=final_content,
                response_tokens=response_tokens,
                prompt_tokens=input_tokens,
            )

    def handle_chat_completion_response(
        self,
        request: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse:
        smss_stream = get_smss_stream()

        response = self.client.chat.completions.create(
            model=self.model_settings.model_name, **request
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
                    try:
                        arguments = json.loads(tool_call["function"]["arguments"])
                    except json.decoder.JSONDecodeError:
                        arguments = tool_call["function"]["arguments"]

                    tool_result.append(
                        {
                            "id": tool_call["id"],
                            "type": tool_call["type"],
                            "name": tool_call["function"]["name"],
                            "arguments": arguments,
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
        response,
        response_tokens: int,
        prompt_tokens: int,
    ) -> AskModelEngineResponse:
        tools_result = []

        if self.chat_type == "chat-completion":
            for i, tool_call in enumerate(response.choices[0].message.tool_calls):
                try:
                    arguments = json.loads(tool_call.function.arguments)
                except json.decoder.JSONDecodeError:
                    arguments = tool_call.function.arguments

                tools_result.append(
                    {
                        "id": tool_call.id,
                        "type": tool_call.type,
                        "name": tool_call.function.name,
                        "arguments": arguments,
                    }
                )

        elif self.chat_type == "responses":
            for i, tool_call in enumerate(response.output):
                if isinstance(tool_call.arguments, str):
                    try:
                        arguments = json.loads(tool_call.arguments)
                    except json.decoder.JSONDecodeError:
                        arguments = tool_call.arguments
                else:
                    # Already a dict/object
                    arguments = tool_call.arguments

                tools_result.append(
                    {
                        "id": tool_call.call_id,
                        "type": tool_call.type,
                        "name": tool_call.name,
                        "arguments": arguments,
                    }
                )

        return AskModelEngineResponse(
            response=tools_result,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            messageType="TOOL",
        )

    def _handle_text_image_response(self, response, input_tokens, response_tokens):
        """Handle the text and image in same response."""
        text_image_response = ""
        output_items = response.output or []

        # No multimodal → simple text
        if len(output_items) == 1 and output_items[0].type == "message":
            text_image_response = response.output_text
            return AskModelEngineResponse(
                response=text_image_response,
                response_tokens=response_tokens,
                prompt_tokens=input_tokens,
                messageType="CHAT",
            )

        # Mixed output: text + image
        text_image_list = []

        for output in output_items:
            # TEXT
            if output.type == "message":
                text_image_list.append({"type": "text", "data": output.content[0].text})

            # IMAGE
            elif output.type == "image_generation_call":
                text_image_list.append(
                    {
                        "type": "image",
                        "data": output.result,
                    }
                )

        text_image_response = json.dumps(text_image_list)  # converting list into string

        return AskModelEngineResponse(
            response=text_image_response,
            prompt_tokens=input_tokens,
            response_tokens=response_tokens,
            messageType="CHAT",
        )
