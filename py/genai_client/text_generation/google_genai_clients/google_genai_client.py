import json, base64, uuid
from typing import List, Optional, Dict, Any
from types import SimpleNamespace
from pydantic import BaseModel
from google.genai import types
from google.genai import Client as GoogleGenAIClient
from ...clients.google_clients import (
    GoogleClient,
    GoogleClientConfig,
    GoogleClientType,
)
from ...constants import (
    AskModelEngineResponse2,
    TEMPLATE,
    TEMPLATE_NAME,
)
from ..abstract_text_generation_client import AbstractTextGenerationClient
from ...message_builders.google_genai.google_genai_builder import (
    GoogleGenAIMessageBuilder,
)
from ...retry_handler import RetryHandler
from smss_thread_local import get_smss_stream
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil
from ..model_engine_exception import ModelEngineException
from ...utils import string_to_bool
from .google_video_client import GoogleGenAiVideoClient


class UsageMetadata(BaseModel):
    candidates_token_count: int
    prompt_token_count: int


class StreamingResponse(BaseModel):
    text: str
    usage_metadata: Optional[UsageMetadata] = None

    class Config:
        arbitrary_types_allowed = True


class GoogleGenAiTextClient(AbstractTextGenerationClient):
    client: GoogleGenAIClient

    def __init__(
        self,
        service_account_credentials: Optional[Dict] = None,
        service_account_key_file: Optional[str] = None,
        region: Optional[str] = None,
        project: Optional[str] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        safety_settings: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__(
            template=kwargs.pop(TEMPLATE, None),
            template_name=kwargs.pop(TEMPLATE_NAME, None),
            **kwargs,
        )
        self.client_config = GoogleClientConfig(
            type=GoogleClientType.GOOGLE,
            service_account_credentials=service_account_credentials,
            service_account_key_file=service_account_key_file,
            region=region,
            project=project,
            api_key=api_key,
            base_url=base_url,
        )
        self.google_client = GoogleClient(config=self.client_config).genai_client
        self.video_client = GoogleGenAiVideoClient(parent_client=self)

        self.safety_settings = safety_settings

        retries = kwargs.get("retries", 0)
        self.retry_handler = RetryHandler(max_retries=retries)

    def ask_call(
        self,
        prefix="",
        **kwargs,
    ):
        if self.google_client is None:
            raise ValueError("Google Gen AI client is not initialized.")

        if (
            hasattr(self.model_settings, "global_param_override")
            and self.model_settings.global_param_override
        ):
            kwargs.update(self.model_settings.global_param_override)

        built_in_tools = kwargs.get("built_in_tools", []) or []
        web_search_enabled = any(
            isinstance(tool, str) and tool.lower() == "web_search"
            for tool in built_in_tools
        )
        inline_citations = kwargs.get("inline_citations", None)
        if inline_citations is None:
            inline_citations_enabled = True
        else:
            try:
                inline_citations_enabled = string_to_bool(inline_citations)
            except ValueError:
                inline_citations_enabled = True

        try:
            semoss_messages = self.build_semoss_messages(self.model_settings, **kwargs)

            if self.model_settings.model_type == "video":
                return self.video_client.ask_call(semoss_messages=semoss_messages)

            try:
                response = GoogleGenAIMessageBuilder().build_messages(
                    semoss_messages, self.model_settings
                )
                google_messages = response["messages"]
                provider_config = response["provider_config"]
                stream = response["stream"]
            except Exception as e:
                raise RuntimeError(
                    f"Failed to build messages from SEMOSS messages: {e}"
                )

            if stream:

                def streaming_call():
                    return self._handle_streaming(
                        prefix=prefix,
                        contents=google_messages,
                        config=provider_config,
                        web_search_enabled=web_search_enabled,
                        inline_citations_enabled=inline_citations_enabled,
                    )

                return self.generate_with_retry(streaming_call)

            def call_generate_content():
                return self.client.models.generate_content(
                    model=self.model_name,
                    contents=google_messages,
                    config=provider_config,
                )

            model_response = self.generate_with_retry(call_generate_content)

            text_response = model_response.text if model_response.text else ""
            if web_search_enabled and inline_citations_enabled:
                text_response = self._add_citations(model_response) or ""

            # Gemini reports candidates and thoughts as DISJOINT (unlike OpenAI/Anthropic).
            # Fold thoughts into response_tokens so output_tokens is total billed output.
            response_tokens = model_response.usage_metadata.candidates_token_count
            prompt_tokens = model_response.usage_metadata.prompt_token_count
            cache_read_tokens = getattr(
                model_response.usage_metadata, "cached_content_token_count", None
            )
            thinking_tokens = getattr(
                model_response.usage_metadata, "thoughts_token_count", None
            )
            if thinking_tokens:
                response_tokens = (response_tokens or 0) + thinking_tokens

            if len(getattr(model_response, "function_calls", None) or []) > 0:
                return self._parse_tools_call_response(
                    response=model_response,
                    response_tokens=response_tokens,
                    prompt_tokens=prompt_tokens,
                    cache_read_tokens=cache_read_tokens,
                    thinking_tokens=thinking_tokens,
                )

            thinking_text = ""
            image_data = []

            if (
                hasattr(model_response, "candidates")
                and len(model_response.candidates) > 0
            ):
                first = model_response.candidates[0]
                if getattr(first, "content", None) and getattr(
                    first.content, "parts", None
                ):
                    for part in first.content.parts:
                        if getattr(part, "text", False) and getattr(
                            part, "thought", False
                        ):
                            thinking_text += getattr(part, "text", "")
                        if part.inline_data:
                            image_data.append(
                                self._create_media_info(
                                    mime_type=part.inline_data.mime_type,
                                    image_bytes=part.inline_data.data,
                                )
                            )

            if thinking_text == "":
                thinking_text = None

            parts = []
            if thinking_text:
                parts.append({"type": "THINKING", "thinking": thinking_text})
            if text_response:
                parts.append({"type": "TEXT", "text": text_response})
            for media_info in image_data or []:
                parts.append({"type": "MEDIA", "media_info": media_info})

            return AskModelEngineResponse2(
                response=text_response,
                prompt_tokens=prompt_tokens,
                response_tokens=response_tokens,
                cache_read_tokens=cache_read_tokens,
                thinking_tokens=thinking_tokens,
                messageType="CHAT",
                schemaVersion=2,
                io="OUTPUT",
                parts=parts,
            )
        except Exception as e:
            return ModelEngineException(
                error=e, client="google", model=self.model_name
            ).parse_error()

    def generate_with_retry(self, generate_func, *args, **kwargs):
        """Helper to run a generation call with retry."""
        if callable(generate_func):
            wrapped = self.retry_handler.retry(generate_func)
            return wrapped(*args, **kwargs)
        return generate_func

    def _parse_tools_call_response(
        self,
        response: types.GenerateContentResponse,
        response_tokens: int,
        prompt_tokens: int,
        cache_read_tokens: Optional[int] = None,
        thinking_tokens: Optional[int] = None,
    ) -> AskModelEngineResponse2:
        tools_result = []

        parts_with_fc = []
        # preamble text the model emitted alongside the function_calls
        preamble_text = ""
        if (
            hasattr(response, "candidates")
            and response.candidates
            and hasattr(response.candidates[0], "content")
            and getattr(response.candidates[0].content, "parts", None)
        ):
            for p in response.candidates[0].content.parts:
                if getattr(p, "function_call", None) is not None:
                    parts_with_fc.append(p)
                elif (
                    getattr(p, "text", None)
                    and not getattr(p, "thought", False)
                ):
                    preamble_text += p.text

        for i, function_call in enumerate(response.function_calls):
            function_id = function_call.id or str(uuid.uuid4())
            tool_entry = {
                "id": function_id,
                "type": "function",
                "name": function_call.name,
                "arguments": getattr(function_call, "args", {}),
            }
            if i < len(parts_with_fc):
                ts = getattr(parts_with_fc[i], "thought_signature", None)
                if ts:
                    tool_entry["thought_signature"] = base64.b64encode(ts).decode(
                        "utf-8"
                    )
            tools_result.append(tool_entry)

        text_parts = (
            [{"type": "TEXT", "text": preamble_text}] if preamble_text else []
        )

        return AskModelEngineResponse2(
            response=tools_result,
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            cache_read_tokens=cache_read_tokens,
            thinking_tokens=thinking_tokens,
            messageType="TOOL",
            schemaVersion=2,
            io="OUTPUT",
            parts=text_parts
            + [{"type": "TOOL_CALL", "tool_call": t} for t in tools_result],
        )

    def _handle_streaming(
        self,
        contents: List[types.Content],
        config: types.GenerateContentConfig,
        prefix: Optional[str] = "",
        web_search_enabled: bool = False,
        inline_citations_enabled: bool = True,
    ) -> AskModelEngineResponse2:
        smss_stream = get_smss_stream()
        final_response = ""
        thinking_response = ""
        image_data = []
        input_tokens = 0
        output_tokens = 0

        content_array = []
        this_content_block: Dict[str, Any] = {}
        latest_grounding_metadata = None
        latest_usage_metadata = None
        tool_result = []

        stream = self.google_client.models.generate_content_stream(
            model=self.model_name, contents=contents, config=config
        )

        for event in stream:
            parts_with_fc = []
            if getattr(event, "usage_metadata", None):
                latest_usage_metadata = event.usage_metadata
            if hasattr(event, "candidates") and event.candidates:
                candidate = event.candidates[0]
                if getattr(candidate, "grounding_metadata", None):
                    latest_grounding_metadata = candidate.grounding_metadata
                if hasattr(candidate, "content") and getattr(
                    candidate.content, "parts", None
                ):
                    for part in candidate.content.parts:
                        if (
                            hasattr(part, "thought")
                            and part.thought
                            and hasattr(part, "text")
                            and part.text
                        ):
                            thinking_response += part.text
                            data = StreamUtil.create_thinking_chunk(part.text)
                            smss_stream(data, stream_type="thinking")
                            print(prefix + part.text, end="", flush=True)
                        if part.inline_data:
                            image_data.append(
                                self._create_media_info(
                                    mime_type=part.inline_data.mime_type,
                                    image_bytes=part.inline_data.data,
                                )
                            )
                        if getattr(part, "function_call", None) is not None:
                            parts_with_fc.append(part)

            if event.text:
                this_content_block["final_response"] = ""
                text_chunk = event.text
                this_content_block["final_response"] += text_chunk

                data = StreamUtil.create_content_chunk(text_chunk)
                smss_stream(data, stream_type="content")
                print(prefix + text_chunk, end="", flush=True)

                response_content = [
                    types.Content(
                        role="model",
                        parts=[
                            types.Part.from_text(
                                text=this_content_block["final_response"]
                            )
                        ],
                    )
                ]

                output_tokens += self._count_tokens(response_content)

                content_array.append(this_content_block)
                this_content_block = {}

            if len(getattr(event, "function_calls", None) or []) > 0:
                for i, function_call in enumerate(event.function_calls):
                    function_id = function_call.id or str(uuid.uuid4())
                    this_content_block.update(
                        {
                            "id": function_id,
                            "type": "function",
                            "function": {"name": None, "arguments": ""},
                        }
                    )
                    this_content_block["function"]["name"] = function_call.name

                    data = StreamUtil.create_tool_id_chunk(
                        index=len(tool_result), tool_id=function_id
                    )
                    smss_stream(data, stream_type="tool")
                    print(prefix + str(data), end="")

                    data = StreamUtil.create_tool_type_chunk(index=len(tool_result))
                    smss_stream(data, stream_type="tool")
                    print(prefix + str(data), end="")

                    data = StreamUtil.create_function_name_chunk(
                        index=len(tool_result),
                        function_name=function_call.name,
                    )
                    smss_stream(data, stream_type="tool")
                    print(prefix + str(data), end="")

                    this_content_block["function"]["arguments"] = function_call.args

                    data = StreamUtil.create_function_arguments_chunk(
                        index=len(tool_result),
                        arguments_chunk=function_call.args,
                    )
                    smss_stream(data, stream_type="tool")
                    print(prefix + str(data), end="")

                    if isinstance(this_content_block["function"]["arguments"], dict):
                        arguments = this_content_block["function"]["arguments"]
                    else:
                        try:
                            arguments = json.loads(
                                this_content_block["function"]["arguments"]
                            )
                        except Exception:
                            arguments = this_content_block["function"]["arguments"]

                    tool_entry = {
                        "id": this_content_block["id"],
                        "type": this_content_block["type"],
                        "name": this_content_block["function"]["name"],
                        "arguments": arguments,
                    }
                    if i < len(parts_with_fc):
                        ts = getattr(parts_with_fc[i], "thought_signature", None)
                        if ts:
                            ts_b64 = base64.b64encode(ts).decode("utf-8")
                            tool_entry["thought_signature"] = ts_b64
                            # Side-channel the signature through the SSE stream so
                            # the AnthropicEndpoint can persist it in the room's
                            # sidecar keyed by tool_use_id. The signature has no
                            # home in the Anthropic wire protocol, so without this
                            # extra chunk it would be dropped on the way to the
                            # Claude Code SDK.
                            sig_chunk = StreamUtil.create_thought_signature_chunk(
                                index=len(tool_result),
                                signature=ts_b64,
                            )
                            smss_stream(sig_chunk, stream_type="tool")
                    tool_result.append(tool_entry)

                    content_array.append(this_content_block)
                    this_content_block = {}

        cache_read_tokens = None
        thinking_tokens = None
        if latest_usage_metadata is not None:
            if getattr(latest_usage_metadata, "prompt_token_count", None) is not None:
                input_tokens = latest_usage_metadata.prompt_token_count
            if (
                getattr(latest_usage_metadata, "candidates_token_count", None)
                is not None
            ):
                output_tokens = latest_usage_metadata.candidates_token_count
            cache_read_tokens = getattr(
                latest_usage_metadata, "cached_content_token_count", None
            )
            thinking_tokens = getattr(
                latest_usage_metadata, "thoughts_token_count", None
            )
            # Gemini reports candidates and thoughts as DISJOINT; fold thoughts in
            # so output_tokens is total billed output (matches OpenAI/Anthropic).
            if thinking_tokens:
                output_tokens = (output_tokens or 0) + thinking_tokens
        else:
            input_tokens = self._count_tokens(contents)

        if tool_result:
            data = StreamUtil.create_finish_reason_chunk("tool_calls")
            smss_stream(data, stream_type="tool", interim=False)
        else:
            data = StreamUtil.create_finish_reason_chunk("stop")
            smss_stream(data, stream_type="content", interim=False)

        for content in content_array:
            if content.get("final_response", None):
                final_response += content.get("final_response")

        if tool_result:
            if getattr(config, "response_schema", None):
                is_schema, json_str = self._flatten_schema_tool(
                    tool_result, "return_json"
                )
                if is_schema:
                    parts = [{"type": "TEXT", "text": json_str}] if json_str else []
                    if thinking_response:
                        parts.append(
                            {"type": "THINKING", "thinking": thinking_response}
                        )
                    for media_info in image_data or []:
                        parts.append({"type": "MEDIA", "media_info": media_info})
                    return AskModelEngineResponse2(
                        response=json_str,
                        response_tokens=output_tokens,
                        prompt_tokens=input_tokens,
                        cache_read_tokens=cache_read_tokens,
                        thinking_tokens=thinking_tokens,
                        messageType="CHAT",
                        schemaVersion=2,
                        io="OUTPUT",
                        parts=parts,
                    )

            parts = []
            if thinking_response:
                parts.append({"type": "THINKING", "thinking": thinking_response})
            # preamble text the model emitted alongside the function_calls
            if final_response:
                parts.append({"type": "TEXT", "text": final_response})
            for media_info in image_data or []:
                parts.append({"type": "MEDIA", "media_info": media_info})
            parts.extend([{"type": "TOOL_CALL", "tool_call": t} for t in tool_result])

            return AskModelEngineResponse2(
                response=tool_result,
                response_tokens=output_tokens,
                prompt_tokens=input_tokens,
                cache_read_tokens=cache_read_tokens,
                thinking_tokens=thinking_tokens,
                messageType="TOOL",
                schemaVersion=2,
                io="OUTPUT",
                parts=parts,
            )

        final_text = final_response
        if (
            web_search_enabled
            and inline_citations_enabled
            and latest_grounding_metadata
            and final_response
        ):
            response_stub = SimpleNamespace(
                text=final_response,
                candidates=[
                    SimpleNamespace(grounding_metadata=latest_grounding_metadata)
                ],
            )
            final_text = self._add_citations(response_stub)

        parts = []
        if thinking_response:
            parts.append({"type": "THINKING", "thinking": thinking_response})
        if final_text:
            parts.append({"type": "TEXT", "text": final_text})
        for media_info in image_data or []:
            parts.append({"type": "MEDIA", "media_info": media_info})

        return AskModelEngineResponse2(
            response=final_text,
            response_tokens=output_tokens,
            prompt_tokens=input_tokens,
            cache_read_tokens=cache_read_tokens,
            thinking_tokens=thinking_tokens,
            messageType="CHAT",
            schemaVersion=2,
            io="OUTPUT",
            parts=parts,
        )

    def _count_tokens(self, contents: List[types.Content]) -> int:
        try:
            response = self.google_client.models.count_tokens(
                model=self.model_name,
                contents=contents,
            )
            return response.total_tokens
        except Exception as e:
            raise RuntimeError(f"Failed to count tokens: {e}")

    def _flatten_schema_tool(self, tools_result, schema_tool_name: str = "return_json"):
        """
        If all tool_use entries are the schema pseudo-tool, return (True, json_str).
        If mixed tools or different tool, return (False, None).
        """
        if not tools_result:
            return False, None

        if any(tr.get("name") != schema_tool_name for tr in tools_result):
            return False, None

        payloads = [tr.get("arguments") for tr in tools_result]

        norm = []
        for p in payloads:
            if isinstance(p, (dict, list)):
                norm.append(p)
            elif isinstance(p, str):
                try:
                    norm.append(json.loads(p))
                except Exception:
                    norm.append(p)
            else:
                norm.append(p)

        if len(norm) == 1:
            final_py = norm[0]
        else:
            if all(isinstance(x, dict) for x in norm):
                merged = {}
                for d in norm:
                    merged.update(d)
                final_py = merged
            elif all(isinstance(x, list) for x in norm):
                arr = []
                for a in norm:
                    arr.extend(a)
                final_py = arr
            else:
                final_py = norm

        try:
            json_str = json.dumps(final_py, ensure_ascii=False)
        except Exception:
            json_str = str(final_py)

        return True, json_str

    def _create_media_info(self, mime_type: str, image_bytes: bytes) -> Dict:
        """
        Create a MessageInputMedia-shaped dict for Java to persist into the room folder.
        """
        if not mime_type:
            mime_type = "application/octet-stream"

        if mime_type == "image/jpeg":
            file_format = "jpeg"
        elif mime_type.startswith("image/"):
            file_format = mime_type.split("/", 1)[1]
        else:
            file_format = "bin"

        base64_data = base64.b64encode(image_bytes).decode("utf-8")
        file_name = f"gen_{uuid.uuid4().hex}.{file_format}"

        return {
            "fileName": file_name,
            "base64Data": base64_data,
            "fileFormat": file_format,
            "mimeType": mime_type,
            "mediaInputType": "FILE",
        }

    def _find_citation_insert_index(self, text: str, segment) -> Optional[int]:
        """Match the grounded span inside the response text to place citations accurately."""
        segment_text = getattr(segment, "text", None) or getattr(
            segment, "content", None
        )
        snippet = segment_text.strip() if segment_text else None

        if not snippet:
            return getattr(segment, "end_index", None)

        text_lower = text.lower()
        snippet_lower = snippet.lower()
        approx_start = getattr(segment, "start_index", None)

        search_windows = []
        if isinstance(approx_start, int):
            window_start = max(0, approx_start - 200)
            window_end = min(len(text_lower), approx_start + len(snippet_lower) + 200)
            search_windows.append((window_start, window_end))
        search_windows.append((0, len(text_lower)))

        for start, end in search_windows:
            idx = text_lower.find(snippet_lower, start, end)
            if idx != -1:
                return idx + len(snippet)

        return getattr(segment, "end_index", None)

    def _add_citations(self, response):
        try:
            text = response.text
            supports = response.candidates[0].grounding_metadata.grounding_supports
            chunks = response.candidates[0].grounding_metadata.grounding_chunks
        except Exception:
            return getattr(response, "text", "") or ""

        if not supports or not chunks:
            return text or ""

        # Sort supports by end_index in descending order to avoid shifting issues when inserting.
        sorted_supports = sorted(
            supports, key=lambda s: getattr(s.segment, "end_index", 0), reverse=True
        )

        for support in sorted_supports:
            segment = getattr(support, "segment", None)
            if not segment:
                continue

            insert_pos = self._find_citation_insert_index(text, segment)
            if insert_pos is None:
                continue

            if support.grounding_chunk_indices:
                citation_links = []
                for i in support.grounding_chunk_indices:
                    if 0 <= i < len(chunks):
                        uri = getattr(getattr(chunks[i], "web", None), "uri", None)
                        if uri:
                            citation_links.append(f"<sup>[{i + 1}]({uri})</sup>")

                if citation_links:
                    if len(citation_links) == 1:
                        citation_string = citation_links[0]
                    else:
                        citation_string = "<sup>,</sup>".join(citation_links)
                    insert_pos = max(0, min(len(text), insert_pos))
                    if insert_pos < len(text) and text[insert_pos].isalnum():
                        insert_pos += 1
                    while insert_pos > 0 and text[insert_pos - 1] in ("\n", "\r"):
                        insert_pos -= 1
                    text = text[:insert_pos] + citation_string + text[insert_pos:]

        return text
