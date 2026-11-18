"""Stateless Interactions inference, retaining the native timeline for replay."""

import base64
from copy import deepcopy
import json
from urllib.parse import urlparse

from google.genai import types
from smss_thread_local import get_smss_stream

from ...constants import AskModelEngineResponse2
from ...clients.google_clients import GoogleClient
from ...message_builders.google_genai.google_interactions_builder import (
    GoogleInteractionsMessageBuilder,
    REPLAY_KEY,
    validate_replay,
)
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil
from ...usage_ceiling import PromptCount, count_prompt_tokens, note_output_stop
from ..model_engine_exception import ErrorDetails, ModelEngineException


class InteractionErrorDetails(ErrorDetails):
    numberOfTokensInPrompt: int
    numberOfTokensInResponse: int
    numberOfThinkingTokens: int
    numberOfCacheReadTokens: int | None = None
    schemaVersion: int = 2
    io: str = "OUTPUT"
    parts: list
    metadata: dict


def _dict(value):
    return (
        deepcopy(value)
        if isinstance(value, dict)
        else value.model_dump(mode="json", exclude_none=True)
    )


def _emit(stream, kind, payload, **kwargs):
    if stream is not None:
        stream(payload, stream_type=kind, **kwargs)


class InteractionStream:
    def __init__(self, stream):
        self.stream = stream
        self.steps = {}
        self.arguments = {}
        self.stopped = set()
        self.interaction = {}
        self.completed = False

    def accept(self, event):
        event = _dict(event)
        usage = (event.get("metadata") or {}).get("total_usage") or event.get("usage")
        if usage:
            self.interaction["usage"] = usage
        kind = event["event_type"]
        if kind in {"interaction.created", "interaction.completed"}:
            self.interaction.update(event["interaction"])
            self.completed = kind == "interaction.completed"
        elif kind == "interaction.status_update":
            self.interaction["status"] = event["status"]
        elif kind == "step.start":
            index = event["index"]
            if index in self.steps or len(self.steps) >= 4096:
                raise ValueError("Invalid Interactions stream step index")
            self.steps[index] = event["step"]
            self._visible(event["step"])
        elif kind == "step.delta":
            index = event["index"]
            if index not in self.steps or index in self.stopped:
                raise ValueError("Interactions delta has no open step")
            step, delta = self.steps[index], event["delta"]
            dtype = delta["type"]
            if dtype == "text":
                blocks = step.setdefault("content", [])
                if not blocks or blocks[-1]["type"] != "text":
                    blocks.append({"type": "text", "text": ""})
                blocks[-1]["text"] = blocks[-1].get("text", "") + delta.get("text", "")
                _emit(
                    self.stream,
                    "content",
                    StreamUtil.create_content_chunk(delta.get("text", "")),
                )
            elif dtype == "text_annotation_delta":
                blocks = step.setdefault("content", [])
                if not blocks or blocks[-1]["type"] != "text":
                    # Grounded streams can announce citations before the associated text.
                    blocks.append({"type": "text", "text": ""})
                blocks[-1].setdefault("annotations", []).extend(
                    delta.get("annotations", [])
                )
            elif dtype == "arguments_delta":
                self.arguments[index] = self.arguments.get(index, "") + delta.get(
                    "arguments", ""
                )
            elif dtype == "thought_signature":
                step["signature"] = delta["signature"]
            elif dtype == "thought_summary":
                block = delta.get("content")
                if block:
                    step.setdefault("summary", []).append(block)
                    self._visible({"type": "thought", "summary": [block]})
            elif dtype in {"image", "audio", "video", "document"}:
                step.setdefault("content", []).append(delta)
            elif dtype == step["type"]:
                # Built-in tool deltas contain signed arguments/results, not client calls.
                step.update(delta)
            else:
                raise ValueError(f"Unsupported Interactions delta: {dtype}")
        elif kind == "step.stop":
            index = event["index"]
            if index not in self.steps or index in self.stopped:
                raise ValueError("Interactions stream stopped an unknown step")
            if index in self.arguments:
                self.steps[index]["arguments"] = json.loads(self.arguments[index])
            self.stopped.add(index)
        elif kind == "error":
            raise RuntimeError(
                f"Google Interactions stream error: {event.get('error', {}).get('message', 'unknown error')}"
            )

    def _visible(self, step):
        if step["type"] == "model_output":
            for block in step.get("content", []):
                if block["type"] == "text":
                    _emit(
                        self.stream,
                        "content",
                        StreamUtil.create_content_chunk(block.get("text", "")),
                    )
        elif step["type"] == "thought":
            for block in step.get("summary", []):
                if block["type"] == "text":
                    _emit(
                        self.stream,
                        "thinking",
                        StreamUtil.create_thinking_chunk(block.get("text", "")),
                    )

    def result(self):
        if not self.completed or self.stopped != self.steps.keys():
            raise RuntimeError("Google Interactions stream ended before completion")
        result = dict(self.interaction)
        result["steps"] = [self.steps[i] for i in sorted(self.steps)]
        return result


def _citation_text(block):
    text = block.get("text", "")
    inserts = {}
    for annotation in block.get("annotations", []):
        url = annotation.get("url")
        end = annotation.get("end_index")
        if (
            annotation.get("type") == "url_citation"
            and isinstance(url, str)
            and urlparse(url).scheme in {"http", "https"}
            and isinstance(end, int)
            and 0 <= end <= len(text.encode("utf-8"))
        ):
            try:
                end = len(text.encode("utf-8")[:end].decode("utf-8"))
            except UnicodeDecodeError:
                continue
            label = annotation.get("title") or urlparse(url).netloc
            label = label.replace("[", "\\[").replace("]", "\\]")
            url = url.replace("(", "%28").replace(")", "%29").replace(" ", "%20")
            citation = f" [{label}]({url})"
            if citation not in inserts.setdefault(end, []):
                inserts[end].append(citation)
    for end, links in sorted(inserts.items(), reverse=True):
        text = text[:end] + "".join(links) + text[end:]
    return text


def _usage(interaction, request):
    usage = interaction.get("usage") or {}
    estimated = not {"total_input_tokens", "total_output_tokens"} <= usage.keys()
    prompt = usage.get("total_input_tokens")
    output = usage.get("total_output_tokens")
    if prompt is None:
        prompt = _estimate_prompt(request).tokens
    if output is None:
        output = count_prompt_tokens(interaction.get("steps", [])).tokens
    thoughts = usage.get("total_thought_tokens", 0)
    return (
        dict(
            prompt_tokens=prompt,
            response_tokens=output + thoughts,
            thinking_tokens=thoughts,
            cache_read_tokens=usage.get("total_cached_tokens"),
        ),
        estimated,
    )


def _normalize(client, interaction, request, inline_citations):
    status = interaction.get("status")
    if status not in {"completed", "requires_action", "incomplete"}:
        raise RuntimeError(
            f"Google Interactions ended with status {status}; the request was not retried"
        )
    steps = deepcopy(interaction.get("steps") or [])
    replay = {"version": 1, "model": client.model_name, "steps": steps}
    validate_replay(replay)
    parts, tools, text, wire_text = [], [], [], []
    media_index = 0
    for step in steps:
        kind = step["type"]
        if kind == "thought":
            summary = "".join(
                b.get("text", "")
                for b in step.get("summary", [])
                if b["type"] == "text"
            )
            if summary:
                parts.append({"type": "THINKING", "thinking": summary})
        elif kind == "function_call":
            if (
                not step.get("id")
                or not step.get("name")
                or not isinstance(step.get("arguments"), dict)
            ):
                raise ValueError(
                    "Google Interactions returned an invalid function call"
                )
            tool = {
                "id": step["id"],
                "type": "function",
                "name": step["name"],
                "arguments": step["arguments"],
            }
            tools.append(tool)
            parts.append({"type": "TOOL_CALL", "tool_call": tool})
        elif kind == "model_output":
            for block in step.get("content", []):
                if block["type"] == "text":
                    rendered = (
                        _citation_text(block)
                        if inline_citations
                        else block.get("text", "")
                    )
                    text.append(rendered)
                    wire_text.append(block.get("text", ""))
                    parts.append({"type": "TEXT", "text": rendered})
                elif block["type"] in {"image", "audio", "video", "document"}:
                    if block.get("data"):
                        media = client._create_media_info(
                            block.get("mime_type"),
                            base64.b64decode(block["data"], validate=True),
                        )
                        del block["data"]
                        block["semoss_media_index"] = media_index
                    elif block.get("uri"):
                        media = {
                            "mediaInputType": "URL",
                            "sourceUrl": block["uri"],
                            "mimeType": block.get("mime_type"),
                        }
                    else:
                        raise ValueError(
                            "Google Interactions returned media without data or URI"
                        )
                    media_index += 1
                    parts.append({"type": "MEDIA", "media_info": media})
    if status == "requires_action" and not tools:
        raise ValueError("Google Interactions requires action without a function call")
    if tools and status != "requires_action":
        raise ValueError(
            "Google Interactions returned function calls without an action handoff"
        )
    usage, estimated = _usage(interaction, request)
    # Only a provider-reported output limit can mark a ceiling-lowered call capped.
    details = interaction.get("incomplete_details") or {}
    limited = details.get("reason") in {"max_output_tokens", "max_tokens"}
    note_output_stop(limited)
    metadata = {
        REPLAY_KEY: replay,
        "interaction_status": status,
        "usage_estimated": estimated,
        "tool_use_tokens": (interaction.get("usage") or {}).get(
            "total_tool_use_tokens", 0
        ),
    }
    response = AskModelEngineResponse2(
        response=tools if tools else "".join(text),
        messageType="TOOL" if tools else "CHAT",
        schemaVersion=2,
        io="OUTPUT",
        parts=parts,
        metadata=metadata,
        **usage,
    )
    assistant = {"role": "assistant", "content": "".join(wire_text)}
    if tools:
        assistant["tool_calls"] = [
            {
                "id": t["id"],
                "type": "function",
                "function": {
                    "name": t["name"],
                    "arguments": json.dumps(t["arguments"], ensure_ascii=False),
                },
            }
            for t in tools
        ]
    return (
        response,
        assistant,
        "tool_calls" if tools else "length" if limited else "stop",
    )


def _failure(client, interaction, request, error):
    usage, estimated = _usage(interaction, request)
    details = ModelEngineException(
        error=error, client="google", model=client.model_name
    ).parse_error()
    partial = [
        {"type": "TEXT", "text": b["text"]}
        for step in interaction.get("steps", [])
        if step["type"] == "model_output"
        for b in step.get("content", [])
        if b["type"] == "text"
    ]
    return InteractionErrorDetails(
        **details.model_dump(),
        numberOfTokensInPrompt=usage["prompt_tokens"],
        numberOfTokensInResponse=usage["response_tokens"],
        numberOfThinkingTokens=usage["thinking_tokens"],
        numberOfCacheReadTokens=usage["cache_read_tokens"],
        parts=partial,
        metadata={
            "interaction_status": interaction.get("status", "interrupted"),
            "usage_estimated": estimated,
        },
    )


def _estimate_prompt(request):
    return count_prompt_tokens(
        [request.get("system_instruction"), request["input"]],
        [request.get("tools"), request.get("response_format")],
        message_count=len(request["input"]),
    )


def _count_prompt(client, request):
    # Count native content without sending opaque signatures to a different API.
    contents = []
    try:
        for step in request["input"]:
            parts = []
            if step["type"] in {"user_input", "model_output"}:
                for block in step.get("content", []):
                    if block["type"] == "text":
                        parts.append(types.Part.from_text(text=block["text"]))
                    elif block.get("data"):
                        parts.append(
                            types.Part.from_bytes(
                                data=base64.b64decode(block["data"]),
                                mime_type=block["mime_type"],
                            )
                        )
                    elif block.get("uri"):
                        parts.append(
                            types.Part.from_uri(
                                file_uri=block["uri"], mime_type=block["mime_type"]
                            )
                        )
            elif step["type"] != "thought":
                parts.append(
                    types.Part.from_text(
                        text=json.dumps(
                            {k: v for k, v in step.items() if k != "signature"}
                        )
                    )
                )
            if parts:
                contents.append(
                    types.Content(
                        role="model" if step["type"] == "model_output" else "user",
                        parts=parts,
                    )
                )
        extras = [
            request.get("system_instruction"),
            request.get("tools"),
            request.get("response_format"),
        ]
        contents.append(
            types.Content(
                role="user", parts=[types.Part.from_text(text=json.dumps(extras))]
            )
        )
        total = client.google_client.models.count_tokens(
            model=client.model_name, contents=contents
        ).total_tokens
        if total is None:
            raise ValueError("Missing token count")
        return PromptCount(tokens=int(total), estimated=True)
    except Exception:
        return _estimate_prompt(request)


def ask_interactions(client, messages, inline_citations=True):
    compatibility = messages[-1].param_map.get("google_interactions_replay_available")
    if compatibility is False:
        raise ValueError(
            "Interactions through the Anthropic endpoint requires KEEP_INPUT_OUTPUT, KEEP_CONVERSATION_HISTORY, and use_history"
        )
    if not client.client_config.api_key and client.client_config.region not in {
        "global",
        "us",
        "eu",
    }:
        raise ValueError(
            "Google Interactions requires a Vertex global, us, or eu location"
        )
    request = GoogleInteractionsMessageBuilder().build_messages(
        messages, client.model_settings, client.safety_settings
    )
    if compatibility is True and any(
        f["type"] != "text" for f in request["response_format"]
    ):
        raise ValueError(
            "The Anthropic endpoint supports text and function output; use a SEMOSS room for generated media"
        )
    config = request["generation_config"]
    fit = client.fit_output_to_usage_ceiling(
        config.get("max_output_tokens"), lambda: _count_prompt(client, request)
    )
    if fit is not None and fit.lowered:
        config["max_output_tokens"] = fit.max_tokens
    # The SDK owns transport retries. Never restart a partially consumed generation.
    if client._interactions_client is None:
        # A per-call version override in SDK 3.4 drops the Vertex project/location prefix.
        client._interactions_client = GoogleClient(
            client.client_config, api_version="v1"
        ).genai_client
    result = client._interactions_client.interactions.create(
        model=client.model_name,
        automatic_continuation=False,
        **request,
    )
    stream = get_smss_stream() if request["stream"] else None
    if request["stream"]:
        accumulator = InteractionStream(stream)
        try:
            for event in result:
                accumulator.accept(event)
                if accumulator.completed:
                    break
            interaction = accumulator.result()
        except Exception as error:
            partial = dict(
                accumulator.interaction, steps=list(accumulator.steps.values())
            )
            usage, _ = _usage(partial, request)
            _emit(
                stream,
                "usage",
                StreamUtil.create_usage_chunk(
                    input_tokens=usage["prompt_tokens"],
                    output_tokens=usage["response_tokens"],
                    reasoning_tokens=usage["thinking_tokens"],
                ),
            )
            return _failure(client, partial, request, error)
        finally:
            if hasattr(result, "close"):
                result.close()
    else:
        interaction = _dict(result)
    try:
        if interaction.get("status") not in {"completed", "requires_action"}:
            raise RuntimeError(
                f"Google Interactions ended with status {interaction.get('status')}; start a new turn to continue"
            )
        response, assistant, finish = _normalize(
            client, interaction, request, inline_citations
        )
    except Exception as error:
        usage, _ = _usage(interaction, request)
        _emit(
            stream,
            "usage",
            StreamUtil.create_usage_chunk(
                input_tokens=usage["prompt_tokens"],
                output_tokens=usage["response_tokens"],
                reasoning_tokens=usage["thinking_tokens"],
            ),
        )
        return _failure(client, interaction, request, error)
    if request["stream"]:
        # Private frame consumed by the compatibility endpoint; never sent over its wire.
        if compatibility is True:
            _emit(
                stream,
                REPLAY_KEY,
                {REPLAY_KEY: response.metadata[REPLAY_KEY], "assistant": assistant},
            )
        for index, part in enumerate(
            p for p in response.parts if p["type"] == "TOOL_CALL"
        ):
            tool = part["tool_call"]
            for chunk in (
                StreamUtil.create_tool_id_chunk(index, tool["id"]),
                StreamUtil.create_tool_type_chunk(index),
                StreamUtil.create_function_name_chunk(index, tool["name"]),
                StreamUtil.create_function_arguments_chunk(
                    index, json.dumps(tool["arguments"])
                ),
            ):
                _emit(stream, "tool", chunk)
        for part in response.parts:
            if part["type"] == "MEDIA":
                _emit(
                    stream, "media", StreamUtil.create_media_chunk(part["media_info"])
                )
        _emit(
            stream,
            "usage",
            StreamUtil.create_usage_chunk(
                input_tokens=response.prompt_tokens,
                output_tokens=response.response_tokens,
                cache_read_input_tokens=response.cache_read_tokens,
                reasoning_tokens=response.thinking_tokens,
            ),
        )
        _emit(
            stream,
            "tool" if response.messageType == "TOOL" else "content",
            StreamUtil.create_finish_reason_chunk(finish),
            interim=False,
        )
    return response
