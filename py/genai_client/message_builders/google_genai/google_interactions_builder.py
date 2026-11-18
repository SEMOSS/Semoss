"""Translate SEMOSS history into the current, stateless Interactions protocol."""

import base64
from copy import deepcopy
import json

from ..semoss_base.builtin_tools import normalize_built_in_tools
from ..semoss_base.media_types import (
    decode_base64_text,
    is_text_mime_type,
    prepare_base64_media,
    resolve_mime_type,
)
from ..semoss_base.reasoning import normalize_reasoning
from ..semoss_base.semoss_models import parse_multimodal_tool_response
from ...utils import string_to_bool

REPLAY_KEY = "google_interactions"


def validate_replay(value):
    if (
        not isinstance(value, dict)
        or value.get("version") != 1
        or not isinstance(value.get("model"), str)
        or not value["model"]
    ):
        raise ValueError("Unsupported Google Interactions history version")
    steps = value.get("steps")
    if not isinstance(steps, list) or len(steps) > 4096:
        raise ValueError("Invalid Google Interactions history steps")
    if len(json.dumps(value).encode("utf-8")) > 64 * 1024 * 1024:
        raise ValueError("Google Interactions history exceeds 64 MiB per message")
    for step in steps:
        if not isinstance(step, dict) or not isinstance(step.get("type"), str):
            raise ValueError("Invalid Google Interactions history step")
        if step["type"] == "user_input":
            raise ValueError(
                "Generated Google Interactions history cannot contain user input"
            )
    return steps


def media_content(media):
    mime = resolve_mime_type(media.mime_type, media.file_name, media.format)
    if media.type == "url" and media.url:
        payload = {"uri": media.url}
    elif media.type == "base64":
        data, mime = prepare_base64_media(media)
        if is_text_mime_type(mime):
            return {"type": "text", "text": decode_base64_text(data)}
        base64.b64decode(data, validate=True)
        payload = {"data": data}
    else:
        raise ValueError("Google Interactions media requires a URI or base64 data")
    kind = mime.split("/", 1)[0]
    return {
        "type": kind if kind in {"image", "audio", "video"} else "document",
        "mime_type": mime,
        **payload,
    }


class GoogleInteractionsMessageBuilder:
    def build_messages(self, messages, settings, safety_settings=None):
        if not messages:
            raise ValueError("Google Interactions requires at least one message")
        steps = []
        calls = {}
        native_started = False
        for message in messages:
            if message.google_interactions is not None:
                native_started = True
                if message.io != "OUTPUT":
                    raise ValueError(
                        "Google Interactions replay belongs to output messages"
                    )
                native = deepcopy(validate_replay(message.google_interactions))
                media = [p.media_info for p in message.parts or [] if p.type == "MEDIA"]
                for step in native:
                    for block in step.get("content", []):
                        index = block.pop("semoss_media_index", None)
                        if index is not None:
                            if not isinstance(index, int) or not 0 <= index < len(
                                media
                            ):
                                raise ValueError(
                                    "Missing Google Interactions replay attachment"
                                )
                            restored = media_content(media[index])
                            if restored["type"] != block["type"]:
                                raise ValueError(
                                    "Google Interactions replay attachment type changed"
                                )
                            block.update(restored)
                    if step["type"] == "function_call":
                        calls[step["id"]] = step["name"]
                steps.extend(native)
                continue

            if native_started and message.io == "OUTPUT":
                raise ValueError(
                    "Missing native Google Interactions turn; start a new conversation"
                )

            content = []

            def flush():
                if content:
                    steps.append(
                        {
                            "type": (
                                "user_input"
                                if message.io == "INPUT"
                                else "model_output"
                            ),
                            "content": list(content),
                        }
                    )
                    content.clear()

            if message.parts:
                for part in message.parts:
                    if part.type == "TEXT":
                        content.append({"type": "text", "text": part.text})
                    elif part.type == "MEDIA":
                        content.append(media_content(part.media_info))
                    elif part.type == "TOOL_CALL":
                        flush()
                        call = part.tool_call
                        if call.thought_signature:
                            raise ValueError(
                                "Start a new Interactions conversation; legacy tool signatures cannot be converted"
                            )
                        if call.server_tool:
                            raise ValueError(
                                "Missing native Google Interactions built-in tool history"
                            )
                        if not call.id or not isinstance(
                            call.function.parameters, dict
                        ):
                            raise ValueError(
                                "A function call requires an ID and object arguments"
                            )
                        steps.append(
                            {
                                "type": "function_call",
                                "id": call.id,
                                "name": call.function.name,
                                "arguments": call.function.parameters,
                            }
                        )
                        calls[call.id] = call.function.name
                    elif part.type == "TOOL_RESULT":
                        flush()
                        result = part.tool_result
                        steps.append(
                            self._result(
                                result.id,
                                result.output,
                                calls,
                                result.tool_status in {"ERROR", "error", "failed"},
                            )
                        )
            elif message.type == "INPUT_TOOL_EXEC":
                steps.append(self._result(message.tool_call_id, message.content, calls))
            else:
                if message.content is not None:
                    content.append({"type": "text", "text": message.content})
                content.extend(media_content(m) for m in message.media_content or [])
                for call in message.tool_calls or []:
                    flush()
                    call = call if isinstance(call, dict) else call.model_dump()
                    if call.get("thought_signature"):
                        raise ValueError(
                            "Legacy tool signatures cannot be replayed as Interactions"
                        )
                    function = call["function"]
                    args = function.get("arguments", function.get("parameters", {}))
                    args = json.loads(args) if isinstance(args, str) else args
                    if not call.get("id") or not isinstance(args, dict):
                        raise ValueError(
                            "A function call requires an ID and object arguments"
                        )
                    steps.append(
                        {
                            "type": "function_call",
                            "id": call["id"],
                            "name": function["name"],
                            "arguments": args,
                        }
                    )
                    calls[call["id"]] = function["name"]
            flush()

        request = self._config(dict(messages[-1].param_map), settings)
        request["input"] = steps
        if safety_settings is not None:
            if isinstance(safety_settings, dict):
                safety_settings = [
                    {"category": k, "threshold": v} for k, v in safety_settings.items()
                ]
            request["safety_settings"] = []
            for setting in safety_settings:
                category = (
                    setting.get("type", setting.get("category", ""))
                    .lower()
                    .removeprefix("harm_category_")
                )
                threshold = setting["threshold"].lower()
                if category not in {
                    "harassment",
                    "hate_speech",
                    "sexually_explicit",
                    "dangerous_content",
                    "civic_integrity",
                }:
                    raise ValueError(
                        f"Unsupported Interactions safety category: {category}"
                    )
                if threshold not in {
                    "block_low_and_above",
                    "block_medium_and_above",
                    "block_only_high",
                    "block_none",
                    "off",
                }:
                    raise ValueError(
                        f"Unsupported Interactions safety threshold: {threshold}"
                    )
                request["safety_settings"].append(
                    {
                        "type": category,
                        "threshold": threshold,
                        **(
                            {"method": setting["method"].lower()}
                            if setting.get("method")
                            else {}
                        ),
                    }
                )
        return request

    @staticmethod
    def _result(call_id, output, calls, is_error=False):
        if call_id not in calls:
            raise ValueError(f"No function call matches tool result {call_id}")
        blocks = (
            parse_multimodal_tool_response(output) if isinstance(output, str) else None
        )
        result = "" if output is None else output
        if blocks is not None:
            result = []
            for block in blocks:
                if block.type == "text":
                    result.append({"type": "text", "text": block.text or ""})
                elif block.type in {"image", "document"} and block.data:
                    data, mime = prepare_base64_media(
                        block,
                        "image/png" if block.type == "image" else "application/pdf",
                    )
                    if is_text_mime_type(mime):
                        result.append(
                            {"type": "text", "text": decode_base64_text(data)}
                        )
                    else:
                        base64.b64decode(data, validate=True)
                        result.append(
                            {"type": block.type, "mime_type": mime, "data": data}
                        )
                else:
                    raise ValueError(
                        "Unsupported Google Interactions tool-result media"
                    )
        return {
            "type": "function_result",
            "call_id": call_id,
            "name": calls[call_id],
            "result": result,
            "is_error": is_error,
        }

    @staticmethod
    def _config(params, settings):
        stream = params.get("streaming")
        if stream is None:
            stream = params.get("stream", True)
        stream = string_to_bool(stream) if isinstance(stream, str) else bool(stream)
        config = {}
        limit = next(
            (
                params[k]
                for k in (
                    "max_new_tokens",
                    "max_completion_tokens",
                    "max_tokens",
                    "max_output_tokens",
                )
                if params.get(k) is not None
            ),
            settings.max_tokens,
        )
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
                raise ValueError("max_output_tokens must be a positive integer")
            config["max_output_tokens"] = limit
        for key in (
            "top_k",
            "presence_penalty",
            "frequency_penalty",
            "http_options",
            "cached_content",
        ):
            if params.get(key) is not None:
                raise ValueError(
                    f"{key} is not supported by the SEMOSS Interactions client"
                )
        for key in ("temperature", "top_p", "seed", "stop_sequences"):
            if params.get(key) is not None:
                if "gemini-4" in settings.model_name and key in {
                    "temperature",
                    "top_p",
                }:
                    raise ValueError(f"{key} is not supported by Gemini 4")
                config[key] = params[key]
        thinking = params.get("thinking")
        effort = params.get("effort", settings.effort)
        if (
            params.get("thinking_budget") is not None
            or settings.thinking_budget is not None
            or isinstance(thinking, dict)
            and thinking.get("budget_tokens") is not None
            or effort is not None
            and (not isinstance(effort, str) or effort.lstrip("-").isdigit())
        ):
            raise ValueError(
                "Interactions uses effort/thinking_level; remove the numeric thinking_budget before enabling it"
            )
        reasoning = normalize_reasoning(params, settings)
        config["thinking_summaries"] = "auto" if reasoning else "none"
        if reasoning:
            level = reasoning.effort
            if level == "max":
                level = (
                    "xhigh" if "gemini-4-hydrogen" in settings.model_name else "high"
                )
            if level in {"none", "minimal"} and any(
                s in settings.model_name
                for s in ("gemini-4", "pro", "gemini-3.8", "gemini-2.5")
            ):
                level = "low"
            elif level == "none":
                level = "minimal"
            if level == "xhigh" and "gemini-4-hydrogen" not in settings.model_name:
                level = "high"
            if "gemini-3-pro-preview" in settings.model_name and level == "medium":
                level = "high"
            if "gemini-3.1-flash-lite-image" in settings.model_name and level in {
                "low",
                "medium",
            }:
                level = "high"
            config["thinking_level"] = level

        tools = []
        for tool in params.get("tools") or []:
            tools.append(
                {
                    "type": "function",
                    "name": tool["name"],
                    "description": tool.get("description", ""),
                    "parameters": tool.get("inputSchema")
                    or {"type": "object", "properties": {}},
                }
            )
        for selection in normalize_built_in_tools(params.get("built_in_tools")):
            for kind, options in selection["params"].items():
                if kind not in {
                    "google_search",
                    "url_context",
                    "code_execution",
                    "file_search",
                    "google_maps",
                }:
                    raise ValueError(f"Unsupported Interactions built-in tool: {kind}")
                if not isinstance(options, dict) or "type" in options:
                    raise ValueError(f"Invalid options for {kind}")
                tools.append({"type": kind, **options})
        choice = params.get("tool_choice") or {}
        if tools:
            mode = choice.get("type", "auto")
            if mode not in {"auto", "required", "forced", "none"}:
                raise ValueError(f"Unsupported tool choice: {mode}")
            name = choice.get("name")
            if mode == "forced" and not name:
                raise ValueError("Forced tool choice requires a function name")
            if name and mode in {"forced", "required"}:
                if name not in {t["name"] for t in tools if t["type"] == "function"}:
                    raise ValueError("Selected function is not declared")
                config["tool_choice"] = {
                    "allowed_tools": {"mode": "any", "tools": [name]}
                }
            else:
                config["tool_choice"] = "any" if mode == "required" else mode
        formats = [{"type": m.lower()} for m in settings.modalities or ["text"]]
        schema = params.get("schema")
        text_config = params.get("text") or {}
        if not isinstance(text_config, dict):
            raise ValueError("OpenAI text configuration must be an object")
        output_format = params.get("response_format") or text_config.get("format")
        if output_format is not None:
            if not isinstance(output_format, dict):
                raise ValueError("OpenAI response format must be an object")
            if output_format.get("type") == "json_schema":
                schema_format = output_format.get("json_schema", output_format)
                if not isinstance(schema_format, dict):
                    raise ValueError("json_schema response format must be an object")
                schema = schema or schema_format.get("schema")
                if not isinstance(schema, dict):
                    raise ValueError("json_schema response format requires a schema")
            elif output_format.get("type") == "json_object":
                formats = [{"type": "text", "mime_type": "application/json"}]
            elif output_format.get("type") != "text":
                raise ValueError("Unsupported OpenAI response format")
        if schema is not None:
            formats = [
                {"type": "text", "mime_type": "application/json", "schema": schema}
            ]
        elif params.get("response_mime_type"):
            formats = [{"type": "text", "mime_type": params["response_mime_type"]}]
        request = {
            "generation_config": config,
            "stream": stream,
            "store": False,
            "response_format": formats,
        }
        system = params.get("system_prompt") or params.get("instructions")
        if system is not None:
            request["system_instruction"] = system
        if tools:
            request["tools"] = tools
        return request
