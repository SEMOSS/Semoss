"""Offline protocol and replay tests against the pinned Interactions schema."""

import base64
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from genai_client.message_builders.google_genai.google_interactions_builder import (
    GoogleInteractionsMessageBuilder,
    validate_replay,
)
from genai_client.message_builders.semoss_base.semoss_message_builder import (
    SEMOSSMessageBuilder,
)
from genai_client.message_builders.semoss_base.semoss_models import ModelSettings
from genai_client.text_generation.google_genai_clients import (
    google_interactions as adapter,
)
from genai_client.text_generation.google_genai_clients.google_genai_client import (
    GoogleGenAiTextClient,
)

MODEL = "gemini-4-carbon"
SETTINGS = ModelSettings(model_name=MODEL, max_tokens=1024)
USER = {"type": "INPUT_TEXT", "io": "INPUT", "inputPrompt": "Hello"}


@pytest.mark.parametrize("responses", [False, True])
def test_openai_json_schema_formats_are_translated(responses):
    schema = {"type": "object", "properties": {"answer": {"type": "string"}}}
    params = (
        {"text": {"format": {"type": "json_schema", "name": "answer", "schema": schema}}}
        if responses
        else {"response_format": {"type": "json_schema", "json_schema": {"name": "answer", "schema": schema}}}
    )
    built = request(**params)
    assert built["response_format"] == [{"type": "text", "mime_type": "application/json", "schema": schema}]


def test_openai_json_object_and_missing_schema():
    assert request(response_format={"type": "json_object"})["response_format"] == [
        {"type": "text", "mime_type": "application/json"}
    ]
    with pytest.raises(ValueError, match="requires a schema"):
        request(response_format={"type": "json_schema"})
    with pytest.raises(ValueError, match="must be an object"):
        request(response_format=[{"type": "text"}])
    with pytest.raises(ValueError, match="must be an object"):
        request(response_format={"type": "json_schema", "json_schema": "invalid"})
    with pytest.raises(ValueError, match="must be an object"):
        request(text="invalid")


def test_compatibility_stream_emits_private_replay_before_tool_handoff(monkeypatch):
    obj = client()
    events = []
    for index, step in enumerate(STEPS):
        events.extend([
            {"event_type": "step.start", "index": index, "step": step},
            {"event_type": "step.stop", "index": index},
        ])
    events.append({"event_type": "interaction.completed", "interaction": interaction()})
    obj.google_client.interactions.create.return_value = iter(events)
    frames = []
    monkeypatch.setattr(adapter, "get_smss_stream", lambda: lambda data, **kw: frames.append((data, kw)))
    result = adapter.ask_interactions(obj, messages(google_interactions_replay_available=True))
    assert result.messageType == "TOOL"
    state_index = next(i for i, (_, kw) in enumerate(frames) if kw["stream_type"] == "google_interactions")
    tool_index = next(i for i, (_, kw) in enumerate(frames) if kw["stream_type"] == "tool")
    assert state_index < tool_index
    state, _ = frames[state_index]
    assert state["google_interactions"]["steps"] == STEPS
    assert [t["id"] for t in state["assistant"]["tool_calls"]] == ["call-1", "call-2"]
    header = frames[tool_index][0]
    assert header["id"] == "call-1" and header["function"]["name"] == "lookup"


def test_compatibility_retention_failure_is_reported_before_calling_google():
    obj = client()
    with pytest.raises(ValueError, match="KEEP_INPUT_OUTPUT"):
        adapter.ask_interactions(obj, messages(google_interactions_replay_available=False))
    obj.google_client.interactions.create.assert_not_called()


STEPS = [
    {"type": "thought", "signature": "opaque-signed-state"},
    {"type": "model_output", "content": [{"type": "text", "text": "Checking."}]},
    {
        "type": "function_call",
        "id": "call-1",
        "name": "lookup",
        "arguments": {"q": "α"},
    },
    {
        "type": "function_call",
        "id": "call-2",
        "name": "lookup",
        "arguments": {"q": "β"},
    },
]


def messages(history=None, **params):
    return SEMOSSMessageBuilder().build_messages(
        history or [deepcopy(USER)], params, SETTINGS
    )


def request(history=None, **params):
    return GoogleInteractionsMessageBuilder().build_messages(
        messages(history, **params), SETTINGS
    )


def client():
    obj = SimpleNamespace(
        model_name=MODEL,
        model_settings=SETTINGS,
        safety_settings=None,
        client_config=SimpleNamespace(api_key="test-key", region=None),
        google_client=Mock(),
        fit_output_to_usage_ceiling=Mock(return_value=None),
    )
    obj._create_media_info = GoogleGenAiTextClient._create_media_info.__get__(obj)
    obj._interactions_client = obj.google_client
    return obj


def interaction(steps=None, status="requires_action"):
    return {
        "status": status,
        "steps": deepcopy(STEPS if steps is None else steps),
        "usage": {
            "total_input_tokens": 31,
            "total_output_tokens": 17,
            "total_thought_tokens": 13,
            "total_cached_tokens": 7,
            "total_tool_use_tokens": 5,
        },
    }


def test_configuration_is_stateless_and_preserves_schema_tool_choice_and_safety():
    schema = {
        "type": "object",
        "properties": {"q": {"type": "string", "minLength": 1}},
        "required": ["q"],
    }
    built = request(
        tools=[{"name": "lookup", "inputSchema": schema}],
        tool_choice={"type": "forced", "name": "lookup"},
        schema=schema,
        system_prompt="System",
        max_tokens=30,
        max_new_tokens=20,
        thinking=True,
        effort="high",
        streaming="false",
    )
    assert built["tools"][0]["parameters"] == schema
    assert built["generation_config"] == {
        "max_output_tokens": 20,
        "thinking_summaries": "auto",
        "thinking_level": "high",
        "tool_choice": {"allowed_tools": {"mode": "any", "tools": ["lookup"]}},
    }
    assert built["system_instruction"] == "System"
    assert built["response_format"] == [
        {"type": "text", "mime_type": "application/json", "schema": schema}
    ]
    assert built["store"] is False and built["stream"] is False
    safe = GoogleInteractionsMessageBuilder().build_messages(
        messages(), SETTINGS, {"HARM_CATEGORY_HARASSMENT": "BLOCK_ONLY_HIGH"}
    )
    assert safe["safety_settings"] == [
        {"type": "harassment", "threshold": "block_only_high"}
    ]


@pytest.mark.parametrize(
    "params",
    [
        {"thinking_budget": 1024},
        {"thinking": {"type": "enabled", "budget_tokens": 4096}},
        {"effort": 4096},
        {"top_k": 20},
        {"temperature": 0.2},
        {"max_tokens": 0},
    ],
)
def test_unsupported_explicit_configuration_is_rejected(params):
    with pytest.raises(ValueError):
        request(**params)


def test_native_steps_replay_once_and_results_match_ids_in_reverse_order():
    history = [
        USER,
        {
            "type": "RESPONSE_TOOL",
            "io": "OUTPUT",
            "content": "Do not duplicate",
            "google_interactions": {
                "version": 1,
                "model": MODEL,
                "steps": deepcopy(STEPS),
            },
        },
        {
            "type": "INPUT_TOOL_EXEC",
            "io": "INPUT",
            "tool_call_id": "call-2",
            "content": "false",
        },
        {
            "type": "INPUT_TOOL_EXEC",
            "io": "INPUT",
            "tool_call_id": "call-1",
            "content": "",
        },
    ]
    built = request(history)
    assert built["input"][1:5] == STEPS
    assert [s["call_id"] for s in built["input"][-2:]] == ["call-2", "call-1"]
    assert built["input"][-1]["result"] == ""
    assert history[1]["google_interactions"]["steps"] == STEPS


@pytest.mark.parametrize("tool_handoff", [False, True])
def test_model_switch_preserves_prior_state_and_uses_current_model_settings(
    tool_handoff,
):
    prior_steps = deepcopy(STEPS if tool_handoff else STEPS[:2])
    prior_state = {"version": 1, "model": MODEL, "steps": prior_steps}
    history = [
        USER,
        {
            "type": "RESPONSE_TOOL" if tool_handoff else "RESPONSE_TEXT",
            "io": "OUTPUT",
            "content": "Display text must not replace the native steps",
            "google_interactions": prior_state,
        },
    ]
    history.extend(
        [
            {
                "type": "INPUT_TOOL_EXEC",
                "io": "INPUT",
                "tool_call_id": step["id"],
                "content": "Result",
            }
            for step in reversed(prior_steps)
            if step["type"] == "function_call"
        ]
        if tool_handoff
        else [USER]
    )
    obj = client()
    obj.model_name = "gemini-4-hydrogen"
    obj.model_settings = SETTINGS.model_copy(update={"model_name": obj.model_name})
    obj.google_client.interactions.create.return_value = interaction(
        STEPS[:2], "completed"
    )

    response = adapter.ask_interactions(
        obj, messages(history, stream=False, thinking=True, effort="max")
    )

    sent = obj.google_client.interactions.create.call_args.kwargs
    assert sent["model"] == "gemini-4-hydrogen"
    assert sent["generation_config"]["thinking_level"] == "xhigh"
    assert sent["input"][1 : 1 + len(prior_steps)] == prior_steps
    assert history[1]["google_interactions"] == {
        "version": 1,
        "model": MODEL,
        "steps": STEPS if tool_handoff else STEPS[:2],
    }
    assert response.metadata["google_interactions"]["model"] == "gemini-4-hydrogen"
    if tool_handoff:
        assert [s["call_id"] for s in sent["input"][-2:]] == ["call-2", "call-1"]


@pytest.mark.parametrize("value", [False, 0, "", None])
def test_tool_results_do_not_coerce_false_or_zero_to_empty(value):
    result = GoogleInteractionsMessageBuilder._result("a", value, {"a": "lookup"})
    assert result["result"] == ("" if value is None else value)


def test_multimodal_tool_results_preserve_bytes_and_error_flag():
    data = base64.b64encode(b"test bytes").decode()
    output = json.dumps(
        {
            "SEMOSSMultimodalToolResponse": [
                {"type": "text", "text": "Found"},
                {"type": "image", "data": data, "mimeType": "image/png"},
                {"type": "document", "data": data, "mimeType": "application/pdf"},
            ]
        }
    )
    result = GoogleInteractionsMessageBuilder._result(
        "a", output, {"a": "lookup"}, True
    )
    assert result == {
        "type": "function_result",
        "call_id": "a",
        "name": "lookup",
        "is_error": True,
        "result": [
            {"type": "text", "text": "Found"},
            {"type": "image", "data": data, "mime_type": "image/png"},
            {"type": "document", "data": data, "mime_type": "application/pdf"},
        ],
    }


def test_stream_initial_content_fragmented_arguments_and_private_steps():
    frames = []
    accumulator = adapter.InteractionStream(
        lambda data, **kw: frames.append((data, kw))
    )
    events = [
        {"event_type": "interaction.created", "interaction": {"status": "in_progress"}},
        {"event_type": "step.start", "index": 0, "step": STEPS[0]},
        {"event_type": "step.stop", "index": 0},
        {
            "event_type": "step.start",
            "index": 1,
            "step": {
                "type": "model_output",
                "content": [{"type": "text", "text": "Hi "}],
            },
        },
        {
            "event_type": "step.delta",
            "index": 1,
            "delta": {"type": "text", "text": "世界"},
        },
        {"event_type": "step.stop", "index": 1},
        {
            "event_type": "step.start",
            "index": 2,
            "step": {
                "type": "function_call",
                "id": "call-1",
                "name": "lookup",
                "arguments": {},
            },
        },
        {
            "event_type": "step.delta",
            "index": 2,
            "delta": {"type": "arguments_delta", "arguments": '{"q":'},
        },
        {
            "event_type": "step.delta",
            "index": 2,
            "delta": {"type": "arguments_delta", "arguments": '"α"}'},
        },
        {"event_type": "step.stop", "index": 2},
        {
            "event_type": "interaction.completed",
            "interaction": {
                "status": "requires_action",
                "usage": interaction()["usage"],
            },
        },
    ]
    for event in events:
        accumulator.accept(event)
    result = accumulator.result()
    assert result["steps"][0] == STEPS[0]
    assert result["steps"][1]["content"][0]["text"] == "Hi 世界"
    assert result["steps"][2] == STEPS[2]
    assert [frame[0] for frame in frames] == [{"content": "Hi "}, {"content": "世界"}]
    normalized, _, _ = adapter._normalize(client(), result, request(), False)
    assert normalized.response_tokens == 30
    assert normalized.thinking_tokens == 13 and normalized.cache_read_tokens == 7
    assert normalized.parts[0]["text"] == "Hi 世界"
    assert normalized.metadata["google_interactions"]["steps"] == result["steps"]


def test_citations_use_utf8_byte_offsets_and_keep_native_text():
    block = {
        "type": "text",
        "text": "α世界!",
        "annotations": [
            {
                "type": "url_citation",
                "end_index": 8,
                "url": "https://example.com",
                "title": "Source",
            }
        ],
    }
    output, _, _ = adapter._normalize(
        client(),
        interaction([{"type": "model_output", "content": [block]}], "completed"),
        request(),
        True,
    )
    assert output.response == "α世界 [Source](https://example.com)!"
    assert output.metadata["google_interactions"]["steps"][0]["content"][0] == block


def test_grounded_stream_accepts_citations_before_text():
    accumulator = adapter.InteractionStream(None)
    citation = {"type": "url_citation", "url": "https://example.com", "end_index": 6}
    accumulator.accept(
        {
            "event_type": "step.start",
            "index": 0,
            "step": {"type": "model_output", "content": []},
        }
    )
    accumulator.accept(
        {
            "event_type": "step.delta",
            "index": 0,
            "delta": {"type": "text_annotation_delta", "annotations": [citation]},
        }
    )
    accumulator.accept(
        {
            "event_type": "step.delta",
            "index": 0,
            "delta": {"type": "text", "text": "Answer"},
        }
    )
    accumulator.accept({"event_type": "step.stop", "index": 0})
    accumulator.accept(
        {"event_type": "interaction.completed", "interaction": {"status": "completed"}}
    )
    result = accumulator.result()
    block = result["steps"][0]["content"][0]
    assert block == {"type": "text", "text": "Answer", "annotations": [citation]}
    assert adapter._citation_text(block) == "Answer [example.com](https://example.com)"


def test_generated_media_replays_from_the_existing_attachment_parts():
    data = base64.b64encode(b"test-image").decode()
    steps = [
        {
            "type": "model_output",
            "content": [
                {"type": "text", "text": "Image"},
                {"type": "image", "mime_type": "image/png", "data": data},
            ],
        }
    ]
    result, _, _ = adapter._normalize(
        client(), interaction(steps, "completed"), request(), False
    )
    replay = result.metadata["google_interactions"]
    assert "data" not in replay["steps"][0]["content"][1]
    history = [
        USER,
        {
            "type": "RESPONSE_MEDIA",
            "schemaVersion": 2,
            "io": "OUTPUT",
            "parts": result.parts,
            "google_interactions": replay,
        },
        USER,
    ]
    assert request(history)["input"][1] == steps[0]


@pytest.mark.parametrize(
    "status", ["failed", "cancelled", "requires_continuation", "in_progress", None]
)
def test_unsuccessful_status_is_not_an_empty_success(status):
    with pytest.raises(RuntimeError, match="status"):
        adapter._normalize(client(), interaction([], status), request(), True)


def test_incomplete_is_not_assumed_to_be_a_usage_ceiling_stop():
    result, _, _ = adapter._normalize(
        client(), interaction(STEPS[:2], "incomplete"), request(), True
    )
    assert result.usage_capped is None
    assert result.metadata["interaction_status"] == "incomplete"


def test_missing_usage_is_explicitly_estimated():
    result = interaction(STEPS[:2], "completed")
    result.pop("usage")
    response, _, _ = adapter._normalize(client(), result, request(), False)
    assert response.prompt_tokens > 0 and response.response_tokens > 0
    assert response.metadata["usage_estimated"] is True


def test_call_uses_v1_no_storage_or_continuation_and_does_not_count_per_delta(
    monkeypatch,
):
    obj = client()
    obj.google_client.interactions.create.return_value = interaction(
        STEPS[:2], "completed"
    )
    result = adapter.ask_interactions(obj, messages(stream=False))
    assert result.response == "Checking."
    sent = obj.google_client.interactions.create.call_args.kwargs
    assert (
        "api_version" not in sent
        and sent["store"] is False
        and sent["automatic_continuation"] is False
    )
    obj.google_client.models.count_tokens.assert_not_called()


def test_broken_stream_closes_without_retry_or_terminal_success(monkeypatch):
    obj = client()
    frames = []
    monkeypatch.setattr(
        adapter, "get_smss_stream", lambda: lambda data, **kw: frames.append((data, kw))
    )
    stream = Mock()
    stream.__iter__ = Mock(
        return_value=iter(
            [
                {
                    "event_type": "step.start",
                    "index": 0,
                    "step": {
                        "type": "model_output",
                        "content": [{"type": "text", "text": "Partial"}],
                    },
                }
            ]
        )
    )
    obj.google_client.interactions.create.return_value = stream
    response = adapter.ask_interactions(obj, messages())
    assert response.messageType == "ERROR"
    assert "before completion" in response.message
    assert response.numberOfTokensInResponse > 0
    stream.close.assert_called_once()
    obj.google_client.interactions.create.assert_called_once()
    assert any(kw["stream_type"] == "usage" for _, kw in frames)
    assert not any("finish_reason" in data for data, _ in frames)


def test_completed_stream_closes_without_waiting_for_network_eof(monkeypatch):
    obj = client()
    monkeypatch.setattr(adapter, "get_smss_stream", lambda: None)

    def events():
        yield {"event_type": "step.start", "index": 0, "step": STEPS[1]}
        yield {"event_type": "step.stop", "index": 0}
        yield {
            "event_type": "interaction.completed",
            "interaction": {"status": "completed", "usage": interaction()["usage"]},
        }
        raise AssertionError("Do not wait for EOF after terminal completion")

    stream = Mock()
    stream.__iter__ = Mock(return_value=events())
    obj.google_client.interactions.create.return_value = stream
    response = adapter.ask_interactions(obj, messages())
    assert response.messageType == "CHAT" and response.response == "Checking."
    stream.close.assert_called_once()


def test_invalid_replay_and_wrong_vertex_region_fail_before_inference():
    with pytest.raises(ValueError):
        validate_replay(
            {"version": 1, "model": MODEL, "steps": [{"type": "user_input"}]}
        )
    obj = client()
    obj.client_config = SimpleNamespace(api_key=None, region="us-central1")
    with pytest.raises(ValueError, match="location"):
        adapter.ask_interactions(obj, messages())
    obj.google_client.interactions.create.assert_not_called()


@pytest.mark.parametrize("vertex", [False, True])
@pytest.mark.parametrize("streaming", [False, True])
def test_pinned_sdk_serializes_real_http_requests_and_parses_unary_and_sse(
    vertex, streaming, monkeypatch
):
    from google.genai import Client
    from google.oauth2.credentials import Credentials

    captured = []
    final = dict(interaction(STEPS[:2], "completed"), id="interaction-test")

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            captured.append(
                (
                    self.path,
                    json.loads(self.rfile.read(int(self.headers["Content-Length"]))),
                )
            )
            if streaming:
                events = []
                for index, step in enumerate(final["steps"]):
                    events.extend(
                        [
                            {"event_type": "step.start", "index": index, "step": step},
                            {"event_type": "step.stop", "index": index},
                        ]
                    )
                events.append(
                    {
                        "event_type": "interaction.completed",
                        "interaction": {k: v for k, v in final.items() if k != "steps"},
                    }
                )
                body = "".join(
                    f"event: {e['event_type']}\ndata: {json.dumps(e)}\n\n"
                    for e in events
                ).encode()
            else:
                body = json.dumps(final).encode()
            self.send_response(200)
            self.send_header(
                "Content-Type", "text/event-stream" if streaming else "application/json"
            )
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = {
        "base_url": f"http://127.0.0.1:{server.server_port}",
        "api_version": "v1",
    }
    sdk = (
        Client(
            vertexai=True,
            project="test-project",
            location="global",
            credentials=Credentials(token="test-token"),
            http_options=config,
        )
        if vertex
        else Client(api_key="test-key", http_options=config)
    )
    monkeypatch.setattr(adapter, "get_smss_stream", lambda: None)
    try:
        obj = client()
        obj.google_client = sdk
        obj._interactions_client = sdk
        result = adapter.ask_interactions(obj, messages(stream=streaming))
        assert result.messageType == "CHAT", result
        assert result.response == "Checking."
        assert result.response_tokens == 30
        assert len(captured) == 1
        path, body = captured[0]
        assert "/v1/" in path
        if vertex:
            assert path.startswith(
                "/v1/projects/test-project/locations/global/interactions"
            )
        assert body["store"] is False
        assert body["input"] == [
            {"type": "user_input", "content": [{"type": "text", "text": "Hello"}]}
        ]
        assert "automatic_continuation" not in body
    finally:
        sdk.close()
        server.shutdown()
        server.server_close()
        thread.join()


def test_incomplete_error_retains_partial_output_and_usage():
    obj = client()
    obj.google_client.interactions.create.return_value = interaction(
        STEPS[:2], "incomplete"
    )
    response = adapter.ask_interactions(obj, messages(stream=False))
    assert response.messageType == "ERROR"
    assert response.numberOfTokensInResponse == 30
    assert response.parts == [{"type": "TEXT", "text": "Checking."}]
    assert "google_interactions" not in response.metadata


def test_invalid_provider_function_call_retains_usage_without_tool_handoff():
    obj = client()
    malformed = deepcopy(STEPS)
    malformed[-1]["arguments"] = "invalid"
    obj.google_client.interactions.create.return_value = interaction(malformed)
    response = adapter.ask_interactions(obj, messages(stream=False))
    assert response.messageType == "ERROR"
    assert response.numberOfTokensInResponse == 30
    assert all(p["type"] == "TEXT" for p in response.parts)
    assert "google_interactions" not in response.metadata


def test_interactions_http_error_keeps_status_without_json_body():
    from google.genai._interactions import NotFoundError
    import httpx2

    error = NotFoundError(
        "Not found",
        body="upstream HTML",
        response=httpx2.Response(
            404, request=httpx2.Request("POST", "https://example.com")
        ),
    )
    parsed = adapter.ModelEngineException(error, "google", MODEL).parse_error()
    assert parsed.code == 404


@pytest.mark.parametrize("vertex", [False, True])
def test_google_client_preserves_verified_tls_and_separate_api_versions(
    vertex, monkeypatch
):
    import ssl
    from genai_client.clients import google_clients

    sdk = Mock()
    monkeypatch.setattr(google_clients, "GoogleGenAIClient", sdk)
    monkeypatch.setattr(
        google_clients.GoogleClient, "_load_credentials", lambda *a: "credentials"
    )
    config = google_clients.GoogleClientConfig(
        type="google",
        **(
            {
                "project": "project",
                "region": "global",
                "service_account_credentials": {"test": True},
            }
            if vertex
            else {"api_key": "test-key"}
        ),
    )
    google_clients.GoogleClient(config)
    legacy = sdk.call_args.kwargs["http_options"]
    assert legacy.api_version == ("v1beta1" if vertex else "v1beta")
    google_clients.GoogleClient(config, api_version="v1")
    current = sdk.call_args.kwargs["http_options"]
    assert current.api_version == "v1"
    context = current.client_args["verify"]
    assert context.verify_mode == ssl.CERT_REQUIRED and context.check_hostname
    assert current.async_client_args["verify"] is context


@pytest.mark.parametrize(
    "mode,history,native",
    [
        ("generate_content", [USER], False),
        ("interactions", [USER], True),
        (
            "interactions",
            [
                USER,
                {"type": "RESPONSE_TEXT", "io": "OUTPUT", "content": "Legacy"},
                USER,
            ],
            False,
        ),
        (
            "generate_content",
            [
                USER,
                {
                    "type": "RESPONSE_TEXT",
                    "io": "OUTPUT",
                    "content": "Native",
                    "google_interactions": {
                        "version": 1,
                        "model": MODEL,
                        "steps": STEPS[:2],
                    },
                },
                USER,
            ],
            True,
        ),
    ],
)
def test_conversation_protocol_survives_enable_and_rollback(
    mode, history, native, monkeypatch
):
    obj = GoogleGenAiTextClient.__new__(GoogleGenAiTextClient)
    obj.model_settings, obj.model_name = SETTINGS, MODEL
    obj.google_client = Mock()
    obj.generation_api = mode
    obj.build_semoss_messages = Mock(return_value=messages(history))
    obj._apply_usage_ceiling = Mock()
    obj.generate_with_retry = Mock(return_value="legacy")
    target = Mock(return_value="native")
    monkeypatch.setattr(adapter, "ask_interactions", target)
    assert obj.ask_call() == ("native" if native else "legacy")
    assert target.called is native
    assert obj.generate_with_retry.called is not native


def test_usage_ceiling_counts_once_and_applies_lower_output_limit():
    from genai_client.text_generation.abstract_text_generation_client import (
        AbstractTextGenerationClient,
    )
    from genai_client.usage_ceiling import UsageCeiling, usage_ceiling_scope

    obj = client()
    obj.fit_output_to_usage_ceiling = (
        AbstractTextGenerationClient.fit_output_to_usage_ceiling.__get__(obj)
    )
    obj.google_client.models.count_tokens.return_value.total_tokens = 100
    obj.google_client.interactions.create.return_value = interaction(
        STEPS[:2], "completed"
    )
    with usage_ceiling_scope(UsageCeiling(tokens=600)):
        result = adapter.ask_interactions(obj, messages(stream=False))
    sent = obj.google_client.interactions.create.call_args.kwargs
    assert 256 <= sent["generation_config"]["max_output_tokens"] < SETTINGS.max_tokens
    obj.google_client.models.count_tokens.assert_called_once()
    assert result.response_tokens == 30


def test_builtin_steps_are_retained_without_becoming_client_tool_calls():
    steps = [
        {
            "type": "google_search_call",
            "id": "search",
            "arguments": {"queries": ["weather"]},
        },
        {
            "type": "google_search_result",
            "call_id": "search",
            "result": [{"url": "https://example.com"}],
        },
        *STEPS[:2],
    ]
    response, _, _ = adapter._normalize(
        client(), interaction(steps, "completed"), request(), True
    )
    assert response.messageType == "CHAT"
    assert not any(p["type"] == "TOOL_CALL" for p in response.parts)
    assert response.metadata["google_interactions"]["steps"] == steps
