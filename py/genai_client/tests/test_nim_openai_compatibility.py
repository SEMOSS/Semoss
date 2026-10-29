"""NIM opt-ins and default OpenAI contracts through the real SDK, without network I/O."""

import json

import httpx
import pytest
from pydantic import ValidationError

from genai_client.message_builders.openai.openai_message_builder import (
    OpenAIMessageBuilder,
)
from genai_client.message_builders.semoss_base.semoss_message_builder import (
    SEMOSSMessageBuilder,
)
from genai_client.message_builders.semoss_base.semoss_models import ModelSettings
from genai_client.text_generation.openai_clients.openai_client import OpenAiClient
from smss_thread_local import clear_smss_stream, set_smss_stream


TOOL_CALL = {
    "id": "call-1",
    "type": "function",
    "function": {"name": "add", "arguments": '{"a":2,"b":3}'},
}
TOOL = {
    "name": "add",
    "description": "Add two integers.",
    "inputSchema": {
        "type": "object",
        "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
        "required": ["a", "b"],
    },
}
REASONING = {
    "type": "THINKING",
    "thinking": "Use addition.",
    "source": "reasoning_content",
}


def message(parts, *, output=False, tool=False):
    return {
        "type": ("RESPONSE_TOOL" if tool else "RESPONSE_TEXT")
        if output
        else "INPUT_TEXT",
        "io": "OUTPUT" if output else "INPUT",
        "schemaVersion": 2,
        "parts": parts,
    }


def user(text="What is 2 + 3?"):
    return message([{"type": "TEXT", "text": text}])


def completion(*, tool=False, content="5", reasoning_field="reasoning_content"):
    return {
        "id": "chatcmpl-fixture",
        "object": "chat.completion",
        "created": 1,
        "model": "fixture",
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": content,
                    reasoning_field: "Use addition.",
                    "tool_calls": [TOOL_CALL] if tool else None,
                },
                "finish_reason": "tool_calls" if tool else "stop",
            }
        ],
        "usage": {"prompt_tokens": 17, "completion_tokens": 6, "total_tokens": 23},
    }


def streamed_response(payload):
    msg = payload["choices"][0]["message"]
    reasoning_field = next(k for k in ("reasoning_content", "reasoning") if k in msg)
    deltas = [
        {"role": "assistant"},
        {reasoning_field: "Use "},
        {reasoning_field: "addition."},
    ]
    if msg["content"]:
        deltas.append({"content": msg["content"]})
    if msg["tool_calls"]:
        deltas.extend(
            [
                {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "call-1",
                            "type": "function",
                            "function": {"name": "add", "arguments": '{"a":2,'},
                        }
                    ]
                },
                {"tool_calls": [{"index": 0, "function": {"arguments": '"b":3}'}}]},
            ]
        )
    chunks = [
        {
            "id": payload["id"],
            "object": "chat.completion.chunk",
            "created": 1,
            "model": "fixture",
            "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
        }
        for delta in deltas
    ]
    chunks.append(
        {
            **chunks[0],
            "choices": [
                {
                    "index": 0,
                    "delta": {},
                    "finish_reason": payload["choices"][0]["finish_reason"],
                }
            ],
        }
    )
    chunks.append({**chunks[0], "choices": [], "usage": payload["usage"]})
    body = (
        "".join(f"data: {json.dumps(chunk)}\n\n" for chunk in chunks)
        + "data: [DONE]\n\n"
    )
    return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=body)


@pytest.fixture
def sdk():
    requests, events, clients = [], [], []
    set_smss_stream(lambda data, **kwargs: events.append({"data": data, **kwargs}))

    def create(payload=None, **settings):
        def handle(request):
            body = json.loads(request.content)
            requests.append((request, body))
            response = payload if payload is not None else completion()
            return (
                streamed_response(response)
                if body["stream"]
                else httpx.Response(200, json=response)
            )

        transport = httpx.Client(transport=httpx.MockTransport(handle))
        clients.append(transport)
        return OpenAiClient(
            is_azure=False,
            model_name="fixture",
            api_key="fixture",
            endpoint="https://nim.invalid/v1",
            http_client=transport,
            max_retries=0,
            **settings,
        )

    yield create, requests, events
    for client in clients:
        client.close()
    clear_smss_stream()


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("tool", [False, True])
@pytest.mark.parametrize("reasoning_field", ["reasoning_content", "reasoning"])
def test_nim_reasoning_survives_sdk_and_history(sdk, stream, tool, reasoning_field):
    create, requests, events = sdk
    client = create(
        completion(
            tool=tool,
            content="Checking." if tool else "5",
            reasoning_field=reasoning_field,
        ),
        preserve_reasoning_content=True,
    )
    params = {"stream": stream, "max_tokens": 64}
    if tool:
        params.update(tools=[TOOL], tool_choice="required")
    result = client.ask(message_json=json.dumps([user()]), **params)
    assert result["parts"][0] == {**REASONING, "source": reasoning_field}
    assert result["numberOfTokensInResponse"] == 6
    assert result["numberOfTokensInPrompt"] == 17
    assert str(requests[0][0].url) == "https://nim.invalid/v1/chat/completions"
    assert requests[0][1]["max_completion_tokens"] == 64
    assert any(event["stream_type"] == "thinking" for event in events) is stream

    history = [user(), message(result["parts"], output=True, tool=tool)]
    if tool:
        assert result["response"][0]["arguments"] == {"a": 2, "b": 3}
        history.append(
            message(
                [
                    {
                        "type": "TOOL_RESULT",
                        "toolResult": {"toolCallId": "call-1", "output": "5"},
                    }
                ]
            )
        )
    else:
        history.append(user("Explain."))
    client.ask(message_json=json.dumps(history), stream=False)
    assistant = requests[-1][1]["messages"][1]
    assert assistant[reasoning_field] == "Use addition."
    assert ({"reasoning_content", "reasoning"} & assistant.keys()) == {reasoning_field}
    assert assistant["content"] == [
        {"type": "text", "text": "Checking." if tool else "5"}
    ]
    if tool:
        call = assistant["tool_calls"][0]
        assert call["id"] == "call-1"
        assert call["function"]["name"] == "add"
        assert json.loads(call["function"]["arguments"]) == {"a": 2, "b": 3}
        assert requests[-1][1]["messages"][-1] == {
            "role": "tool",
            "content": "5",
            "tool_call_id": "call-1",
        }


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("reasoning_field", ["reasoning_content", "reasoning"])
def test_reasoning_only_cutoff_still_returns_a_chat_string(
    sdk, stream, reasoning_field
):
    create, _, _ = sdk
    client = create(
        completion(content=None, reasoning_field=reasoning_field),
        preserve_reasoning_content=True,
    )
    result = client.ask(message_json=json.dumps([user()]), stream=stream)
    assert result["response"] == ""
    assert result["parts"] == [{**REASONING, "source": reasoning_field}]


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("tool", [False, True])
def test_thinking_left_in_the_text_is_not_the_answer(sdk, stream, tool):
    # a server without a reasoning parser sends the thinking as text, closed by </think>
    create, _, _ = sdk
    answer = "Checking." if tool else "5"
    client = create(
        completion(tool=tool, content="Add 2 and 3. Wait, recheck.</think>\n\n" + answer)
    )
    params = {"stream": stream, "max_tokens": 64}
    if tool:
        params.update(tools=[TOOL], tool_choice="required")
    result = client.ask(message_json=json.dumps([user()]), **params)
    assert [part for part in result["parts"] if part["type"] == "TEXT"] == [
        {"type": "TEXT", "text": answer}
    ]
    if not tool:
        assert result["response"] == answer


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("reasoning_field", ["reasoning_content", "reasoning"])
def test_default_openai_ignores_provider_reasoning_and_preserves_tool_contract(
    sdk, stream, reasoning_field
):
    create, requests, events = sdk
    client = create(
        completion(tool=True, content="Checking.", reasoning_field=reasoning_field)
    )
    history = [
        user(),
        message(
            [
                {**REASONING, "source": reasoning_field},
                {"type": "TEXT", "text": "Earlier answer."},
            ],
            output=True,
        ),
        user(),
    ]
    result = client.ask(
        message_json=json.dumps(history),
        stream=stream,
        tools=[TOOL],
        tool_choice="required",
    )
    assert all(part["type"] != "THINKING" for part in result["parts"])
    assert not any(event["stream_type"] == "thinking" for event in events)
    assert (
        not {"reasoning_content", "reasoning"} & requests[-1][1]["messages"][1].keys()
    )
    assert "chat_template_kwargs" not in requests[-1][1]
    assert requests[-1][1]["tools"][0]["function"]["name"] == "add"
    history = [user(), message(result["parts"], output=True, tool=True), user()]
    client.ask(message_json=json.dumps(history), stream=False)
    assert requests[-1][1]["messages"][1]["content"] == ""


@pytest.mark.parametrize(
    "thinking,enabled", [(True, True), (False, False), ("false", False)]
)
def test_template_reasoning_switch_reaches_sdk_without_openai_effort(
    sdk, thinking, enabled
):
    create, requests, _ = sdk
    client = create(
        preserve_reasoning_content=True, chat_reasoning_mode="chat_template"
    )
    result = client.ask(
        message_json=json.dumps([user()]),
        stream=False,
        thinking=thinking,
        effort="high",
        thinking_budget=1024,
        reasoning_effort="high",
        temperature=0.6,
        extra_body={"top_k": 20, "chat_template_kwargs": {"keep": "value"}},
    )
    assert result["response"] == "5"
    body = requests[-1][1]
    assert body["chat_template_kwargs"] == {"keep": "value", "enable_thinking": enabled}
    assert body["top_k"] == 20
    assert body["temperature"] == 0.6
    assert (
        not {
            "thinking",
            "thinking_budget",
            "effort",
            "reasoning_effort",
            "preserve_reasoning_content",
            "chat_reasoning_mode",
        }
        & body.keys()
    )


def build_request(chat_type="chat-completion", history=None, settings=None, **params):
    settings = ModelSettings(model_name="fixture", **(settings or {}))
    messages = SEMOSSMessageBuilder().build_messages(
        history or [user()], params, settings
    )
    return OpenAIMessageBuilder(settings, chat_type).build_request(messages)


def test_no_reasoning_control_does_not_send_unsupported_parameters():
    request = build_request(
        settings={"chat_reasoning_mode": "none"},
        thinking=True,
        effort="high",
        reasoning_effort="high",
        temperature=0.6,
    )
    assert request["temperature"] == 0.6
    assert not {"thinking", "effort", "reasoning_effort", "extra_body"} & request.keys()


def test_default_openai_reasoning_effort_and_temperature_behavior_is_unchanged():
    request = build_request(thinking=True, effort="high", temperature=0.6, stream=False)
    assert request["reasoning_effort"] == "high"
    assert "temperature" not in request
    assert "extra_body" not in request
    assert request["stream"] is False


@pytest.mark.parametrize("chat_type", ["chat-completion", "responses"])
@pytest.mark.parametrize("preserve", [False, True])
def test_summaries_are_never_replayed_as_raw_reasoning(chat_type, preserve):
    summary = {"type": "THINKING", "thinking": "A provider summary, not raw reasoning."}
    history = [
        user(),
        message([summary, {"type": "TEXT", "text": "5"}], output=True),
        user(),
    ]
    request = build_request(
        chat_type, history, {"preserve_reasoning_content": preserve}
    )
    assert "provider summary" not in json.dumps(request)
    assert "reasoning_content" not in json.dumps(request)


def test_responses_never_replays_chat_reasoning_or_template_parameters():
    history = [
        user(),
        message([REASONING, {"type": "TEXT", "text": "5"}], output=True),
        user(),
    ]
    request = build_request(
        "responses",
        history,
        {"preserve_reasoning_content": True, "chat_reasoning_mode": "chat_template"},
        thinking=True,
        effort="high",
    )
    assert request["reasoning"]["effort"] == "high"
    assert "reasoning_content" not in json.dumps(request)
    assert "chat_template_kwargs" not in json.dumps(request)


@pytest.mark.parametrize(
    "chat_type,format_key",
    [("chat-completion", "response_format"), ("responses", "text")],
)
def test_existing_structured_output_keeps_its_api_specific_format(
    chat_type, format_key
):
    schema = {
        "type": "object",
        "properties": {"answer": {"type": "integer"}},
        "required": ["answer"],
    }
    request = build_request(chat_type, schema=schema, stream=False)
    assert format_key in request
    assert "json_schema" in json.dumps(request[format_key])
    assert "chat_template_kwargs" not in json.dumps(request)


def test_invalid_reasoning_mode_is_rejected_before_client_creation(sdk):
    create, requests, _ = sdk
    with pytest.raises(ValidationError, match="chat_reasoning_mode"):
        create(chat_reasoning_mode="typo")
    assert requests == []


def test_default_openai_token_summary_stays_display_only(sdk):
    create, requests, _ = sdk
    payload = completion()
    payload["usage"]["completion_tokens_details"] = {"reasoning_tokens": 3}
    client = create(payload)
    result = client.ask(message_json=json.dumps([user()]), stream=False)
    thinking = result["parts"][0]
    assert thinking["type"] == "THINKING"
    assert "3 tokens" in thinking["thinking"]
    assert "source" not in thinking
    history = [user(), message(result["parts"], output=True), user()]
    client.ask(message_json=json.dumps(history), stream=False)
    assert "3 tokens" not in json.dumps(requests[-1][1])
