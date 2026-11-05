import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from genai_client.text_generation.anthropic_client.anthropic_text_client import (
    AnthropicTextClient,
)

SAMPLING = {"temperature": 0.2, "top_p": 0.5, "top_k": 5}


def anthropic_client(monkeypatch, provider):
    monkeypatch.setattr(AnthropicTextClient, "_get_client", Mock())
    return AnthropicTextClient(provider=provider, model_name="claude-sonnet-4-5")


@pytest.mark.parametrize(
    "provider, extra_body",
    [
        ("anthropic", None),
        # Bedrock serves API version 2026-10-21 only for Claude 4.7 and later
        ("bedrock", {"anthropic_version": "bedrock-2023-05-31"}),
    ],
)
def test_ask_sends_no_sampling_params(monkeypatch, provider, extra_body):
    client = anthropic_client(monkeypatch, provider)
    create = client.client.messages.create
    create.return_value = SimpleNamespace(
        stop_reason="end_turn",
        content=[SimpleNamespace(type="text", text="ok")],
        usage=SimpleNamespace(input_tokens=1, output_tokens=1),
    )
    message_json = json.dumps(
        [
            {
                "messageId": "input-1",
                "type": "INPUT_TEXT",
                "io": "INPUT",
                "schemaVersion": 2,
                "parts": [{"type": "TEXT", "text": "hi"}],
            }
        ]
    )
    response = client.ask_call(message_json=message_json, stream=False, **SAMPLING)
    assert create.called, response
    sent = create.call_args.kwargs
    assert not SAMPLING.keys() & sent.keys()
    assert sent.get("extra_body") == extra_body


def test_batch_sends_no_sampling_params(monkeypatch):
    client = anthropic_client(monkeypatch, "anthropic")
    create = client.client.messages.batches.create
    create.return_value = SimpleNamespace(
        id="msgbatch_1",
        processing_status="in_progress",
        results_url=None,
        model_dump=dict,
    )
    client.submit_batch([{"custom_id": "r1", "command": "hi", **SAMPLING}])
    params = create.call_args.kwargs["requests"][0]["params"]
    assert not SAMPLING.keys() & params.keys()
