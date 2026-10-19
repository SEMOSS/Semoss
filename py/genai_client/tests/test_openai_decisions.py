"""Decisions contracts through the public client and real SDK, without network I/O."""

import json

import httpx2
import pytest
from openai import APIStatusError, BadRequestError

from genai_client import OpenAiClient


QUESTIONS = [
    {"type": "predicate", "instructions": "Does the message mention billing?"},
    {
        "type": "choice",
        "name": "follow_up",
        "instructions": "Does the customer need a follow-up?",
        "choices": [{"value": True}, {"value": "true"}, {"value": False}],
    },
    {
        "type": "score",
        "instructions": "How urgent is this message?",
        "levels": [{"label": "low"}, {"label": "high"}],
    },
    {
        "type": "predicate",
        "name": "sensitive",
        "instructions": "Does the input reveal sensitive personal information?",
    },
]

DECISION = {
    "model": "fixture-decision-model",
    "answers": [
        {
            "type": "predicate",
            "name": None,
            "probability": 0.95,
            "provider_detail": "preserved",
        },
        {
            "type": "choice",
            "name": "follow_up",
            "choice": True,
            "confidence": 0.8,
            "probabilities": [
                {"value": True, "probability": 0.8},
                {"value": "true", "probability": 0.1},
                {"value": False, "probability": 0.1},
            ],
        },
        {
            "type": "score",
            "score": 0.7,
            "confidence": 0.7,
            "probabilities": [
                {"label": "low", "value": 0, "probability": 0.3},
                {"label": "high", "value": 1, "probability": 0.7},
            ],
        },
        {"type": "refusal", "name": "sensitive"},
    ],
    "usage": {
        "input_tokens": 25,
        "input_tokens_details": {"cached_tokens": 5, "cache_write_tokens": 0},
        "output_tokens": 0,
        "output_tokens_details": {"reasoning_tokens": 0},
        "total_tokens": 25,
    },
    "provider_request_id": "decision-fixture",
}


@pytest.fixture
def sdk():
    requests, clients = [], []

    def create(payload=DECISION, status_code=200, **settings):
        def handle(request):
            requests.append((request, json.loads(request.content)))
            return httpx2.Response(status_code, json=payload)

        transport = httpx2.Client(transport=httpx2.MockTransport(handle))
        clients.append(transport)
        return OpenAiClient(
            model_name="fixture-decision-model",
            api_key="fixture-key",
            endpoint="https://decisions.invalid/custom/v1",
            http_client=transport,
            max_retries=0,
            **settings,
        )

    yield create, requests
    for client in clients:
        client.close()


def test_decisions_preserves_native_answers_and_uses_configured_model(sdk):
    create, requests = sdk
    client = create(
        chat_type="responses",
        max_tokens=64,
        global_param_override={"temperature": 0.5, "stream": True},
    )

    result = client.decisions(input="Please fix my invoice.", questions=QUESTIONS)

    assert result == DECISION
    assert result["answers"][1]["choice"] is True
    assert result["answers"][1]["probabilities"][0]["value"] is True
    assert result["answers"][1]["probabilities"][1]["value"] == "true"
    assert "name" not in result["answers"][2]
    request, body = requests[0]
    assert request.method == "POST"
    assert str(request.url) == "https://decisions.invalid/custom/v1/decisions"
    assert request.headers["authorization"] == "Bearer fixture-key"
    assert body == {
        "model": "fixture-decision-model",
        "input": "Please fix my invoice.",
        "questions": QUESTIONS,
    }


def test_decisions_passes_multimodal_input_and_sdk_options(sdk):
    create, requests = sdk
    input_messages = [
        {
            "role": "user",
            "content": [
                {"type": "input_text", "text": "Inspect this invoice."},
                {
                    "type": "input_image",
                    "image_url": "https://images.invalid/invoice.png",
                    "detail": "low",
                },
            ],
        }
    ]
    options = {
        "prompt_cache_key": "invoice-checks",
        "prompt_cache_options": {"mode": "explicit", "ttl": "30m"},
        "safety_identifier": "fixture-user",
    }

    result = create().decisions(
        input=input_messages,
        questions=QUESTIONS,
        **options,
        extra_headers={"X-Decision-Trace": "fixture"},
        extra_body={"provider_extension": "enabled"},
        timeout=12.5,
    )

    assert result == DECISION
    request, body = requests[0]
    assert body == {
        "model": "fixture-decision-model",
        "input": input_messages,
        "questions": QUESTIONS,
        **options,
        "provider_extension": "enabled",
    }
    assert request.headers["X-Decision-Trace"] == "fixture"
    assert request.extensions["timeout"]["read"] == 12.5


def test_decisions_propagates_sdk_api_errors(sdk):
    create, requests = sdk
    error = {
        "message": "Unsupported decision question.",
        "type": "invalid_request_error",
        "param": "questions",
        "code": "invalid_value",
    }
    client = create(payload={"error": error}, status_code=400)

    with pytest.raises(BadRequestError) as caught:
        client.decisions(input="Check this.", questions=QUESTIONS)

    assert caught.value.status_code == 400
    assert caught.value.body == error
    assert len(requests) == 1


@pytest.mark.parametrize("status_code", [429, 500])
def test_decisions_overrides_retries_for_one_call_only(status_code):
    requests = []

    def handle(request):
        requests.append(json.loads(request.content))
        if len(requests) == 3:
            return httpx2.Response(200, json=DECISION)
        return httpx2.Response(
            status_code,
            json={"error": {"message": "Try again", "type": "server_error"}},
            headers={"Retry-After": "0.001"},
        )

    with httpx2.Client(transport=httpx2.MockTransport(handle)) as transport:
        client = OpenAiClient(
            model_name="fixture-decision-model",
            api_key="fixture-key",
            endpoint="https://decisions.invalid/custom/v1",
            http_client=transport,
            max_retries=0,
        )

        assert (
            client.decisions(input="ticket", questions=QUESTIONS, max_retries=2)
            == DECISION
        )
        assert len(requests) == 3
        assert client.openai_class.client.max_retries == 0

        with pytest.raises(APIStatusError) as caught:
            client.decisions(input="ticket", questions=QUESTIONS)

    assert caught.value.status_code == status_code
    assert len(requests) == 4
    assert all(
        body
        == {
            "model": "fixture-decision-model",
            "input": "ticket",
            "questions": QUESTIONS,
        }
        for body in requests
    )


@pytest.mark.parametrize("max_retries", [-1, 1.5, "2", True, False])
def test_decisions_rejects_invalid_retry_overrides(sdk, max_retries):
    create, requests = sdk

    with pytest.raises(ValueError, match="max_retries"):
        create().decisions(
            input="ticket", questions=QUESTIONS, max_retries=max_retries
        )

    assert requests == []


def test_decisions_consumes_usage_ceiling_without_reserving_output(sdk, monkeypatch):
    create, requests = sdk
    client = create()
    monkeypatch.setattr(client.openai_class, "_usage_text_counter", lambda: (None, False))
    questions = [{"type": "predicate", "instructions": "Urgent?"}]

    result = client.decisions(
        input="ticket",
        questions=questions,
        usage_ceiling={"tokens": 100, "min_output_tokens": 256},
    )

    assert result == DECISION
    assert requests[0][1] == {
        "model": "fixture-decision-model",
        "input": "ticket",
        "questions": questions,
    }


@pytest.mark.parametrize(
    "usage_ceiling",
    [{"tokens": 1000}, {"cost_micros": 1000, "rates": {"input": 2.0}}],
)
@pytest.mark.parametrize(
    "input",
    [
        "x" * 6000,
        [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_image",
                        "image_url": "https://images.invalid/invoice.png",
                    }
                ],
            }
        ],
    ],
)
def test_decisions_refuses_input_exceeding_token_or_cost_budget(
    sdk, monkeypatch, usage_ceiling, input
):
    create, requests = sdk
    client = create()
    monkeypatch.setattr(client.openai_class, "_usage_text_counter", lambda: (None, False))

    result = client.decisions(
        input=input, questions=QUESTIONS, usage_ceiling=usage_ceiling
    )

    assert result["messageType"] == "ERROR"
    assert result["reason"] == "USAGE_LIMIT_DOES_NOT_FIT"
    assert requests == []


@pytest.mark.parametrize("large_part", ["questions", "extra_body"])
def test_decisions_includes_questions_and_extra_body_in_usage_budget(
    sdk, monkeypatch, large_part
):
    create, requests = sdk
    client = create()
    monkeypatch.setattr(client.openai_class, "_usage_text_counter", lambda: (None, False))
    questions = [{"type": "predicate", "instructions": "Urgent?"}]
    options = {}
    if large_part == "questions":
        questions[0]["instructions"] = "x" * 6000
    else:
        options["extra_body"] = {"provider_context": "x" * 6000}

    result = client.decisions(
        input="ticket", questions=questions, usage_ceiling={"tokens": 1000}, **options
    )

    assert result["reason"] == "USAGE_LIMIT_DOES_NOT_FIT"
    assert requests == []
