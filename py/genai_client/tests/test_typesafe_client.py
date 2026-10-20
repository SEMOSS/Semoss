"""Exercise the real SDK and HTTP serialization without a TypeSafe account."""

import json
from unittest.mock import patch

import httpx2
import pytest
from pydantic import ValidationError
from typesafe_sdk import (
    Choice,
    Noul,
    Score,
    TypeSafeAPIResponseValidationError,
    TypeSafeClient,
    TypeSafeError,
    TypeSafeRateLimitError,
)

from genai_client.typesafe import TypeSafeClientWrapper


@pytest.fixture
def response_body():
    return {
        "model": "jev-1.13.0",
        "usage": {"input_tokens": 425, "output_tokens": 73},
        "answers": {
            "department": {
                "type": "choice",
                "choice": "technical",
                "confidence": 0.71,
                "probabilities": {"billing": 0.19, "technical": 0.81},
            },
            "frustration": {
                "type": "score",
                "score": 0.75,
                "confidence": 0.8,
                "legend": {"0": "calm", "1": {"description": "angry"}},
                "probabilities": {"0": 0.25, "1": 0.75},
            },
            "is_urgent": {"type": "noul", "noul": 0.99},
        },
    }


@pytest.fixture
def questions():
    return {
        "department": {
            "type": "choice",
            "instructions": {"task": "Route this ticket"},
            "criteria": {"billing": None, "technical": ["bugs", "support"]},
        },
        "frustration": {"type": "score", "criteria": ["calm", {"description": "angry"}]},
        "is_urgent": {
            "type": "noul",
            "instructions": "Is this urgent?",
            "criteria": {"true": "Requires immediate action", "false": None},
        },
    }


@pytest.fixture
def make_client():
    clients = []

    def build(handler):
        sdk = TypeSafeClient(
            api_key="test-key",
            model="jev-latest",
            transport=httpx2.MockTransport(handler),
        )
        with patch.object(TypeSafeClientWrapper, "_get_client", return_value=sdk):
            wrapper = TypeSafeClientWrapper(api_key="test-key")
        clients.append(wrapper)
        return wrapper

    yield build
    for client in clients:
        client.close()


@pytest.mark.parametrize("state", [
    "Quotes: \" ' ''' \\\n\t café 🚀",
    {"ticket": "charged twice", "active": False, "count": 0, "context": None},
    [{"role": "customer", "text": "help"}, None, True, 3],
])
def test_round_trip_preserves_state_questions_and_every_answer(
    make_client, response_body, questions, state
):
    requests = []

    def handle(request):
        requests.append(json.loads(request.content))
        assert request.url.path == "/v1/systemone"
        assert request.headers["authorization"] == "Bearer test-key"
        return httpx2.Response(200, json=response_body)

    result = make_client(handle).ask(state, questions)
    assert requests == [{"model": "jev-latest", "state": state, "questions": questions}]
    assert result == response_body
    assert json.loads(json.dumps(result)) == result
    assert result["answers"]["is_urgent"]["noul"] == 0.99
    assert result["answers"]["frustration"]["probabilities"]["1"] == 0.75


def test_accepts_sdk_objects_and_preserves_future_fields(make_client, response_body, questions):
    response_body["answers"]["department"]["new_metric"] = 0.5
    response_body["answers"]["future"] = {"type": "future_kind", "value": [1, 2]}
    response_body["new_metadata"] = {"value": "kept"}
    client = make_client(lambda request: httpx2.Response(200, json=response_body))
    typed = {
        "department": Choice(**questions["department"]),
        "frustration": Score(**questions["frustration"]),
        "is_urgent": Noul(**questions["is_urgent"]),
    }
    assert client.ask("ticket", typed) == response_body


@pytest.mark.parametrize("questions", [
    {}, [], {"": {"type": "noul"}}, {1: {"type": "noul"}},
    {"q": "raw text"}, {"q": {"type": "unknown"}}, {"q": {"type": []}},
    {"q": {"type": "choice", "criteria": {}}},
    {"q": {"type": "score", "criteria": []}},
    {"q": {"type": "score", "criteria": {"0": "calm", "1": "angry"}}},
    {"q": {"type": "noul", "instruction": "misspelled field"}},
])
def test_invalid_questions_fail_before_http(make_client, questions):
    def unexpected(request):
        pytest.fail("Invalid questions reached the provider")

    with pytest.raises((ValueError, ValidationError)):
        make_client(unexpected).ask("ticket", questions)


@pytest.mark.parametrize("state", [None, True, 1, 1.5, object()])
def test_invalid_state_fails_before_http(make_client, questions, state):
    with pytest.raises(ValidationError):
        make_client(lambda request: pytest.fail("Invalid state reached HTTP")).ask(state, questions)


@pytest.mark.parametrize("parameters", [
    {"model": "another-model"}, {"temperature": 0.5}, {"max_retries": -1},
    {"max_retries": True}, {"timeout": 0}, {"timeout": float("nan")},
    {"extra_body": {"questions": {}}},
])
def test_invalid_options_fail_before_http(make_client, questions, parameters):
    with pytest.raises(ValidationError):
        make_client(lambda request: pytest.fail("Invalid options reached HTTP")).ask(
            "ticket", questions, parameters
        )


def test_timeout_and_zero_retries_propagate_provider_error(make_client, questions):
    calls = []

    def handle(request):
        calls.append(request)
        assert request.extensions["timeout"]["read"] == 1.5
        return httpx2.Response(429, json={"error": "rate limited"})

    with pytest.raises(TypeSafeRateLimitError):
        make_client(handle).ask("ticket", questions, {"timeout": 1.5, "max_retries": 0})
    assert len(calls) == 1


def test_invalid_known_answer_is_not_silently_returned(make_client, questions, response_body):
    response_body["answers"]["is_urgent"]["noul"] = "not a probability"
    client = make_client(lambda request: httpx2.Response(200, json=response_body))
    with pytest.raises(TypeSafeAPIResponseValidationError):
        client.ask("ticket", questions)


def test_null_usage_is_preserved(make_client, questions, response_body):
    response_body["usage"] = {"input_tokens": None, "output_tokens": None}
    client = make_client(lambda request: httpx2.Response(200, json=response_body))
    assert client.ask("ticket", questions)["usage"] == response_body["usage"]


def test_client_configuration_and_context_manager_close():
    with patch("genai_client.typesafe.typesafe_client.TypeSafeClient") as sdk:
        with TypeSafeClientWrapper(
            api_key="test-key", model="jev-1.13.0", base_url="https://example.test",
            timeout=5, max_retries=0,
        ):
            options = sdk.call_args.kwargs
            assert options["base_url"] == "https://example.test"
            assert options["model"] == "jev-1.13.0"
            assert options["http_client"].timeout.read == 5
            assert options["retry"].max_retries == 0
        sdk.return_value.close.assert_called_once()
        options["http_client"].close()


def test_failed_initialization_closes_http_client():
    with patch("genai_client.typesafe.typesafe_client.httpx2.Client") as http:
        with pytest.raises(TypeSafeError):
            TypeSafeClientWrapper(api_key="")
        http.return_value.close.assert_called_once()
