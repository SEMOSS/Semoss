"""
To run the tests, navigate to the Semoss_Dev/py directory and then run the following command:

    pytest testing/clients/anthropic/anthropic_guardrail_test.py

No credentials are required — these tests only cover the Bedrock guardrail
header construction on the AnthropicTextClient.
"""

import pytest
from genai_client.text_generation.anthropic_client.anthropic_text_client import (
    AnthropicTextClient,
)

GUARDRAIL_ID = "arn:aws:bedrock:us-east-1:123456789012:guardrail/abc123"
GUARDRAIL_VERSION = "DRAFT"


def test_no_guardrail_returns_none():
    assert (
        AnthropicTextClient._get_bedrock_guardrail_headers(None, None) is None
    )
    assert AnthropicTextClient._get_bedrock_guardrail_headers("", "") is None


def test_guardrail_headers_built_with_trace_by_default():
    headers = AnthropicTextClient._get_bedrock_guardrail_headers(
        GUARDRAIL_ID, GUARDRAIL_VERSION
    )
    assert headers == {
        "X-Amzn-Bedrock-GuardrailIdentifier": GUARDRAIL_ID,
        "X-Amzn-Bedrock-GuardrailVersion": GUARDRAIL_VERSION,
        "X-Amzn-Bedrock-Trace": "ENABLED",
    }


def test_guardrail_trace_disabled():
    for trace in (False, "false"):
        headers = AnthropicTextClient._get_bedrock_guardrail_headers(
            GUARDRAIL_ID, GUARDRAIL_VERSION, trace=trace
        )
        assert "X-Amzn-Bedrock-Trace" not in headers


def test_guardrail_intervention_returns_part_with_violations(capsys):
    from anthropic.types import RawMessageStopEvent

    event = RawMessageStopEvent.model_validate(
        {
            "type": "message_stop",
            "amazon-bedrock-guardrailAction": "INTERVENED",
            "amazon-bedrock-trace": {
                "guardrail": {
                    "input": {
                        "gr123": {
                            "topicPolicy": {
                                "topics": [
                                    {"name": "Politics", "type": "DENY", "action": "BLOCKED"}
                                ]
                            },
                            "contentPolicy": {
                                "filters": [
                                    {"type": "VIOLENCE", "confidence": "HIGH", "action": "BLOCKED"},
                                    {"type": "HATE", "confidence": "NONE", "action": "NONE"},
                                ]
                            },
                        }
                    },
                    "outputs": [
                        {
                            "gr123": {
                                "sensitiveInformationPolicy": {
                                    "piiEntities": [
                                        {"type": "EMAIL", "match": "a@b.com", "action": "ANONYMIZED"}
                                    ]
                                }
                            }
                        }
                    ],
                }
            },
        }
    )
    part = AnthropicTextClient._process_guardrail_trace(event)
    assert "[guardrail] action=INTERVENED" in capsys.readouterr().out
    assert part["type"] == "GUARDRAIL"
    assert part["action"] == "INTERVENED"
    assert {
        "source": "INPUT",
        "policy": "topicPolicy",
        "rule": "Politics",
        "action": "BLOCKED",
    } in part["violations"]
    assert {
        "source": "INPUT",
        "policy": "contentPolicy",
        "rule": "VIOLENCE",
        "action": "BLOCKED",
    } in part["violations"]
    assert {
        "source": "OUTPUT",
        "policy": "sensitiveInformationPolicy",
        "rule": "EMAIL",
        "action": "ANONYMIZED",
    } in part["violations"]
    # action NONE rules are excluded
    assert all(v["action"] != "NONE" for v in part["violations"])


def test_guardrail_pass_through_logs_but_returns_no_part(capsys):
    from anthropic.types import RawMessageStopEvent

    event = RawMessageStopEvent.model_validate(
        {
            "type": "message_stop",
            "amazon-bedrock-guardrailAction": "NONE",
            "amazon-bedrock-trace": {"guardrail": {"input": {"gr123": {}}}},
        }
    )
    part = AnthropicTextClient._process_guardrail_trace(event)
    assert "[guardrail] action=NONE" in capsys.readouterr().out
    assert part is None


def test_no_trace_logged_without_guardrail_fields(capsys):
    from anthropic.types import RawMessageStopEvent

    event = RawMessageStopEvent.model_validate({"type": "message_stop"})
    part = AnthropicTextClient._process_guardrail_trace(event)
    assert capsys.readouterr().out == ""
    assert part is None


def test_partial_guardrail_config_raises():
    with pytest.raises(ValueError):
        AnthropicTextClient._get_bedrock_guardrail_headers(GUARDRAIL_ID, None)
    with pytest.raises(ValueError):
        AnthropicTextClient._get_bedrock_guardrail_headers(None, GUARDRAIL_VERSION)
