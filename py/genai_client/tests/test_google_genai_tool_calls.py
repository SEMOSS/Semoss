"""Offline regression checks for harness tool-result replay."""

import base64

import pytest

from genai_client.message_builders.google_genai.google_genai_builder import (
    GoogleGenAIMessageBuilder,
)
from genai_client.message_builders.semoss_base.semoss_message_builder import (
    SEMOSSMessageBuilder,
)
from genai_client.message_builders.semoss_base.semoss_models import ModelSettings


@pytest.mark.parametrize("runtime_text", [False, True])
def test_parallel_tool_results_keep_names_ids_and_thought_signatures(runtime_text):
    calls = [("call-1", "LoadSkill"), ("call-2", "ReadFile"), ("call-3", "LoadSkill")]
    history = [
        {"type": "INPUT_TEXT", "io": "INPUT", "inputPrompt": "Read the project."},
        {
            "type": "RESPONSE_TOOL",
            "io": "OUTPUT",
            "schemaVersion": 2,
            "parts": [
                {
                    "type": "TOOL_CALL",
                    "toolCall": {
                        "id": call_id,
                        "name": name,
                        "arguments": {"path": call_id},
                        "thought_signature": (
                            base64.b64encode(b"signed-state").decode()
                            if call_id == "call-1"
                            else None
                        ),
                    },
                }
                for call_id, name in calls
            ],
        },
        {
            "type": "INPUT_TOOL_EXEC",
            "io": "INPUT",
            "schemaVersion": 2,
            "parts": [
                {
                    "type": "TOOL_RESULT",
                    "toolResult": {"toolCallId": call_id, "output": call_id},
                }
                for call_id, _ in reversed(calls)
            ]
            + ([{"type": "TEXT", "text": "Continue."}] if runtime_text else []),
        },
    ]
    settings = ModelSettings(model_name="gemini-4-carbon")
    messages = SEMOSSMessageBuilder().build_messages(history, {}, settings)
    contents = GoogleGenAIMessageBuilder().build_messages(messages, settings)["messages"]

    assert [content.role for content in contents] == (
        ["user", "model", "user", "user"] if runtime_text else ["user", "model", "user"]
    )
    responses = [part.function_response for part in contents[2].parts]
    assert [(result.name, result.response) for result in responses] == [
        (name, {"result": call_id}) for call_id, name in reversed(calls)
    ]
    assert [result.id for result in responses] == [
        call_id for call_id, _ in reversed(calls)
    ]
    assert [part.function_call.id for part in contents[1].parts] == [
        call_id for call_id, _ in calls
    ]
    assert contents[1].parts[0].thought_signature == b"signed-state"
    assert contents[1].parts[1].thought_signature is None
    if runtime_text:
        assert len(contents[-1].parts) == 1
        assert contents[-1].parts[0].text == "Continue."
