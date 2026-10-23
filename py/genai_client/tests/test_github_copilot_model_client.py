"""Offline checks for the GitHub Copilot model client's prompt flattening.

No Copilot runtime and no model calls: only ``build_prompt``.
"""

from genai_client.message_builders.semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSTextMessagePart,
)
from genai_client.text_generation.github_copilot_model_client import build_prompt


def _message(io, text=None, content=None, param_map=None):
    return SEMOSSMessage(
        type="INPUT_TEXT",
        io=io,
        parts=[SEMOSSTextMessagePart(text=text)] if text is not None else None,
        content=content,
        param_map=param_map or {},
    )


def test_single_turn_is_sent_verbatim_with_its_system_prompt():
    system_prompt, prompt = build_prompt(
        [_message("INPUT", "hello", param_map={"system_prompt": "be brief"})]
    )
    assert system_prompt == "be brief"
    assert prompt == "hello"


def test_history_is_flattened_in_order_with_roles():
    system_prompt, prompt = build_prompt(
        [_message("INPUT", "a"), _message("OUTPUT", "b"), _message("INPUT", "c")]
    )
    assert system_prompt is None
    assert prompt.endswith("User: a\n\nAssistant: b\n\nUser: c")
    assert prompt.startswith("Conversation so far")


def test_legacy_content_is_used_and_empty_messages_are_skipped():
    _, prompt = build_prompt(
        [_message("INPUT", content="legacy"), _message("OUTPUT"), _message("INPUT", "now")]
    )
    assert prompt.endswith("User: legacy\n\nUser: now")


def test_no_messages_gives_empty_prompt():
    assert build_prompt([]) == (None, "")
