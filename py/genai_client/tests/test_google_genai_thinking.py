"""Offline checks of Gemini thinking parameters sent to the provider."""

import pytest

from genai_client.message_builders.google_genai.google_genai_builder import (
    GoogleGenAIMessageBuilder,
)
from genai_client.message_builders.semoss_base.semoss_message_builder import (
    SEMOSSMessageBuilder,
)
from genai_client.message_builders.semoss_base.semoss_models import ModelSettings


def thinking_config(settings, **params):
    messages = SEMOSSMessageBuilder().build_messages(
        [{"type": "INPUT_TEXT", "io": "INPUT", "inputPrompt": "Hello"}],
        params,
        settings,
    )
    result = GoogleGenAIMessageBuilder().build_messages(messages, settings)
    return result["provider_config"].thinking_config.model_dump(exclude_none=True)


@pytest.mark.parametrize(
    "model_name", ["gemini-4-carbon", "publishers/google/models/gemini-4-carbon"]
)
@pytest.mark.parametrize(
    "effort,level",
    [
        ("none", "LOW"),
        ("minimal", "LOW"),
        ("low", "LOW"),
        ("medium", "MEDIUM"),
        ("high", "HIGH"),
        ("xhigh", "HIGH"),
        ("max", "HIGH"),
    ],
)
def test_carbon_sends_supported_levels_without_a_budget(model_name, effort, level):
    config = thinking_config(
        ModelSettings(model_name=model_name), thinking=True, effort=effort
    )
    assert config == {"include_thoughts": True, "thinking_level": level}


def test_carbon_uses_settings_and_request_effort_takes_precedence():
    settings = ModelSettings(
        model_name="gemini-4-carbon", thinking=True, thinking_budget=8192, effort="low"
    )
    assert thinking_config(settings) == {
        "include_thoughts": True,
        "thinking_level": "LOW",
    }
    assert thinking_config(settings, effort="high") == {
        "include_thoughts": True,
        "thinking_level": "HIGH",
    }
    assert thinking_config(settings, thinking=False) == {"include_thoughts": False}


def test_carbon_defaults_to_medium_when_thinking_is_enabled():
    assert thinking_config(
        ModelSettings(model_name="gemini-4-carbon"), thinking=True
    ) == {"include_thoughts": True, "thinking_level": "MEDIUM"}


def test_gemini_3_keeps_minimal_thinking_level():
    assert thinking_config(
        ModelSettings(model_name="gemini-3-flash-preview"),
        thinking=True,
        effort="minimal",
    ) == {"include_thoughts": True, "thinking_level": "MINIMAL"}


@pytest.mark.parametrize("model_name", ["gemini-2.5-flash", "gemini-3-pro-image-preview"])
def test_budget_models_keep_their_explicit_budget(model_name):
    assert thinking_config(
        ModelSettings(model_name=model_name), thinking=True, thinking_budget=4096
    ) == {"include_thoughts": True, "thinking_budget": 4096}
