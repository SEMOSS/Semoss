"""Offline prefix-preservation checks for the production provider message builders.

Run with the SEMOSS Python environment. Set SEMOSS_RUNTIME_CONTEXT_FIXTURE to the
JSON exported by RoomRuntimeContextTest to check actual Java message serialization.
"""
import copy
import json
import os
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[5] / "py"))
from genai_client.message_builders.semoss_base.semoss_message_builder import SEMOSSMessageBuilder
from genai_client.message_builders.semoss_base.semoss_models import ModelSettings
from genai_client.message_builders.openai.openai_message_builder import OpenAIMessageBuilder
from genai_client.message_builders.anthropic.anthropic_message_builder import AnthropicMessageBuilder
from genai_client.message_builders.bedrock.bedrock_message_builder import BedrockMessageBuilder
from genai_client.message_builders.google_genai.google_genai_builder import GoogleGenAIMessageBuilder

SYSTEM = "Stable agent instructions. Use the latest SEMOSS runtime status."
TOOLS = [{"name": "ReadFile", "description": "Read a file", "inputSchema": {
    "type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}}]


def note(rounds):
    return f"[SEMOSS runtime status]\n{rounds} remaining\n[/SEMOSS runtime status]"


def fixture():
    filename = os.environ.get("SEMOSS_RUNTIME_CONTEXT_FIXTURE")
    if filename:
        return json.loads(Path(filename).read_text())["requests"]
    def message(kind, io, parts):
        return {"schemaVersion": 2, "type": kind, "io": io, "parts": parts}
    first = [message("INPUT_TEXT", "INPUT", [{"type": "SYSTEM", "prompt": SYSTEM},
        {"type": "TEXT", "text": "Create slides.\n\n" + note(40), "uiText": "Create slides."}])]
    requests = []
    for index, ids in enumerate([["a", "b"], ["c"]]):
        first.append(message("RESPONSE_TOOL", "OUTPUT", [{"type": "TOOL_CALL", "toolCall": {
            "id": id, "name": "ReadFile", "arguments": {"path": id + ".txt"}}} for id in ids]))
        parts = [{"type": "SYSTEM", "prompt": SYSTEM}] + [{"type": "TOOL_RESULT", "toolResult": {
            "id": id, "toolName": "ReadFile", "output": "output-" + id, "status": "success"}} for id in ids]
        parts.append({"type": "TEXT", "text": note(39-index)})
        first.append(message("INPUT_TOOL_EXEC", "INPUT", parts))
        requests.append(copy.deepcopy(first))
    return requests


def plain(value):
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=True)
    if isinstance(value, list):
        return [plain(item) for item in value]
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    return value


def provider_request(provider, history):
    settings = ModelSettings(model_name="fixture", max_tokens=2000)
    messages = SEMOSSMessageBuilder().build_messages(copy.deepcopy(history), {"tools": copy.deepcopy(TOOLS)}, settings)
    if provider in {"chat-completion", "responses"}:
        req = OpenAIMessageBuilder(settings, provider).build_request(messages)
        if provider == "chat-completion":
            return req["messages"][0], req["messages"][1:], req.get("tools")
        return req.get("instructions"), req["input"], req.get("tools")
    if provider == "anthropic":
        req = plain(AnthropicMessageBuilder().build_messages(messages, settings, "claude-sonnet-4-5").request_config)
        return req.get("system"), req["messages"], req.get("tools")
    if provider == "bedrock":
        req = BedrockMessageBuilder().build_messages(messages)
        return req["system"], req["messages"], req["toolConfig"]
    req = GoogleGenAIMessageBuilder().build_messages(messages, settings)
    config = plain(req["provider_config"])
    return config.get("system_instruction"), plain(req["messages"]), config.get("tools")


class RuntimeContextWireTest(unittest.TestCase):
    def assert_provider_prefix(self, provider):
        first, second = [tuple(plain(v) for v in provider_request(provider, h)) for h in fixture()]
        first_system, first_messages, first_tools = first
        second_system, second_messages, second_tools = second
        self.assertEqual(first_system, second_system)
        self.assertIn(SYSTEM, json.dumps(first_system))
        self.assertNotIn("remaining", json.dumps(first_system))
        self.assertEqual(first_tools, second_tools)
        self.assertEqual(first_messages, second_messages[:len(first_messages)])
        self.assertGreater(len(second_messages), len(first_messages))
        self.assertIn("39 remaining", json.dumps(first_messages[-1]))
        self.assertIn("38 remaining", json.dumps(second_messages[-1]))
        self.assertIn("40 remaining", json.dumps(first_messages[0]))
        # The newest status follows both parallel tool results on the provider wire.
        serialized = json.dumps(first_messages)
        self.assertLess(serialized.index("output-a"), serialized.index("39 remaining"))
        self.assertLess(serialized.index("output-b"), serialized.index("39 remaining"))
        self.assertIn("output-c", json.dumps(second_messages))

    def test_chat_completion_prefix(self): self.assert_provider_prefix("chat-completion")
    def test_responses_prefix(self): self.assert_provider_prefix("responses")
    def test_anthropic_prefix(self): self.assert_provider_prefix("anthropic")
    def test_bedrock_prefix(self): self.assert_provider_prefix("bedrock")
    def test_google_prefix(self): self.assert_provider_prefix("google")

    def test_detector_catches_the_previous_system_prompt_countdown(self):
        histories = fixture()
        for count, history in zip([39, 38], histories):
            history[-1]["parts"][0] = {"type": "SYSTEM", "prompt": SYSTEM + "\n" + note(count)}
        before, after = [provider_request("chat-completion", h)[0] for h in histories]
        self.assertNotEqual(before, after)


if __name__ == "__main__":
    unittest.main()
