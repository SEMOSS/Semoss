"""Offline contract check for the SEMOSS Python provider request used by InspectPptx.

Run with the SEMOSS Python environment: python test/prerna/util/pptx/fixtures/check_provider_wire.py
"""
import base64
import contextlib
import io
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[5] / "py"))
import httpx
from genai_client.text_generation.openai_clients.openai_client import OpenAiClient


class InspectionWireTest(unittest.TestCase):
    def test_strict_schema_and_exact_image_bytes_reach_provider(self):
        image = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")
        schema = {"type": "object", "properties": {"assessment": {"type": "string", "enum": ["reviewed", "inconclusive"]}},
                  "required": ["assessment"], "additionalProperties": False}
        calls = []
        def transport(request):
            body = json.loads(request.content)
            calls.append(body)
            self.assertEqual(body["response_format"]["type"], "json_schema")
            self.assertIs(body["response_format"]["json_schema"]["strict"], True)
            self.assertEqual(body["response_format"]["json_schema"]["schema"], schema)
            content = body["messages"][-1]["content"]
            images = [p["image_url"]["url"] for p in content if p["type"] == "image_url"]
            self.assertEqual(len(images), 1)
            self.assertEqual(base64.b64decode(images[0].split(",", 1)[1]), image)
            for key in ["schema", "tools", "tool_choice"]:
                self.assertNotIn(key, body)
            return httpx.Response(200, json={"id": "fixture", "object": "chat.completion", "created": 0, "model": "fixture",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": '{"assessment":"reviewed"}'}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
        params = {"schema": schema, "stream": False, "max_tokens": 100}
        messages = [{"schemaVersion": 2, "type": "INPUT_TEXT", "io": "INPUT", "paramMap": params, "parts": [
            {"type": "SYSTEM", "prompt": "Inspect the attached slide."}, {"type": "TEXT", "text": "Check readability."},
            {"type": "MEDIA", "mediaInfo": {"fileName": "slide.png", "fileFormat": "png", "mimeType": "image/png", "base64Data": base64.b64encode(image).decode()}}]}]
        with contextlib.redirect_stdout(io.StringIO()), httpx.Client(transport=httpx.MockTransport(transport)) as http:
            client = OpenAiClient(False, model_name="fixture", api_key="test-only", endpoint="https://fixture.invalid/v1", chat_type="chat-completion", http_client=http, max_retries=0)
            # Java forwards the current parameter map as keyword arguments as well as message_json.
            result = client.ask(message_json=json.dumps(messages), **params)
        self.assertEqual(len(calls), 1)
        self.assertNotEqual(result.get("messageType"), "ERROR", result.get("message"))


if __name__ == "__main__":
    unittest.main()
