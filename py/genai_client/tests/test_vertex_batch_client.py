"""Offline checks of building and reading Vertex batch lines; no model calls.
The Claude lines are from a live Claude-on-Vertex batch job."""

import json

from google.genai import Client, types

from genai_client.clients.vertex_batch_client import (
    normalize_gemini_result_line,
    normalize_result_line,
    read_output_files,
)
from genai_client.text_generation.google_genai_clients.google_genai_client import (
    GoogleGenAiTextClient,
)

CLAUDE_ANSWER = r'{"custom_id":"req-1","status":"","processed_time":"2026-10-19T20:21:38.212+00:00","response":{"container":null,"content":[{"text":"Red","type":"text"}],"id":"msg_vrtx_011CgCDbjoDFK4o8wLZp5jKg","model":"claude-sonnet-5","role":"assistant","stop_details":null,"stop_reason":"end_turn","stop_sequence":null,"type":"message","usage":{"cache_creation":{"ephemeral_1h_input_tokens":0,"ephemeral_5m_input_tokens":0},"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"input_tokens":24,"output_tokens":4,"output_tokens_details":{"thinking_tokens":0}}}}'
CLAUDE_REFUSED = r'{"custom_id":"req-3","status":"Bad Request: {\"type\": \"error\", \"error\": {\"type\": \"invalid_request_error\", \"message\": \"temperature: range: 0..1\"}, \"request_id\": \"req_vrtx_011CgCDbpppdpExiR6YEezy2\"}","processed_time":"2026-10-19T20:21:39.369+00:00","response":{}}'


def test_claude_answer():
    item = normalize_result_line(json.loads(CLAUDE_ANSWER))
    assert item["ok"] and item["custom_id"] == "req-1"
    assert item["message"] == {"role": "assistant", "content": [{"text": "Red", "type": "text"}]}
    assert (item["input_tokens"], item["output_tokens"]) == (24, 4)


def test_claude_refused_request_is_an_error():
    item = normalize_result_line(json.loads(CLAUDE_REFUSED))
    assert item["ok"] is False and item["message"] is None
    assert "temperature: range: 0..1" in item["error"]


def test_gemini_answer_skips_thoughts_and_bills_them_as_output():
    item = normalize_gemini_result_line({"id": "req-0", "status": "", "response": {
        "candidates": [{"content": {"role": "model", "parts": [
            {"text": "thinking it over", "thought": True}, {"text": "The answer "}, {"text": "is 4."}]}}],
        "usageMetadata": {"promptTokenCount": 7, "candidatesTokenCount": 5, "thoughtsTokenCount": 30}}})
    assert item["message"]["content"] == [{"type": "text", "text": "The answer is 4."}]
    assert (item["input_tokens"], item["output_tokens"]) == (7, 35)


def test_gemini_refused_request_is_an_error():
    item = normalize_gemini_result_line({"id": "req-1", "status": "Quota exceeded", "request": {}})
    assert item["ok"] is False and item["error"] == "Quota exceeded"


def test_read_output_files_reads_every_line(tmp_path):
    output = tmp_path / "predictions.jsonl"
    output.write_text(CLAUDE_ANSWER + "\n\n" + CLAUDE_REFUSED + "\n", encoding="utf-8")
    items = read_output_files([str(output)], normalize_result_line)
    assert [(item["custom_id"], item["ok"]) for item in items] == [("req-1", True), ("req-3", False)]


def _gemini_client():
    client = GoogleGenAiTextClient(model_name="gemini-2.5-flash", api_key="offline-test")
    # Vertex's request conversion, with no credentials or network
    client.google_client = Client(vertexai=True, project="p", location="global")
    client.client_config.project, client.client_config.region = "p", "global"
    return client


def _batch_lines(client, requests):
    built = client.submit_batch(requests)
    return [json.loads(line) for line in built["raw"]["jsonl_content"].splitlines()]


def test_gemini_batch_line_is_the_vertex_request_shape():
    # built by a private SDK converter: this fails if a google-genai upgrade moves or changes it
    [line] = _batch_lines(_gemini_client(), [{"custom_id": "r0", "command": "What is 2+2?",
                                              "context": "Answer briefly.", "max_tokens": 50, "temperature": 0.2}])
    request = line["request"]
    assert line["id"] == "r0"
    assert request["contents"] == [{"parts": [{"text": "What is 2+2?"}], "role": "user"}]
    assert request["systemInstruction"] == {"parts": [{"text": "Answer briefly."}], "role": "user"}
    assert (request["generationConfig"]["maxOutputTokens"], request["generationConfig"]["temperature"]) == (50, 0.2)


def test_gemini_batch_line_sends_thought_signatures_as_base64(monkeypatch):
    client = _gemini_client()
    signed = types.Content(role="model", parts=[types.Part(
        function_call=types.FunctionCall(name="get_weather", args={"city": "Paris"}), thought_signature=b"sig-bytes")])
    monkeypatch.setattr(client, "_build_batch_contents", lambda req, idx: ([signed], types.GenerateContentConfig()))
    [line] = _batch_lines(client, [{"custom_id": "r0", "command": "weather?"}])
    assert line["request"]["contents"][0]["parts"][0]["thoughtSignature"] == "c2lnLWJ5dGVz"
