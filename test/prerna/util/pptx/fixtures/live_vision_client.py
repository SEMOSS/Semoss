"""Fixture-only live inference through the installed SEMOSS Python client."""
import base64
import contextlib
import io
import hashlib
import httpx
import json
import os
import re
from pathlib import Path
import sys

repo = Path(sys.argv[1])
sys.path.insert(0, str(repo / "py"))
# Mirror the application startup before importing provider transports.
import smss_system_certs
from genai_client.text_generation.openai_clients.openai_client import OpenAiClient

config = {}
engine_id = os.environ["PPTX_TEST_VISION_ENGINE"]
model_file = next((repo / "model").glob("*__" + engine_id + ".smss"))
for line in model_file.read_text().splitlines():
    bits = line.split(None, 1)
    if len(bits) == 2 and not line.startswith("#"):
        config[bits[0]] = bits[1]
endpoint_match = re.search(r"endpoint\s*=\s*[\"\']([^\"\']+)[\"\']", config.get("INIT_MODEL_ENGINE", ""))
client_options = {"endpoint": endpoint_match.group(1)} if endpoint_match else {}
request = json.load(sys.stdin)
parts = [{"type": "SYSTEM", "prompt": request["system"]}, {"type": "TEXT", "text": request["prompt"]}]
for image in request["images"]:
    image_path = Path(image)
    parts.append({"type": "MEDIA", "mediaInfo": {"fileName": image_path.name, "fileFormat": "png",
                 "mimeType": "image/png", "base64Data": base64.b64encode(image_path.read_bytes()).decode()}})
messages = [{"schemaVersion": 2, "type": "INPUT_TEXT", "io": "INPUT", "parts": parts,
             "paramMap": {"stream": False, "max_tokens": 2000, "schema": request["schema"]}}]
wire = []
def inspect_wire(outgoing):
    body = json.loads(outgoing.content)
    fmt = body.get("response_format", body.get("text", {}).get("format", {}))
    details = fmt.get("json_schema", fmt)
    assert fmt.get("type") == "json_schema" and details["strict"] is True, "Missing strict schema on provider wire"
    assert details["schema"] == request["schema"], "Schema changed on provider wire"
    parts = [p for m in body.get("messages", body.get("input", [])) for p in (m["content"] if isinstance(m["content"], list) else [])]
    imgs = [p["image_url"]["url"] if isinstance(p["image_url"], dict) else p["image_url"] for p in parts if p.get("type") in {"image_url", "input_image"}]
    assert len(imgs) == len(request["images"]) == 1
    expected = hashlib.sha256(Path(request["images"][0]).read_bytes()).hexdigest()
    actual = hashlib.sha256(base64.b64decode(imgs[0].split(",", 1)[1])).hexdigest()
    assert expected == actual, "Image bytes changed on provider wire"
    assert "schema" not in body and "tools" not in body and "tool_choice" not in body
    wire.append({"strictSchema": True, "imageSha256": actual})
with contextlib.redirect_stdout(io.StringIO()):
    client = OpenAiClient(False, model_name=config["MODEL"], api_key=config["OPEN_AI_KEY"],
                          chat_type=config.get("CHAT_TYPE", "responses"), timeout=120, max_retries=0, http_client=httpx.Client(event_hooks={"request": [inspect_wire]}), **client_options)
    result = client.ask(message_json=json.dumps(messages), stream=False, max_tokens=request.get("maxTokens", 2000), schema=request["schema"])
text = result.get("response")
if not isinstance(text, str):
    text = "\n".join(p.get("text", "") for p in result.get("parts", []) if p.get("type") == "TEXT")
if not text:
    detail = json.dumps(result).replace(config["OPEN_AI_KEY"], "<redacted>")
    raise RuntimeError("Vision provider returned no text: " + detail[-6000:])
json.dump({"text": text, "wire": wire, "inputTokens": result.get("numberOfTokensInPrompt", result.get("numberOfTokensInInput", 0)) or 0,
           "outputTokens": result.get("numberOfTokensInResponse", result.get("numberOfTokensInOutput", 0)) or 0}, sys.stdout)
