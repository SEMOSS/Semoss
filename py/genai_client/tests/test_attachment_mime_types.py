"""Offline checks of attachment payloads at each provider boundary."""

import base64
import json
from pathlib import Path

import pytest

from genai_client.message_builders.anthropic.anthropic_message_builder import (
    AnthropicMessageBuilder,
)
from genai_client.message_builders.ask_sage_builder import ask_sage_message_builder
from genai_client.message_builders.bedrock.bedrock_message_builder import (
    BedrockMessageBuilder,
)
from genai_client.message_builders.google_genai.google_genai_builder import (
    GoogleGenAIMessageBuilder,
)
from genai_client.message_builders.openai.openai_message_builder import (
    OpenAIMessageBuilder,
)
from genai_client.message_builders.semoss_base.media_types import (
    decode_base64_text,
    is_text_mime_type,
    prepare_base64_media,
    resolve_mime_type,
)
from genai_client.message_builders.semoss_base.semoss_message_builder import (
    SEMOSSMessageBuilder,
)
from genai_client.message_builders.semoss_base.semoss_models import (
    ModelSettings,
    SEMOSSMediaContent,
    SEMOSSMultimodalToolBlock,
)

TEXT = "# Notes\n\nCafé — 日本語\n```python\nprint('hello')\n```\n"
PAYLOAD = base64.b64encode(TEXT.encode()).decode()
SETTINGS = ModelSettings(model_name="fixture", max_tokens=1024)
PROVIDERS = ["responses", "chat-completion", "anthropic", "bedrock", "google"]


def messages_for(media, schema_version=2):
    history = [{"type": "INPUT_TEXT", "io": "INPUT", "schemaVersion": schema_version}]
    if schema_version == 2:
        history[0]["parts"] = [
            {"type": "MEDIA", "mediaInfo": media},
            {"type": "TEXT", "text": "Summarize the attachment."},
        ]
    else:
        history[0].update(
            type="INPUT_MEDIA",
            inputPrompt="Summarize the attachment.",
            mediaInputs=[media],
        )
    return SEMOSSMessageBuilder().build_messages(history, {}, SETTINGS)


def build_attachment(provider, messages):
    index = 0 if messages[0].parts else -1
    if provider in {"responses", "chat-completion"}:
        request = OpenAIMessageBuilder(SETTINGS, provider).build_request(messages)
        key = "input" if provider == "responses" else "messages"
        content = request[key][0]["content"]
        return (
            {"type": "text", "text": content}
            if isinstance(content, str)
            else content[index]
        )
    if provider == "anthropic":
        result = AnthropicMessageBuilder().build_messages(
            messages, SETTINGS, "claude-sonnet-4-5"
        )
        return result.request_config.messages[0]["content"][index]
    if provider == "bedrock":
        return BedrockMessageBuilder().build_messages(messages)["messages"][0][
            "content"
        ][index]
    result = GoogleGenAIMessageBuilder().build_messages(messages, SETTINGS)
    return result["messages"][0].parts[index].model_dump(exclude_none=True)


@pytest.mark.parametrize("provider", PROVIDERS)
@pytest.mark.parametrize(
    "mime,filename",
    [
        ("text/x-web-markdown", "notes.md"),
        (" TEXT/X-WEB-MARKDOWN; charset=utf-8 ", "notes.MD"),
        ("text/x-markdown", "notes.markdown"),
        ("text/markdown", "notes.md"),
        (None, "notes.md"),
        ("application/octet-stream", "notes.MD"),
        ("text/x-custom-source", "source.custom"),
        ("application/x-yaml", "config.yaml"),
        (None, "app.log"),
        ("application/json", "config.json"),
    ],
)
def test_text_attachments_are_serialized_for_the_provider(provider, mime, filename):
    messages = messages_for(
        {"mimeType": mime, "fileName": filename, "base64Data": PAYLOAD}
    )
    original = messages[0].parts[0].media_info.model_dump()
    part = build_attachment(provider, messages)
    if provider == "responses":
        assert part == {
            "type": "input_file",
            "filename": filename,
            "file_data": f"data:text/plain;base64,{PAYLOAD}",
        }
    elif provider == "chat-completion":
        assert part == {"type": "text", "text": f"Attached file: {filename}\n\n{TEXT}"}
    elif provider == "anthropic":
        assert part == {
            "type": "document",
            "source": {"type": "text", "media_type": "text/plain", "data": TEXT},
        }
    elif provider == "bedrock":
        assert part["document"]["format"] in {"txt", "md"}
        assert part["document"]["source"]["bytes"] == TEXT.encode()
    else:
        assert part["inline_data"] == {"mime_type": "text/plain", "data": TEXT.encode()}
    assert messages[0].parts[0].media_info.model_dump() == original


@pytest.mark.parametrize("provider", PROVIDERS)
def test_base64_data_url_is_not_nested_or_double_encoded(provider):
    messages = messages_for(
        {
            "fileName": "notes.md",
            "base64Data": f"data:text/x-web-markdown;base64,{PAYLOAD}",
        }
    )
    part = build_attachment(provider, messages)
    assert "text/x-web-markdown" not in str(part)
    if provider == "responses":
        assert part["file_data"] == f"data:text/plain;base64,{PAYLOAD}"
    elif provider == "google":
        assert part["inline_data"]["data"] == TEXT.encode()
    elif provider == "bedrock":
        assert part["document"]["source"]["bytes"] == TEXT.encode()
    elif provider == "anthropic":
        assert part["source"]["data"] == TEXT
    else:
        assert part["text"].endswith(TEXT)


@pytest.mark.parametrize("provider", PROVIDERS)
@pytest.mark.parametrize(
    "mime,filename,raw",
    [
        ("application/pdf", "report.pdf", b"%PDF-1.7\n\x00\xff"),
        ("image/jpg", "photo.jpg", b"\xff\xd8\xff\x00"),
    ],
)
def test_binary_attachments_retain_their_type_and_bytes(provider, mime, filename, raw):
    data = base64.b64encode(raw).decode()
    messages = messages_for(
        {"mimeType": mime, "fileName": filename, "base64Data": data}
    )
    part = build_attachment(provider, messages)
    canonical = "image/jpeg" if mime == "image/jpg" else mime
    if provider == "responses":
        assert (
            part.get("file_data") or part.get("image_url")
        ) == f"data:{canonical};base64,{data}"
    elif provider == "chat-completion":
        if mime.startswith("image/"):
            assert part["image_url"]["url"] == f"data:{canonical};base64,{data}"
        else:
            assert part["file"]["file_data"] == f"data:{canonical};base64,{data}"
    elif provider == "anthropic":
        assert part["source"] == {
            "type": "base64",
            "media_type": canonical,
            "data": data,
        }
    elif provider == "bedrock":
        block = part["image"] if mime.startswith("image/") else part["document"]
        assert block["format"] == ("jpeg" if mime.startswith("image/") else "pdf")
        assert block["source"]["bytes"] == raw
    else:
        assert part["inline_data"] == {"mime_type": canonical, "data": raw}


@pytest.mark.parametrize("provider", PROVIDERS)
def test_legacy_messages_use_the_same_text_conversion(provider):
    messages = messages_for(
        {
            "mimeType": "text/x-web-markdown",
            "fileName": "notes.md",
            "base64Data": PAYLOAD,
        },
        schema_version=1,
    )
    # Legacy builders put the prompt before the attachment.
    messages[0].content = ""
    part = build_attachment(provider, messages)
    assert "text/x-web-markdown" not in str(part)
    assert (
        TEXT in part.get("text", "")
        or TEXT == part.get("source", {}).get("data")
        or PAYLOAD in str(part)
        or TEXT.encode()
        in (
            part.get("inline_data", {}).get("data", b""),
            part.get("document", {}).get("source", {}).get("bytes", b""),
        )
    )


def test_text_tool_result_attachments_use_provider_wire_formats():
    blocks = [
        SEMOSSMultimodalToolBlock(
            type="document", mime_type="text/x-web-markdown", data=PAYLOAD
        )
    ]
    openai = OpenAIMessageBuilder(SETTINGS, "responses")._build_responses_tool_output(
        "", blocks
    )
    assert openai == [
        {
            "type": "input_file",
            "filename": "document.txt",
            "file_data": f"data:text/plain;base64,{PAYLOAD}",
        }
    ]
    output = json.dumps(
        {"SEMOSSMultimodalToolResponse": [b.model_dump() for b in blocks]}
    )
    anthropic = AnthropicMessageBuilder()._parse_tool_result_content(output)
    assert anthropic[0].source.model_dump() == {
        "type": "text",
        "media_type": "text/plain",
        "data": TEXT,
    }
    bedrock = BedrockMessageBuilder()._build_bedrock_tool_content("", blocks)
    assert bedrock[0]["document"]["format"] == "md"
    assert bedrock[0]["document"]["source"]["bytes"] == TEXT.encode()
    google = GoogleGenAIMessageBuilder()._build_function_response_part(
        "read", "", blocks
    )
    assert google.function_response.response == {"result": TEXT}


@pytest.mark.parametrize("schema_version", [1, 2])
def test_ask_sage_uploads_text_as_a_txt_file(tmp_path, monkeypatch, schema_version):
    monkeypatch.setattr(
        ask_sage_message_builder, "__file__", str(tmp_path / "builder.py")
    )
    messages = messages_for(
        {
            "mimeType": "text/x-web-markdown",
            "fileName": "notes.md",
            "base64Data": f"data:text/x-web-markdown;base64,{PAYLOAD}",
        },
        schema_version,
    )
    request, paths = ask_sage_message_builder.AskSageMessageBuilder(
        SETTINGS
    ).build_request(messages)
    assert request.message[0]["message"] == "Summarize the attachment."
    assert len(paths) == 1
    assert Path(paths[0]).suffix == ".txt"
    assert Path(paths[0]).read_bytes() == TEXT.encode()


def test_direct_anthropic_documents_use_text_sources():
    builder = AnthropicMessageBuilder()
    doc = {"mime_type": "text/x-web-markdown", "data": PAYLOAD}
    for value in [doc, [doc]]:
        parts = builder._handle_base_64_docs_direct(value)
        assert parts[0].source.model_dump() == {
            "type": "text",
            "media_type": "text/plain",
            "data": TEXT,
        }


@pytest.mark.parametrize(
    "mime,filename",
    [
        ("application/pdf", "misleading.md"),
        ("application/zip", "archive.zip"),
        ("application/octet-stream", "unknown.bin"),
        ("application/octet-stream", "notes.txt.gz"),
        ("text/rtf", "document.rtf"),
        ("application/rtf", "document.rtf"),
    ],
)
def test_binary_and_rich_document_types_are_not_relabelled_as_text(mime, filename):
    resolved = resolve_mime_type(mime, filename)
    assert not is_text_mime_type(resolved)
    assert resolved == mime


def test_missing_mime_uses_file_format_and_does_not_modify_history():
    media = SEMOSSMediaContent(type="base64", data=PAYLOAD, format="MD")
    assert prepare_base64_media(media) == (PAYLOAD, "text/markdown")
    assert media.mime_type is None


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "utf-16", "utf-32"])
def test_decoded_text_preserves_unicode(encoding):
    assert decode_base64_text(base64.b64encode(TEXT.encode(encoding)).decode()) == TEXT


def test_text_decoding_does_not_silently_replace_binary_bytes():
    with pytest.raises(UnicodeDecodeError):
        decode_base64_text(base64.b64encode(b"\xff\x00\x80").decode())


def test_csv_keeps_spreadsheet_handling_where_supported():
    media = {"mimeType": "text/csv", "fileName": "table.csv", "base64Data": PAYLOAD}
    assert build_attachment("responses", messages_for(media))["file_data"].startswith(
        "data:text/csv;"
    )
    assert (
        build_attachment("bedrock", messages_for(media))["document"]["format"] == "csv"
    )
