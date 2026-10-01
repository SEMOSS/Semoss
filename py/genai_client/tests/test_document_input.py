"""Document delivery at the shared-client/provider boundary (no model calls)."""

import base64
import json
from unittest.mock import Mock

import pytest
from genai_client.message_builders.openai.openai_message_builder import (
    OpenAIMessageBuilder,
)
from genai_client.message_builders.semoss_base.document_input import (
    DocumentInputError,
    DocumentInputProcessor,
    ExtractedDocument,
)
from genai_client.message_builders.semoss_base.semoss_message_builder import (
    SEMOSSMessageBuilder,
)
from genai_client.message_builders.semoss_base.semoss_models import ModelSettings


def settings(**kwargs):
    return ModelSettings(model_name="fixture", native_document_mime_types=[], **kwargs)


def history(
    filename="report.pdf", raw=b"pdf fixture", mime="application/pdf", *, legacy=False
):
    media = {
        "fileName": filename,
        "mimeType": mime,
        "base64Data": base64.b64encode(raw).decode(),
    }
    common = {"messageId": "input-1", "type": "INPUT_TEXT", "io": "INPUT"}
    if legacy:
        return [
            {
                **common,
                "type": "INPUT_MEDIA",
                "inputPrompt": "Summarize.",
                "mediaInputs": [media],
            }
        ]
    return [
        {
            **common,
            "schemaVersion": 2,
            "parts": [
                {"type": "TEXT", "text": "Summarize."},
                {"type": "MEDIA", "mediaInfo": media},
            ],
        }
    ]


def messages(source=None):
    return SEMOSSMessageBuilder().build_messages(source or history(), {}, settings())


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("chat_type", ["chat-completion", "responses"])
def test_documents_reach_provider_as_text_without_changing_originals(legacy, chat_type):
    processor = DocumentInputProcessor()
    processor._convert = Mock(return_value=ExtractedDocument("## Page 1\nRevenue: 42"))
    original = messages(history(legacy=legacy))
    before = [message.model_dump() for message in original]
    prepared = processor.prepare(original, settings(), cache_scope="room-a")
    request = OpenAIMessageBuilder(settings(), chat_type).build_request(prepared)
    wire = json.dumps(request)
    assert "Revenue: 42" in wire
    assert "report.pdf" in wire
    assert "base64" not in wire
    assert "file_data" not in wire
    assert [message.model_dump() for message in original] == before
    content = request.get("messages", request.get("input"))[0]["content"]
    assert isinstance(content, str) or all(
        part["type"] in {"text", "input_text"} for part in content
    )


def test_unspecified_capabilities_preserve_native_delivery_without_docling():
    processor = DocumentInputProcessor()
    processor._convert = Mock(side_effect=AssertionError("must not extract"))
    original = messages()
    prepared = processor.prepare(original, ModelSettings(model_name="native"))
    assert prepared is original
    wire = OpenAIMessageBuilder(
        ModelSettings(model_name="native"), "chat-completion"
    ).build_request(prepared)
    assert wire["messages"][0]["content"][1]["type"] == "file"


@pytest.mark.parametrize(
    "modalities,native_extensions",
    [
        (["TEXT"], set()),
        (["TEXT", "IMAGE"], set()),
        (["text", "pdf"], {"pdf"}),
        (["TEXT", "FILE"], {"pdf", "docx", "pptx", "xlsx", "csv"}),
    ],
)
@pytest.mark.parametrize("extension", ["pdf", "docx", "pptx", "xlsx", "csv"])
@pytest.mark.parametrize("legacy", [False, True])
def test_engine_modalities_choose_delivery_without_init_setting(
    modalities, native_extensions, extension, legacy
):
    processor = DocumentInputProcessor()
    processor._convert = Mock(return_value=ExtractedDocument("Converted document"))
    config = ModelSettings(model_name="metadata-configured")
    source = history(f"report.{extension}", b"Revenue,42", None, legacy=legacy)
    original = messages(source)
    before = original[0].model_dump()
    prepared = processor.prepare(original, config, input_modalities=modalities)
    if extension in native_extensions:
        assert prepared[0].model_dump() == before
        processor._convert.assert_not_called()
    else:
        wire = json.dumps(
            OpenAIMessageBuilder(config, "chat-completion").build_request(prepared)
        )
        assert ("Revenue,42" if extension == "csv" else "Converted document") in wire
        assert "file_data" not in wire
        assert processor._convert.call_count == (0 if extension == "csv" else 1)
    assert original[0].model_dump() == before


def test_engine_metadata_takes_precedence_over_old_init_setting():
    processor = DocumentInputProcessor()
    processor._convert = Mock(return_value=ExtractedDocument("Extracted PDF"))
    native_config = ModelSettings(
        model_name="legacy", native_document_mime_types=["application/pdf"]
    )
    assert (
        processor.prepare(messages(), native_config, input_modalities=["TEXT"])[0]
        .parts[1]
        .type
        == "TEXT"
    )
    original = messages()
    assert (
        processor.prepare(original, settings(), input_modalities=["TEXT", "FILE"])
        is original
    )


def test_documents_cannot_be_extracted_for_a_model_without_text_input():
    processor = DocumentInputProcessor()
    processor._convert = Mock(side_effect=AssertionError("must not extract"))
    with pytest.raises(DocumentInputError, match="TEXT input capability"):
        processor.prepare(messages(), settings(), input_modalities=["AUDIO"])


def test_request_metadata_is_consumed_without_changing_client_or_history(monkeypatch):
    from genai_client.text_generation.openai_clients.openai_client import OpenAiClient

    monkeypatch.setattr(OpenAiClient, "_get_client", Mock())
    client = OpenAiClient(is_azure=False, model_name="laguna", api_key="fixture")
    client._document_input_processor._convert = Mock(
        return_value=ExtractedDocument("Revenue 42")
    )
    source = history()
    for modalities, expected_type in [
        (["TEXT"], "TEXT"),
        (["TEXT", "PDF"], "MEDIA"),
        (["TEXT"], "TEXT"),
    ]:
        prepared = client.build_semoss_messages(
            client.model_settings,
            message_json=json.dumps(source),
            _semoss_input_modalities=modalities,
        )
        assert prepared[0].parts[1].type == expected_type
        assert "_semoss_input_modalities" not in json.dumps(
            client.message_builder.build_request(prepared)
        )
    assert client.model_settings.native_document_mime_types is None
    client._document_input_processor._convert.assert_called_once()
    assert source[0]["parts"][1]["type"] == "MEDIA"


@pytest.mark.parametrize("model_type", ["image", "audio"])
def test_internal_metadata_does_not_reach_openai_media_clients(monkeypatch, model_type):
    from genai_client.text_generation.openai_clients.openai_client import OpenAiClient

    monkeypatch.setattr(OpenAiClient, "_get_client", Mock())
    client = OpenAiClient(
        is_azure=False, model_name="media", api_key="fixture", model_type=model_type
    )
    handler = Mock()
    if model_type == "image":
        client.image_client.ask_call = handler
    else:
        client.audio_client.ask = handler
    client.ask_call(
        message_json=json.dumps(history("notes.txt", b"Hello", "text/plain")),
        _semoss_input_modalities=["TEXT"],
    )
    handler.assert_called_once()
    assert "_semoss_input_modalities" not in handler.call_args.kwargs


@pytest.mark.parametrize("provider", ["openai", "anthropic"])
def test_batch_history_uses_engine_metadata_without_leaking_it(monkeypatch, provider):
    if provider == "openai":
        from genai_client.text_generation.openai_clients.openai_client import (
            OpenAiClient,
        )

        monkeypatch.setattr(OpenAiClient, "_get_client", Mock())
        client = OpenAiClient(is_azure=False, model_name="fixture", api_key="fixture")
    else:
        from genai_client.text_generation.anthropic_client.anthropic_text_client import (
            AnthropicTextClient,
        )

        monkeypatch.setattr(AnthropicTextClient, "_get_client", Mock())
        client = AnthropicTextClient(
            provider="anthropic", model_name="fixture", api_key="fixture"
        )
    client._document_input_processor._convert = Mock(
        return_value=ExtractedDocument("Revenue 42")
    )
    normalized = client._normalize_request_for_batch(
        {
            "custom_id": "document",
            "message_json": json.dumps(history()),
            "_semoss_input_modalities": ["TEXT"],
        },
        0,
    )
    wire = json.dumps(normalized)
    assert "Revenue 42" in wire
    assert "_semoss_input_modalities" not in wire
    assert "file_data" not in wire


def test_native_mime_list_routes_each_document_separately():
    processor = DocumentInputProcessor()
    processor._convert = Mock(return_value=ExtractedDocument("Slide contents"))
    original = messages(
        history()
        + history(
            "deck.pptx",
            mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        )
    )
    config = ModelSettings(
        model_name="pdf-capable", native_document_mime_types=["application/pdf"]
    )
    prepared = processor.prepare(original, config)
    assert prepared[0].parts[1].type == "MEDIA"
    assert prepared[1].parts[1].type == "TEXT"
    processor._convert.assert_called_once()


@pytest.mark.parametrize(
    "filename,mime",
    [("data.csv", "text/csv"), ("notes.md", None), ("config.json", "application/json")],
)
def test_text_attachments_need_no_docling(filename, mime):
    processor = DocumentInputProcessor()
    processor._convert = Mock(side_effect=AssertionError("must not load Docling"))
    prepared = processor.prepare(
        messages(history(filename, "Café,42".encode(), mime)), settings()
    )
    assert "Café,42" in prepared[0].parts[1].text


def test_data_uri_and_missing_mime_are_resolved():
    source = history("deck.pptx", mime=None)
    media = source[0]["parts"][1]["mediaInfo"]
    media["base64Data"] = (
        "data:application/vnd.openxmlformats-officedocument.presentationml.presentation;base64,"
        + media["base64Data"]
    )
    processor = DocumentInputProcessor()
    processor._convert = Mock(return_value=ExtractedDocument("Slides"))
    processor.prepare(messages(source), settings())
    assert processor._convert.call_args.args[1] == "pptx"


def test_old_attachments_are_reprepared_and_cached_within_conversation_only():
    processor = DocumentInputProcessor()
    processor._convert = Mock(return_value=ExtractedDocument("Cached contents"))
    old = messages()
    later = messages(history("data.csv", b"value\n42", "text/csv"))
    first = processor.prepare(old, settings(), cache_scope="room-a")
    second = processor.prepare(old + later, settings(), cache_scope="room-a")
    assert first[0].parts[1].text == second[0].parts[1].text
    assert processor._convert.call_count == 1
    processor.prepare(old, settings(), cache_scope="room-b")
    assert processor._convert.call_count == 2
    processor.prepare(
        messages(history(raw=b"different bytes")), settings(), cache_scope="room-a"
    )
    assert processor._convert.call_count == 3


def test_cache_is_bounded_and_absent_scope_is_not_shared():
    processor = DocumentInputProcessor(max_cache_bytes=8)
    processor._convert = Mock(return_value=ExtractedDocument("12345"))
    for raw in (b"a", b"b", b"a"):
        processor.prepare(messages(history(raw=raw)), settings(), cache_scope="room-a")
    assert processor._convert.call_count == 3
    assert processor._cache_bytes == 5
    assert len(processor._cache) == 1
    for _ in range(2):
        processor.prepare(messages(), settings())
    assert processor._convert.call_count == 5


@pytest.mark.parametrize(
    "raw,mime,filename,match",
    [
        (b"", "text/plain", "empty.txt", "No readable text"),
        (b"\xff", "text/plain", "bad.txt", "UTF-8"),
        (b"binary", "application/zip", "archive.zip", "Cannot extract"),
        (b"binary", "application/msword", "old.doc", "Cannot extract"),
    ],
)
def test_invalid_documents_fail_explicitly(raw, mime, filename, match):
    # Empty attachments are normally rejected before reaching this layer.
    original = messages(history(filename, raw or b" ", mime))
    with pytest.raises(DocumentInputError, match=match):
        DocumentInputProcessor().prepare(original, settings())


def test_invalid_base64_and_file_limits():
    source = history()
    source[0]["parts"][1]["mediaInfo"]["base64Data"] = "!!!!"
    with pytest.raises(DocumentInputError, match="invalid attachment data"):
        DocumentInputProcessor().prepare(messages(source), settings())
    with pytest.raises(DocumentInputError, match="size limit"):
        DocumentInputProcessor(max_file_bytes=2).prepare(messages(), settings())


def test_total_extraction_and_context_limits_do_not_truncate():
    processor = DocumentInputProcessor(max_text_chars=50)
    processor._convert = Mock(return_value=ExtractedDocument("a" * 51))
    with pytest.raises(DocumentInputError, match="extracted-character"):
        processor.prepare(messages(), settings())
    processor = DocumentInputProcessor()
    processor._convert = Mock(return_value=ExtractedDocument("a" * 1000))
    original = messages()
    with pytest.raises(DocumentInputError, match="text budget"):
        processor.prepare(original, settings(context_window=1200, max_tokens=500))
    assert original[0].parts[1].type == "MEDIA"


def test_context_budget_includes_history_system_tools_and_output_override():
    processor = DocumentInputProcessor()
    processor._convert = Mock(return_value=ExtractedDocument("brief text"))
    original = messages()
    original[0].param_map["system_prompt"] = "s" * 1000
    original[0].param_map["tools"] = [{"description": "t" * 1000}]
    with pytest.raises(DocumentInputError, match="text budget"):
        processor.prepare(
            original,
            settings(context_window=3000, global_param_override={"max_tokens": 2000}),
        )


def test_images_pass_through_and_remote_documents_require_upload():
    processor = DocumentInputProcessor()
    processor._convert = Mock(side_effect=AssertionError("must not extract"))
    image = messages(history("photo.png", b"png", "image/png"))
    assert processor.prepare(image, settings())[0].parts[1].type == "MEDIA"
    source = history()
    source[0]["parts"][1]["mediaInfo"] = {
        "fileName": "remote.pdf",
        "sourceUrl": "https://example.invalid/doc.pdf",
    }
    with pytest.raises(DocumentInputError, match="Upload.*remote.pdf"):
        processor.prepare(messages(source), settings())


def test_picture_notice_and_filename_are_in_text():
    processor = DocumentInputProcessor()
    processor._convert = Mock(
        return_value=ExtractedDocument("Readable text", pictures=2)
    )
    prepared = processor.prepare(messages(), settings())
    assert "2 image(s) were not visually analyzed" in prepared[0].parts[1].text


def test_client_setting_is_consumed_and_not_sent_to_sdk_or_provider(monkeypatch):
    from genai_client.text_generation.openai_clients.openai_client import OpenAiClient

    sdk = Mock()
    monkeypatch.setattr(OpenAiClient, "_get_client", sdk)
    client = OpenAiClient(
        is_azure=False,
        model_name="laguna",
        api_key="fixture",
        native_document_mime_types=[],
    )
    assert "native_document_mime_types" not in sdk.call_args.kwargs
    client._document_input_processor._convert = Mock(
        return_value=ExtractedDocument("Revenue is 42")
    )
    source = history()
    prepared = client.build_semoss_messages(
        client.model_settings, message_json=json.dumps(source)
    )
    request = client.message_builder.build_request(prepared)
    assert "Revenue is 42" in json.dumps(request)
    assert "native_document_mime_types" not in request
    assert source[0]["parts"][1]["mediaInfo"]["base64Data"]


@pytest.mark.parametrize("legacy", [False, True])
def test_empty_attachment_cannot_be_silently_dropped_by_message_parser(legacy):
    source = history(raw=b"", legacy=legacy)
    with pytest.raises(ValueError, match="has no readable data|must have either a URL"):
        DocumentInputProcessor().prepare(
            messages(source), settings(), source_messages=source
        )


def test_partial_docling_result_is_never_sent(monkeypatch):
    pytest.importorskip("docling.document_converter")
    from docling.datamodel.base_models import ConversionStatus

    processor = DocumentInputProcessor()
    processor._converter = Mock()
    processor._converter.convert.return_value.status = ConversionStatus.PARTIAL_SUCCESS
    with pytest.raises(DocumentInputError, match="No partial document"):
        processor.prepare(messages(), settings())
