"""Veo request construction at the provider boundary; no model calls."""

import base64
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from google.genai.types import VideoGenerationReferenceType

from genai_client.message_builders.semoss_base.semoss_message_builder import (
    SEMOSSMessageBuilder,
)
from genai_client.message_builders.semoss_base.semoss_models import ModelSettings
from genai_client.text_generation.google_genai_clients.google_video_client import (
    GoogleGenAiVideoClient,
)


PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jP1sAAAAASUVORK5CYII="
)


def message(*attachments):
    return {
        "type": "INPUT_MEDIA" if attachments else "INPUT_TEXT",
        "schemaVersion": 2,
        "io": "INPUT",
        "parts": [
            {"type": "TEXT", "text": "Animate these references."},
            *[
                {
                    "type": "MEDIA",
                    "mediaInfo": {
                        "fileName": name,
                        "mimeType": mime,
                        "base64Data": base64.b64encode(data).decode(),
                    },
                }
                for name, mime, data in attachments
            ],
        ],
    }


@pytest.fixture
def client():
    generate = Mock(
        return_value=SimpleNamespace(
            done=True,
            error=None,
            response=SimpleNamespace(
                generated_videos=[
                    SimpleNamespace(
                        video=SimpleNamespace(
                            video_bytes=b"video fixture", mime_type="video/mp4"
                        )
                    )
                ]
            ),
        )
    )
    return GoogleGenAiVideoClient(
        SimpleNamespace(
            model_settings=ModelSettings(
                model_name="veo-3.1-generate-001", model_type="video"
            ),
            google_client=SimpleNamespace(
                models=SimpleNamespace(generate_videos=generate)
            ),
        )
    )


def request(client, source, params):
    messages = SEMOSSMessageBuilder().build_messages(
        source, params, client.parent_client.model_settings
    )
    before = [item.model_dump() for item in messages]
    response = client.ask_call(messages)
    generate = client.parent_client.google_client.models.generate_videos
    generate.assert_called_once()
    assert [item.model_dump() for item in messages] == before
    assert response.parts[0]["media_info"]["base64Data"] == base64.b64encode(
        b"video fixture"
    ).decode()
    return generate.call_args.kwargs


@pytest.mark.parametrize("count", [1, 3])
def test_uploaded_images_reach_veo_as_assets_with_original_bytes(client, count):
    attachments = [(f"reference {i}.png", "image/png", PNG) for i in range(count)]
    wire = request(client, [message(*attachments)], {"durationSeconds": "8"})
    assert wire["model"] == "veo-3.1-generate-001"
    assert wire["prompt"] == "Animate these references."
    references = wire["config"].reference_images
    assert len(references) == count
    for reference in references:
        assert reference.reference_type == VideoGenerationReferenceType.ASSET
        assert reference.image.image_bytes == PNG
        assert reference.image.mime_type == "image/png"


@pytest.mark.parametrize(
    "params",
    [
        {"aspectRatio": "9:16", "durationSeconds": "8", "resolution": "1080p"},
        {"aspect_ratio": "9:16", "duration_seconds": 8, "resolution": "1080p"},
    ],
)
def test_video_settings_accept_api_aliases_and_sdk_names(client, params):
    wire = request(client, [message()], {**params, "use_history": True})
    assert wire["config"].aspect_ratio == "9:16"
    assert wire["config"].duration_seconds == 8
    assert wire["config"].resolution == "1080p"
    assert "use_history" not in wire["config"].model_dump()


def test_sdk_name_takes_precedence_when_both_names_are_supplied(client):
    wire = request(
        client,
        [message()],
        {"durationSeconds": "4", "duration_seconds": 8},
    )
    assert wire["config"].duration_seconds == 8


def test_text_only_request_does_not_reuse_previous_reference_images(client):
    wire = request(
        client,
        [message(("previous.png", "image/png", PNG)), message()],
        {"durationSeconds": "4"},
    )
    assert not wire["config"].reference_images
    assert wire["config"].duration_seconds == 4


def test_other_media_is_not_sent_as_a_reference_image(client):
    wire = request(
        client,
        [message(("sound.wav", "audio/wav", b"audio"), ("ref.png", "image/png", PNG))],
        {"durationSeconds": 8},
    )
    assert len(wire["config"].reference_images) == 1
    assert wire["config"].reference_images[0].image.image_bytes == PNG


def test_uploaded_images_replace_explicit_reference_config_without_duplicate_keys(client):
    wire = request(
        client,
        [message(("ref.png", "image/png", PNG))],
        {"referenceImages": [], "durationSeconds": 8},
    )
    assert len(wire["config"].reference_images) == 1
