import asyncio
import hashlib
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import audio_compat


class AudioCompatTests(unittest.TestCase):
    def source(self):
        return (
            "from pipecat.processors.transcript_processor import TranscriptProcessor\n"
            "async def pipeline():\n"
            "        transcript = TranscriptProcessor()\n"
            "        stages = [\n"
            "                transcript.user(),\n"
            "                transcript.assistant(),\n"
            "        ]\n" + audio_compat.OLD_HANDLER
        ).encode()

    def test_patch_requires_exact_source_and_records_hashes(self):
        original = self.source()
        path = Mock()
        path.read_bytes.return_value = original
        digest = hashlib.sha256(original).hexdigest()
        with patch.object(audio_compat, "EXPECTED_SHA256", digest):
            report = audio_compat.patch_audio(path)
        updated = path.write_bytes.call_args.args[0]
        self.assertNotIn(b"TranscriptProcessor", updated)
        self.assertNotIn(b"transcript.user()", updated)
        self.assertEqual(report["original_sha256"], digest)
        self.assertEqual(report["patched_sha256"], hashlib.sha256(updated).hexdigest())
        with self.assertRaisesRegex(ValueError, "source hash"):
            audio_compat.patch_audio(path)

    def test_patch_rejects_missing_or_duplicate_targets(self):
        for original in (b"not the expected source", self.source() + self.source()):
            with self.subTest(original=original[:10]):
                path = Mock()
                path.read_bytes.return_value = original
                with patch.object(audio_compat, "EXPECTED_SHA256", hashlib.sha256(original).hexdigest()):
                    with self.assertRaisesRegex(ValueError, "exactly once"):
                        audio_compat.patch_audio(path)
                path.write_bytes.assert_not_called()

    def test_turn_events_preserve_client_payload_and_surface_transport_errors(self):
        async def exercise():
            events = {}

            def event_handler(name):
                def register(handler):
                    events[name] = handler
                    return handler
                return register

            transport = SimpleNamespace(send_message=AsyncMock())
            namespace = {"json": json}
            exec("async def register(user_agg, assistant_agg, transport):\n"
                 + audio_compat.NEW_HANDLER, namespace)
            aggregator = SimpleNamespace(event_handler=event_handler)
            await namespace["register"](aggregator, aggregator, transport)
            message = SimpleNamespace(content="hello", timestamp="2026-01-01T00:00:00Z")
            await events["on_user_turn_stopped"](aggregator, None, message)
            await events["on_assistant_turn_stopped"](aggregator, message)
            for call, role in zip(transport.send_message.call_args_list, ("user", "assistant")):
                self.assertEqual(json.loads(call.args[0]), {
                    "type": "transcription", "role": role, "text": "hello",
                    "ts": message.timestamp, "isFinal": True,
                })
            await events["on_assistant_turn_stopped"](
                aggregator, SimpleNamespace(content="", timestamp=message.timestamp))
            self.assertEqual(transport.send_message.call_count, 2)
            transport.send_message.side_effect = RuntimeError("transport failed")
            with self.assertRaisesRegex(RuntimeError, "transport failed"):
                await events["on_user_turn_stopped"](aggregator, None, message)

        asyncio.run(exercise())
