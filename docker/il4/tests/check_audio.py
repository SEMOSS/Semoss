"""Offline integration tests against the installed, patched SEMOSS audio module."""
import asyncio
import json
import sys
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, "/opt/semosshome/py")

from audio import lk_to_pcat as audio
from pipecat.processors.aggregators.llm_response_universal import (
    AssistantTurnStoppedMessage, LLMAssistantAggregator,
    LLMUserAggregator, UserTurnStoppedMessage,
)


class AudioIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_pipeline_construction_and_transcript_delivery(self):
        for method in ("speech_to_speech_realtime", "listen_and_transcribe",
                       "listen_translate_and_transcribe"):
            with self.subTest(method=method):
                pipelines, transports = [], []
                pipeline_class, transport_class = audio.Pipeline, audio.LiveKitTransport

                def make_pipeline(*args, **kwargs):
                    pipeline = pipeline_class(*args, **kwargs)
                    pipelines.append(pipeline)
                    return pipeline

                def make_transport(*args, **kwargs):
                    transport = transport_class(*args, **kwargs)
                    transport.send_message = AsyncMock()
                    transports.append(transport)
                    return transport

                with patch.object(audio, "Pipeline", side_effect=make_pipeline), \
                        patch.object(audio, "LiveKitTransport", side_effect=make_transport), \
                        patch.object(audio, "PipelineRunner") as runner, \
                        patch.object(audio.ServerProxy, "__init__", return_value=None), \
                        patch.object(audio.boto3, "client", return_value=Mock()):
                    runner.return_value.run = AsyncMock()
                    listener = audio.LiveKitToPipecatListener(
                        room_name="test-room", jwt="test-token", url="wss://example.invalid",
                        operation=method, model="test-model", model_type="test",
                        api_key="test-key", model_url="", insight_id="test-insight",
                        param_map={"aws_access_key": "test-key", "aws_secret_key": "test-key"},
                    )
                    try:
                        await getattr(listener, method)()
                        runner.return_value.run.assert_awaited_once()
                        self.assertEqual(len(pipelines), 1)
                        if method == "speech_to_speech_realtime":
                            await self.assert_transcripts(pipelines[0], transports[0])
                    finally:
                        for pipeline in pipelines:
                            await pipeline.cleanup()

    async def assert_transcripts(self, pipeline, transport):
        user = next(p for p in pipeline.processors if isinstance(p, LLMUserAggregator))
        assistant = next(p for p in pipeline.processors if isinstance(p, LLMAssistantAggregator))
        delivered = asyncio.Event()
        messages = []

        async def send(message):
            messages.append(json.loads(message))
            if len(messages) == 2:
                delivered.set()

        transport.send_message.side_effect = send
        timestamp = "2026-01-01T00:00:00Z"
        await user._call_event_handler(
            "on_user_turn_stopped", None, UserTurnStoppedMessage("hello", timestamp))
        await assistant._call_event_handler(
            "on_assistant_turn_stopped", AssistantTurnStoppedMessage("partial reply", True, timestamp))
        await asyncio.wait_for(delivered.wait(), timeout=5)
        self.assertCountEqual(messages, [
            {"type": "transcription", "role": role, "text": text,
             "ts": timestamp, "isFinal": True}
            for role, text in (("user", "hello"), ("assistant", "partial reply"))
        ])


if __name__ == "__main__":
    unittest.main()
