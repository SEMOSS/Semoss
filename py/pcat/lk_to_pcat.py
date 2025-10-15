from loguru import logger

logger.remove()
logger.add("pipecat.log", encoding="utf-8", level="DEBUG")

import logging
import asyncio
import json
from typing import Optional

from pipecat.audio.turn.smart_turn.base_smart_turn import SmartTurnParams
from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.vad.vad_analyzer import VADParams
from pipecat.frames.frames import TranscriptionFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection, Frame
from pipecat.transports.livekit.transport import LiveKitParams, LiveKitTransport
from pipecat.services.openai.stt import OpenAISTTService
from pipecat.frames.frames import TranscriptionFrame, InterimTranscriptionFrame
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection, Frame
from gaas_server_proxy import ServerProxy


class LiveKitDataBridge(FrameProcessor):
    """Bridges Pipecat frames to LiveKit data messages."""

    def __init__(
        self, transport: LiveKitTransport, send_interim: Optional[bool] = True, **kwargs
    ):
        super().__init__(**kwargs)
        self.transport = transport
        self.send_interim = send_interim

        self.logger = logging.getLogger("LiveKitDataBridge")
        self.logger.setLevel(logging.DEBUG)

    async def process_frame(
        self,
        frame: Frame,
        direction: FrameDirection,
        debug_frames: Optional[bool] = False,
    ):
        await super().process_frame(frame, direction)

        if debug_frames:
            self.logger.debug(f"Processing frame: {frame}")

        if isinstance(frame, TranscriptionFrame):
            payload = {
                "type": "transcription",
                "text": frame.text,
                "isFinal": True,
                "userId": getattr(frame, "user_id", None),
                "ts": getattr(frame, "timestamp", None),
            }
            await self.transport.send_message(json.dumps(payload))

        elif self.send_interim and isinstance(frame, InterimTranscriptionFrame):
            payload = {
                "type": "transcription",
                "text": frame.text,
                "isFinal": False,
                "userId": getattr(frame, "user_id", None),
                "ts": getattr(frame, "timestamp", None),
            }
            await self.transport.send_message(json.dumps(payload))

        await self.push_frame(frame, direction)


class TranscriptionLogger(FrameProcessor):
    """Logs transcription frames to a file."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger("TranscriptionLogger")
        self.logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        self.logger.addHandler(console_handler)

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if isinstance(frame, TranscriptionFrame):
            self.logger.info(f"Transcription: {frame.text}")

        await self.push_frame(frame, direction)


class LiveKitToPipecatListener(ServerProxy):
    def __init__(
        self,
        room_name: str,
        jwt: str,
        url: str,
        operation: str,
        model: str,
        model_type: str,
        api_key: str,
        model_url: str,
        insight_id: str,
    ):
        super().__init__()
        self.room_name = room_name
        self.token = jwt
        self.url = url
        self.operation = operation
        self.model = model
        self.model_type = model_type
        self.api_key = api_key
        self.model_url = model_url
        self.insight_id = insight_id

        self.logger = logging.getLogger("LiveKitToPipecatListener")
        self.logger.setLevel(logging.DEBUG)

        self.logger.info(
            f"Initialized LiveKitToPipecatListener for room: {room_name}, url: {url}"
        )

    async def listen_and_transcribe(self):
        self.logger.info(f"Setting up LiveKit Transport")
        transport = LiveKitTransport(
            url=self.url,
            token=self.token,
            room_name=self.room_name,
            params=LiveKitParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                vad_analyzer=SileroVADAnalyzer(params=VADParams(stop_secs=0.2)),
                turn_analyzer=LocalSmartTurnAnalyzerV3(params=SmartTurnParams()),
            ),
        )

        self.logger.info("LiveKit Transport configured")

        self.logger.info("Setting up STT service")

        stt = OpenAISTTService(
            api_key=self.api_key,
            model=self.model,
            prompt="Expect conversational speech with various topics.",
            language="en",
            temperature=0.0,
        )

        self.logger.info("STT service configured")

        self.logger.info("Setting up Pipecat pipeline")

        transcription_logger = TranscriptionLogger()
        lk_bridge = LiveKitDataBridge(transport, send_interim=True)

        pipeline = Pipeline(
            [
                transport.input(),
                stt,
                lk_bridge,
                transcription_logger,
                transport.output(),
            ]
        )

        self.logger.info("Pipeline configured")

        self.logger.info("Starting pipeline task")

        task = PipelineTask(
            pipeline,
            params=PipelineParams(
                enable_metrics=True,
                enable_usage_metrics=True,
            ),
            enable_tracing=True,
        )

        # -------------------START TRANSPORT EVENTS-------------------
        @transport.event_handler("on_connected")
        async def on_connected(_t):
            self.logger.info(
                f"TRANSPORT EVENT: Connected to LiveKit room: {self.room_name}"
            )

        @transport.event_handler("on_disconnected")
        async def on_client_disconnected(transport, client):
            self.logger.info(
                f"TRANSPORT EVENT: Disconnected from LiveKit client: {client.identity}"
            )

        @transport.event_handler("on_participant_connected")
        async def on_participant_connected(participant_id):
            self.logger.info(
                f"TRANSPORT EVENT: Participant connected: {participant_id}"
            )

        @transport.event_handler("on_participant_disconnected")
        async def on_participant_disconnected(participant_id):
            self.logger.info(
                f"TRANSPORT EVENT: Participant disconnected: {participant_id}"
            )

        # -------------------END TRANSPORT EVENTS-------------------

        # -------------------START TASK EVENTS-------------------
        @task.event_handler("on_pipeline_started")
        async def on_pipeline_started(task, frame):
            self.logger.info("TASK EVENT: Pipeline started")

        @task.event_handler("on_pipeline_error")
        async def on_pipeline_error(task, frame):
            self.logger.error(f"TASK EVENT: Pipeline error: {frame}")

        # -------------------END TASK EVENTS-------------------

        runner = PipelineRunner(handle_sigint=False)
        await runner.run(task)


def join_as_listener(
    room_name: str,
    jwt: str,
    url: str,
    operation: str,
    model: str,
    model_type: str,
    api_key: str,
    model_url: str,
    insight_id: str,
):
    """Entry point method that provides a non-blocking background thread listening to room events."""
    import threading

    def background_task():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        listener = LiveKitToPipecatListener(
            room_name=room_name,
            jwt=jwt,
            url=url,
            operation=operation,
            model=model,
            model_type=model_type,
            api_key=api_key,
            model_url=model_url,
            insight_id=insight_id,
        )

        async def run():
            await listener.listen_and_transcribe()

        try:
            loop.run_until_complete(run())
        finally:
            loop.close()

    thread = threading.Thread(
        target=background_task, daemon=True, name=f"LiveKit-{room_name}"
    )
    thread.start()

    return f"SUCCESS: Listener thread started for {room_name}"
