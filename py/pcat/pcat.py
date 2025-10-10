import os
import asyncio
import logging
from typing import Optional

from pipecat.frames.frames import AudioRawFrame, EndFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask, PipelineParams
from pipecat.services.openai.stt import OpenAISTTService
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor


class TranscriptionLogger(FrameProcessor):
    """Simple processor that logs transcriptions"""

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)

        # Log transcription frames
        from pipecat.frames.frames import TranscriptionFrame

        if isinstance(frame, TranscriptionFrame):
            self.logger.info(
                f"[{frame.user_id or 'Unknown'}] Transcription: {frame.text}"
            )

        # Pass frame downstream
        await self.push_frame(frame, direction)


class PipecatTranscriber:
    """
    Standalone Pipecat transcriber that can receive audio from LiveKit
    and output real-time transcriptions using GPT-4o-transcribe.
    """

    def __init__(self, openai_api_key: str, log_directory: Optional[str] = None):
        self.openai_api_key = openai_api_key
        self.log_directory = log_directory

        self._setup_logging()

        self.stt: Optional[OpenAISTTService] = None
        self.pipeline: Optional[Pipeline] = None
        self.task: Optional[PipelineTask] = None
        self.runner: Optional[PipelineRunner] = None
        self._audio_queue: asyncio.Queue = asyncio.Queue()
        self._running = False

    def _setup_logging(self):
        """Setup logging for the transcriber"""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        if self.log_directory:
            log_file = os.path.join(self.log_directory, "pipecat_transcriber.log")
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        self.logger.addHandler(console_handler)

    async def start(self):
        """Initialize and start the Pipecat pipeline"""
        self.logger.info("Starting Pipecat transcriber...")

        # Initialize STT service with GPT-4o-transcribe
        self.stt = OpenAISTTService(
            api_key=self.openai_api_key,
            model="gpt-4o-transcribe",
            prompt="Expect conversational speech with various topics.",
        )

        # Create a simple pipeline: STT -> TranscriptionLogger
        transcription_logger = TranscriptionLogger()

        self.pipeline = Pipeline([self.stt, transcription_logger])

        # Create pipeline task
        self.task = PipelineTask(
            self.pipeline,
            params=PipelineParams(
                audio_in_sample_rate=48000,  # Match LiveKit's sample rate
                enable_metrics=True,
            ),
        )

        # Create and start runner
        self.runner = PipelineRunner(handle_sigint=False)

        self._running = True

        # Start the pipeline in the background
        asyncio.create_task(self._run_pipeline())

        # Start audio feeding task
        asyncio.create_task(self._feed_audio())

        self.logger.info("Pipecat transcriber started successfully")

    async def _run_pipeline(self):
        """Run the pipeline"""
        try:
            await self.runner.run(self.task)
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}", exc_info=True)
            self._running = False

    async def _feed_audio(self):
        """Feed audio frames from queue into pipeline"""
        while self._running:
            try:
                audio_data = await asyncio.wait_for(
                    self._audio_queue.get(), timeout=1.0
                )

                if audio_data is None:  # Shutdown signal
                    break

                pcm_bytes, sample_rate, num_channels, user_id = audio_data

                # Create AudioRawFrame for Pipecat
                frame = AudioRawFrame(
                    audio=pcm_bytes, sample_rate=sample_rate, num_channels=num_channels
                )

                # Set user_id if available (for multi-participant tracking)
                if user_id:
                    frame.user_id = user_id

                # Queue frame to pipeline
                await self.task.queue_frame(frame)

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error feeding audio: {e}", exc_info=True)

    async def process_audio(
        self, pcm_bytes: bytes, sample_rate: int, num_channels: int, user_id: str
    ):
        """
        Process audio from LiveKit.
        This method should be called from your LiveKit on_pcm callback.

        Args:
            pcm_bytes: Raw PCM audio data
            sample_rate: Sample rate (e.g., 48000)
            num_channels: Number of channels (e.g., 1 for mono)
            user_id: Identifier for the speaker
        """
        if not self._running:
            self.logger.warning("Transcriber not running, ignoring audio")
            return

        # Queue audio for processing
        await self._audio_queue.put((pcm_bytes, sample_rate, num_channels, user_id))

    async def stop(self):
        """Stop the transcriber"""
        self.logger.info("Stopping Pipecat transcriber...")
        self._running = False

        # Send shutdown signal
        await self._audio_queue.put(None)

        # Send EndFrame to pipeline
        if self.task:
            await self.task.queue_frame(EndFrame())
            await self.task.cancel()

        self.logger.info("Pipecat transcriber stopped")


# Example usage integrated with your LiveKit listener
async def example_with_livekit():
    """
    Example of how to integrate with your LiveKitClient
    """
    # Initialize transcriber
    transcriber = PipecatTranscriber(
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        insight_id="your_insight_id",
        log_directory="./logs",
    )

    await transcriber.start()

    # Modify your LiveKitClient to use this callback:
    async def on_pcm_callback(pcm_bytes, sample_rate, channels, who):
        await transcriber.process_audio(pcm_bytes, sample_rate, channels, who)

    # Pass on_pcm_callback to your LiveKitClient
    # listener = LiveKitClient(
    #     room_name=room_name,
    #     jwt=jwt,
    #     url=url,
    #     insight_id=insight_id,
    #     on_pcm=on_pcm_callback
    # )

    # Keep running
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await transcriber.stop()


if __name__ == "__main__":
    asyncio.run(example_with_livekit())
