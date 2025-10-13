from loguru import logger

logger.remove()
logger.add("pipecat.log", encoding="utf-8", level="INFO")
from pcat.pcat import PipecatTranscriber

import logging
import inspect
import asyncio
from typing import Callable, Optional
from livekit.rtc import Room
from livekit.rtc.audio_stream import AudioStream
from livekit.rtc.track import Track
from gaas_server_proxy import ServerProxy

_transcriber = None


class LiveKitClient(ServerProxy):
    def __init__(
        self,
        room_name: str,
        jwt: str,
        url: str,
        insight_id: str,
        on_pcm: Optional[Callable[[bytes, int, int, str], None]] = None,
    ):
        super().__init__()
        self.on_pcm = on_pcm
        self.room_name = room_name
        self.token = jwt
        self.url = url
        self.insight_id = insight_id

        self._setup_logging()

        self.logger.info(
            f"This is a log for a LiveKit Listener in room: {self.room_name} in insight: {self.insight_id}"
        )

        self.room = Room()
        self._tasks: set[asyncio.Task] = set()

    def _setup_logging(self):
        """
        Setup class logging and livekit logging to a specific livekit file in the insight cache directory
        """
        log_file = self.server.insight_folder + f"\\livekit_client.log"

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.logger.propagate = False

        livekit_loggers = [
            logging.getLogger("livekit"),
            logging.getLogger("livekit.rtc"),
            logging.getLogger("livekit.rtc.room"),
            logging.getLogger("livekit.rtc.track"),
        ]

        for lk_logger in livekit_loggers:
            lk_logger.setLevel(logging.INFO)
            lk_logger.handlers.clear()
            lk_logger.addHandler(file_handler)
            lk_logger.addHandler(console_handler)
            lk_logger.propagate = False

    async def connect(self):

        @self.room.on("track_subscribed")
        def _on_track_subscribed(track: Track, pub, participant):
            if pub.kind != 1:
                return
            self.logger.info(
                f"Subscribed to AUDIO track {track.sid} from {participant.identity}"
            )
            self._tasks.add(
                asyncio.create_task(self._consume_audio(track, participant.identity))
            )

        @self.room.on("connected")
        def _on_connected():
            self.logger.info(f"Connected to LiveKit room: {self.room.name}")
            self._tasks.add(asyncio.create_task(self._attach_existing_audio_tracks()))

        @self.room.on("disconnected")
        def _on_disconnected():
            self.logger.info("Disconnected from room")

        await self.room.connect(self.url, self.token)

    async def _attach_existing_audio_tracks(self):
        for rp in self.room.remote_participants.values():
            for pub in rp.track_publications.values():
                if pub.track:
                    self._tasks.add(
                        asyncio.create_task(self._consume_audio(pub.track, rp.identity))
                    )

    async def _consume_audio(self, track: Track, who: str):
        stream = AudioStream.from_track(track=track, sample_rate=48000, num_channels=1)
        self.logger.info(f"[{who}] audio stream started")
        try:
            async for ev in stream:
                frame = ev.frame
                if self.on_pcm:
                    result = self.on_pcm(
                        frame.data, frame.sample_rate, frame.num_channels, who
                    )
                    if inspect.isawaitable(result):
                        await result
        finally:
            await stream.aclose()
            self.logger.info(f"[{who}] audio stream closed")

    async def run_forever(self):
        try:
            while True:
                await asyncio.sleep(1)
        finally:
            await self.close()

    async def close(self):
        for t in list(self._tasks):
            t.cancel()
        await self.room.disconnect()


async def handle_pcm(pcm_bytes, sample_rate, channels, who):
    print(
        f"Received {len(pcm_bytes)} bytes of PCM from {who} @ {sample_rate}Hz x{channels}"
    )
    if _transcriber:
        await _transcriber.process_audio(pcm_bytes, sample_rate, channels, who)
    else:
        print("Transcriber not initialized yet")


def join_as_listener(room_name: str, jwt: str, url: str, insight_id: str):
    """Non-blocking version that runs in background"""
    import threading

    def background_task():
        global _transcriber
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        _transcriber = PipecatTranscriber(
            openai_api_key="...",
            log_directory="C:\\workspace\\Semoss\\py\\pcat\\",
        )

        listener = LiveKitClient(
            room_name=room_name,
            jwt=jwt,
            url=url,
            insight_id=insight_id,
            on_pcm=handle_pcm,
        )

        async def run():
            await _transcriber.start()

            await listener.connect()
            await listener.run_forever()

        try:
            loop.run_until_complete(run())
        finally:
            loop.close()

    thread = threading.Thread(
        target=background_task, daemon=True, name=f"LiveKit-{room_name}"
    )
    thread.start()

    return f"SUCCESS: Listener thread started for {room_name}"
