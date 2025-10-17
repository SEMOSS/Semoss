import logging, inspect, asyncio
from typing import Callable, Optional
from livekit.rtc import Room
from livekit.rtc.audio_stream import AudioStream
from livekit.rtc.track import Track
from gaas_server_proxy import ServerProxy
from audio.openai_realtime import OpenAIRealTimeClient

BYTES_PER_100MS_48K_MONO_PCM16 = 9600


def make_on_pcm(
    rt_client: OpenAIRealTimeClient,
    logger: logging.Logger,
    threshold_bytes: int = BYTES_PER_100MS_48K_MONO_PCM16,
    commit_every: int = 3,
):
    """
    Returns a function matching on_pcm signature that:
    - accumulates PCM bytes
    - sends input_audio_buffer.append in fixed-size chunks
    - commits every `commit_every` chunks (and requests a transcript)
    """
    buf = bytearray()
    chunks_since_commit = 0

    def on_pcm(
        pcm_bytes,
        sample_rate,
        channels,
        who,
        operation: str,
        model: str,
        api_key: str,
        _logger: logging.Logger,
    ):
        nonlocal chunks_since_commit
        if operation != "real_time_transcription":
            return

        buf.extend(pcm_bytes)

        while len(buf) >= threshold_bytes:
            slice_bytes = bytes(buf[:threshold_bytes])
            del buf[:threshold_bytes]
            rt_client.append_pcm16(slice_bytes)
            chunks_since_commit += 1

            if chunks_since_commit >= commit_every:
                rt_client.commit_audio()
                rt_client.request_transcript()
                chunks_since_commit = 0

    return on_pcm


class LiveKitClient(ServerProxy):
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
        on_pcm: Optional[Callable[[bytes, int, int, str], None]] = None,
    ):
        super().__init__()
        self.on_pcm = on_pcm
        self.room_name = room_name
        self.token = jwt
        self.url = url
        self.insight_id = insight_id
        self.operation = operation
        self.model = model
        self.model_type = model_type
        self.api_key = api_key
        self.model_url = model_url

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
                    res = self.on_pcm(
                        frame.data,
                        frame.sample_rate,
                        frame.num_channels,
                        who,
                        self.operation,
                        self.model,
                        self.api_key,
                        self.logger,
                    )
                    if inspect.isawaitable(res):
                        await res
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
    """Non-blocking version that runs in background"""
    import threading

    def background_task():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # 1) create ONE realtime client and connect it
        rt_client = OpenAIRealTimeClient(
            model=model, api_key=api_key, logger=logging.getLogger(__name__)
        )
        rt_client.connect()

        # 2) bind a buffered on_pcm that uses that client
        on_pcm = make_on_pcm(
            rt_client=rt_client,
            logger=rt_client.logger,
            threshold_bytes=BYTES_PER_100MS_48K_MONO_PCM16,
            commit_every=3,
        )

        listener = LiveKitClient(
            room_name=room_name,
            jwt=jwt,
            url=url,
            insight_id=insight_id,
            operation=operation.lower(),
            model=model.lower(),
            model_type=model_type.lower(),
            api_key=api_key,
            model_url=model_url,
            on_pcm=on_pcm,
        )

        async def run():
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
