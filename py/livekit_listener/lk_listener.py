import os
import logging
import inspect
import asyncio
from typing import Callable, Optional
from livekit.rtc import Room
from livekit.rtc.audio_stream import AudioStream
from livekit.rtc.track import Track
from livekit.api.access_token import AccessToken, VideoGrants


class LiveKitClient:
    def __init__(
        self,
        room_name: str,
        on_pcm: Optional[Callable[[bytes, int, int, str], None]] = None,
    ):

        self.on_pcm = on_pcm
        self.room_name = room_name
        self.token = self.build_token(room_name)

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.url = os.getenv("LIVEKIT_URL")
        if not self.url:
            raise RuntimeError("LIVEKIT_URL is not set; expected wss://your-server")

        self.room = Room()
        self._tasks: set[asyncio.Task] = set()

    def build_token(self, room_name: str) -> str:
        api_key = os.getenv("LIVEKIT_API_KEY")
        api_secret = os.getenv("LIVEKIT_API_SECRET")

        if not api_key or not api_secret:
            raise RuntimeError(
                "LIVEKIT_API_KEY or LIVEKIT_API_SECRET is not set; cannot connect"
            )

        video_grants = VideoGrants(room_join=True, room=room_name)
        identity = "python_listener"
        token = (
            AccessToken(api_key, api_secret)
            .with_identity(identity)
            .with_grants(video_grants)
        )

        return token.to_jwt()

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


def join_as_listener(room_name: str):
    """Non-blocking version that runs in background"""
    import threading

    def background_task():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        listener = LiveKitClient(room_name=room_name, on_pcm=handle_pcm)

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
