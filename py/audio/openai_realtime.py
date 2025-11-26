import json
import websocket
import logging
import threading
import base64


class OpenAIRealTimeClient:
    def __init__(self, model: str, api_key: str, logger: logging.Logger):
        self.logger = logger

        self.url = "wss://api.openai.com/v1/realtime?model=" + model
        self.headers = ["Authorization: Bearer " + api_key]

    def connect(self):
        self.ws = websocket.WebSocketApp(
            self.url,
            header=self.headers,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        t = threading.Thread(target=self.ws.run_forever, daemon=True)
        t.start()

    # --- event helpers ---
    def send_event(self, event: dict):
        if self.ws:
            self.ws.send(json.dumps(event))

    def send_session_update(self, sr=48000, ch=1):
        self.send_event(
            {
                "type": "session.update",
                "session": {
                    "model": "gpt-realtime",
                    "input_audio_format": {
                        "type": "pcm16",
                        "sample_rate_hz": sr,
                        "channels": ch,
                    },
                    "input_audio_transcription": {"enabled": True, "language": "en"},
                    "turn_detection": {
                        "type": "server_vad",
                        "silence_duration_ms": 200,
                    },
                },
            }
        )

    def append_pcm16(self, pcm_bytes: bytes):
        self.send_event(
            {
                "type": "input_audio_buffer.append",
                "audio": base64.b64encode(pcm_bytes).decode("ascii"),
            }
        )

    def commit_audio(self):
        self.send_event({"type": "input_audio_buffer.commit"})

    def request_transcript(self):
        self.send_event(
            {
                "type": "response.create",
                "response": {
                    "modalities": ["text"],
                    "instructions": "Transcribe the latest user audio.",
                },
            }
        )

    # --- ws callbacks ---
    def on_open(self, ws):
        self.logger.info("Realtime WS opened")
        self.send_session_update()

    def on_message(self, ws, message):
        self.logger.info(f"Realtime event: {message}")

    def on_error(self, ws, error):
        self.logger.error(f"Realtime WS error: {error}")

    def on_close(self, ws, code, msg):
        self.logger.info(f"Realtime WS closed {code} {msg}")
