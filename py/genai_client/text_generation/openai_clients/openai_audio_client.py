import base64
import uuid
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict
from pydantic_core import ErrorDetails

from ...constants import AskModelEngineResponse2
from ...message_builders.semoss_base.semoss_models import (
    SEMOSSMediaContent,
    SEMOSSMediaInputType,
    SEMOSSMessage,
    SEMOSSMessagePartType,
)
from ...utils import StringEnum
from ..model_engine_exception import ModelEngineException


class ResponseFormat(StringEnum):
    MP3 = "mp3"
    OPUS = "opus"
    AAC = "aac"
    FLAC = "flac"
    WAV = "wav"
    PCM = "pcm"


class TranscriptionResponseFormat(StringEnum):
    JSON = "json"
    TEXT = "text"
    SRT = "srt"
    VERBOSE_JSON = "verbose_json"
    VTT = "vtt"


_FORMAT_TO_MIME = {
    "mp3": "audio/mpeg",
    "mpeg": "audio/mpeg",
    "mpga": "audio/mpeg",
    "opus": "audio/ogg",
    "aac": "audio/aac",
    "flac": "audio/flac",
    "wav": "audio/wav",
    "pcm": "audio/L16",
    "webm": "audio/webm",
    "m4a": "audio/mp4",
    "mp4": "audio/mp4",
    "ogg": "audio/ogg",
    "oga": "audio/ogg",
}

_RESERVED_KWARGS = {
    "message_json",
    "tools",
    "schema",
    "full_prompt",
    "prefix",
    "template",
    "template_name",
}


class TTSConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    input: str
    model: str
    voice: str
    instructions: Optional[str] = None
    response_format: Optional[str] = None


class STTConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    model: str
    language: Optional[str] = None
    prompt: Optional[str] = None
    response_format: Optional[str] = None
    temperature: Optional[float] = None


class OpenAiAudioClient:

    def __init__(self, client):
        self.client = client

    def ask(
        self,
        semoss_messages: List[SEMOSSMessage],
        **kwargs,
    ) -> AskModelEngineResponse2 | ErrorDetails:
        try:
            kwargs = self._clean_kwargs(kwargs)
            action = self._resolve_audio_action(kwargs.pop("audio_action", None))
            if action == "speech":
                return self._do_tts(semoss_messages, **kwargs)
            return self._do_stt(semoss_messages, **kwargs)
        except Exception as e:
            return ModelEngineException(
                error=e, client="openai", model=self.client.model_name
            ).parse_error()

    @staticmethod
    def _clean_kwargs(kwargs: dict) -> dict:
        """Drop SEMOSS plumbing kwargs so only OpenAI-audio params remain."""
        return {k: v for k, v in kwargs.items() if k not in _RESERVED_KWARGS}

    def _resolve_audio_action(self, explicit: Optional[str]) -> str:
        if explicit is None:
            overrides = (
                getattr(self.client.model_settings, "global_param_override", None) or {}
            )
            explicit = overrides.get("audio_action")

        if explicit:
            if explicit not in ("speech", "transcribe"):
                raise ValueError(
                    f"Unsupported audio_action '{explicit}'. Use 'speech' or 'transcribe'."
                )
            return explicit

        name = (self.client.model_name or "").lower()
        if "tts" in name:
            return "speech"
        if "whisper" in name or "transcribe" in name:
            return "transcribe"

        raise ValueError(
            f"Cannot infer audio action from model '{self.client.model_name}'. "
            "Pass audio_action='speech' or 'transcribe'."
        )

    # -------------------- TTS --------------------

    def _do_tts(
        self, semoss_messages: List[SEMOSSMessage], **kwargs
    ) -> AskModelEngineResponse2:
        text = self._extract_last_input_text(semoss_messages)
        if not text:
            raise ValueError("No text prompt found in the input messages.")

        merged = self._merge_with_overrides(kwargs)
        stream = merged.pop("stream", True)
        audio_config = self._create_tts_config(text, **merged)
        return self._generate_audio(audio_config, stream=stream)

    def _create_tts_config(self, text: str, **kwargs) -> TTSConfig:
        voice = kwargs.pop("voice", "alloy")
        instructions = kwargs.pop("instructions", None)
        response_format = kwargs.pop("response_format", ResponseFormat.MP3.value)
        if hasattr(response_format, "value"):
            response_format = response_format.value
        if response_format not in ResponseFormat.values():
            response_format = ResponseFormat.MP3.value

        return TTSConfig(
            input=text,
            model=self.client.model_name,
            voice=voice,
            instructions=instructions,
            response_format=response_format,
            **kwargs,
        )

    def _generate_audio(
        self, audio_config: TTSConfig, stream: bool = True
    ) -> AskModelEngineResponse2:
        if stream:
            with self.client.client.audio.speech.with_streaming_response.create(
                **audio_config.model_dump(exclude_none=True)
            ) as response:
                audio_bytes = response.read()
        else:
            response = self.client.client.audio.speech.create(
                **audio_config.model_dump(exclude_none=True)
            )
            audio_bytes = getattr(response, "content", b"") or b""

        audio_b64 = base64.b64encode(audio_bytes).decode("ascii")
        file_format = audio_config.response_format or ResponseFormat.MP3.value
        mime_type = _FORMAT_TO_MIME.get(file_format, "application/octet-stream")
        media_info = self._create_media_info(
            mime_type=mime_type, file_format=file_format, base64_data=audio_b64
        )

        return AskModelEngineResponse2(
            response="",
            prompt_tokens=0,
            response_tokens=0,
            schemaVersion=2,
            io="OUTPUT",
            messageType="CHAT",
            parts=[{"type": "MEDIA", "media_info": media_info}],
        )

    # -------------------- STT --------------------

    def _do_stt(
        self, semoss_messages: List[SEMOSSMessage], **kwargs
    ) -> AskModelEngineResponse2:
        merged = self._merge_with_overrides(kwargs)
        audio_format = (merged.pop("audio_format", None) or "mp3").lower().lstrip(".")

        b64_data, file_format, file_name = self._resolve_stt_audio(
            semoss_messages, audio_format
        )

        b64_data = self._sanitize_base64(b64_data)
        try:
            audio_bytes = base64.b64decode(b64_data, validate=True)
        except Exception as e:
            raise ValueError(f"Failed to decode base64 audio data: {e}") from e

        upload_name = file_name or f"audio.{file_format}"
        mime_type = _FORMAT_TO_MIME.get(file_format, "application/octet-stream")
        file_tuple = (upload_name, audio_bytes, mime_type)

        stt_config = self._create_stt_config(**merged)
        return self._generate_transcription(file_tuple, stt_config)

    @staticmethod
    def _sanitize_base64(s: str) -> str:
        """
        Reverse common transit corruptions of base64 strings:
          - strip a `data:<mime>;base64,` prefix if present
          - restore `+` chars converted to spaces by Java's URLDecoder
          - drop whitespace / newlines
          - re-pad to a multiple of 4 with `=`
        """
        if not s:
            return s
        s = s.strip()
        comma_idx = s.find(",")
        if s[:5].lower() == "data:" and comma_idx != -1:
            s = s[comma_idx + 1 :]
        s = s.replace(" ", "+")
        s = "".join(s.split())
        pad = (-len(s)) % 4
        if pad:
            s += "=" * pad
        return s

    def _resolve_stt_audio(
        self, semoss_messages: List[SEMOSSMessage], default_format: str
    ) -> tuple:
        """
        Pull base64 audio from inputs. Prefers a MEDIA part (proper structured
        input); falls back to the last INPUT text, treated as raw base64 — that
        way callers can just pass the base64 string in `command` from the pixel
        without any Java-side plumbing for media.
        """
        media = self._extract_last_input_audio(semoss_messages)
        if media is not None:
            if media.type != SEMOSSMediaInputType.BASE64:
                raise ValueError(
                    "Only base64-encoded audio is supported for transcription in v1. "
                    "URL input is not yet supported."
                )
            if not media.data:
                raise ValueError("Audio media has no base64 data.")
            file_format = (media.format or default_format).lower().lstrip(".")
            return media.data, file_format, media.file_name

        text = self._extract_last_input_text(semoss_messages)
        if text:
            return text.strip(), default_format, None

        raise ValueError(
            "No audio input found. Pass base64 audio as the command, or supply "
            "an audio MEDIA part."
        )

    def _create_stt_config(self, **kwargs) -> STTConfig:
        response_format = kwargs.pop(
            "response_format", TranscriptionResponseFormat.JSON.value
        )
        if hasattr(response_format, "value"):
            response_format = response_format.value
        if response_format not in TranscriptionResponseFormat.values():
            response_format = TranscriptionResponseFormat.JSON.value

        return STTConfig(
            model=self.client.model_name,
            language=kwargs.pop("language", None),
            prompt=kwargs.pop("prompt", None),
            response_format=response_format,
            temperature=kwargs.pop("temperature", None),
            **kwargs,
        )

    def _generate_transcription(
        self, file_tuple: tuple, stt_config: STTConfig
    ) -> AskModelEngineResponse2:
        response = self.client.client.audio.transcriptions.create(
            file=file_tuple,
            **stt_config.model_dump(exclude_none=True),
        )

        if isinstance(response, str):
            text = response
        else:
            text = getattr(response, "text", None) or str(response)

        return AskModelEngineResponse2(
            response=text,
            prompt_tokens=0,
            response_tokens=0,
            schemaVersion=2,
            io="OUTPUT",
            messageType="CHAT",
            parts=[{"type": "TEXT", "text": text}] if text else [],
        )

    # -------------------- Helpers --------------------

    def _merge_with_overrides(self, kwargs: dict) -> dict:
        """Merge SMSS-level global_param_override as defaults under per-call kwargs."""
        overrides = (
            getattr(self.client.model_settings, "global_param_override", None) or {}
        )
        merged = {k: v for k, v in overrides.items() if k != "audio_action"}
        merged.update(kwargs)
        return merged

    @staticmethod
    def _extract_last_input_text(semoss_messages: List[SEMOSSMessage]) -> Optional[str]:
        for msg in reversed(semoss_messages):
            if getattr(msg, "io", None) != "INPUT":
                continue
            parts = getattr(msg, "parts", None)
            if parts:
                for part in reversed(parts):
                    if getattr(part, "type", None) == SEMOSSMessagePartType.TEXT:
                        return part.text
            content = getattr(msg, "content", None)
            if content:
                return content
        return None

    @staticmethod
    def _extract_last_input_audio(
        semoss_messages: List[SEMOSSMessage],
    ) -> Optional[SEMOSSMediaContent]:
        for msg in reversed(semoss_messages):
            if getattr(msg, "io", None) != "INPUT":
                continue
            parts = getattr(msg, "parts", None)
            if parts:
                for part in reversed(parts):
                    if getattr(part, "type", None) != SEMOSSMessagePartType.MEDIA:
                        continue
                    media = getattr(part, "media_info", None)
                    if media and (media.mime_type or "").startswith("audio"):
                        return media
            media_content = getattr(msg, "media_content", None)
            if media_content:
                for media in reversed(media_content):
                    if (media.mime_type or "").startswith("audio"):
                        return media
        return None

    @staticmethod
    def _create_media_info(mime_type: str, file_format: str, base64_data: str) -> dict:
        file_name = (
            f"genAudio_{datetime.now().strftime('%Y%m%d_%H%M%S')}_"
            f"{uuid.uuid4().hex[:8]}.{file_format}"
        )
        return {
            "fileName": file_name,
            "base64Data": base64_data,
            "fileFormat": file_format,
            "mimeType": mime_type,
            "mediaInputType": "FILE",
        }
