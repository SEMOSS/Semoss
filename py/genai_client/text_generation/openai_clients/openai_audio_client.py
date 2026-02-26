import base64
from typing import Optional
from pydantic import BaseModel
from ...constants import AskModelEngineResponse
from ...utils import StringEnum


class ResponseFormat(StringEnum):
    MP3 = "mp3"
    OPUS = "opus"
    AAC = "aac"
    FLAC = "flac"
    WAV = "wav"
    PCM = "pcm"


class TTSConfig(BaseModel):
    input: str
    model: str
    voice: str
    instructions: Optional[str] = None
    response_format: Optional[str] = None


class OpenAiAudioClient:

    def __init__(self, client):
        self.client = client

    def ask(
        self,
        text: str,
        **kwargs,
    ) -> AskModelEngineResponse:
        stream = kwargs.pop("stream", True)
        audio_config = self._create_audio_config(text, **kwargs)
        return self._generate_audio(audio_config, stream=stream)

    def _generate_audio(
        self, audio_config: TTSConfig, stream: bool = True
    ) -> AskModelEngineResponse:
        audio_bytes = ""
        # TODO track tokens
        input_tokens = 0
        output_tokens = 0
        try:
            if stream:
                with self.client.client.audio.speech.with_streaming_response.create(
                    **audio_config.model_dump(exclude_none=True)
                ) as response:
                    audio_bytes = response.read()
            else:
                response = self.client.client.audio.speech.create(
                    **audio_config.model_dump(exclude_none=True)
                )
                if response and hasattr(response, "content"):
                    audio_bytes = response.content
                else:
                    audio_bytes = b""

            audio_b64 = base64.b64encode(audio_bytes).decode("ascii")

            return AskModelEngineResponse(
                response=audio_b64,
                response_tokens=output_tokens,
                prompt_tokens=input_tokens,
                messageType="CHAT",
            )
        except Exception as e:
            raise Exception(f"Error generating audio: {e}")

    def _create_audio_config(self, text: str, **kwargs) -> TTSConfig:
        """
        Create the configuration for the OpenAI Text to Speech generation request.
        """
        model = self.client.model_name
        voice = kwargs.pop("voice", "alloy")
        instructions = kwargs.pop("instructions", None)
        response_format = kwargs.pop("response_format", ResponseFormat.MP3)

        if response_format not in ResponseFormat.values():
            response_format = ResponseFormat.MP3

        return TTSConfig(
            input=text,
            model=model,
            voice=voice,
            instructions=instructions,
            response_format=response_format,
        )
