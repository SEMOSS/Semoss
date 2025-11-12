from typing import Literal, Optional
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


class VoiceOption(StringEnum):
    ALLOY = "alloy"
    ASH = "ash"
    BALLAD = "ballad"
    CORAL = "coral"
    ECHO = "echo"
    FABLE = "fable"
    NOVA = "nova"
    ONYX = "onyx"
    SAGE = "sage"
    SHIMMER = "shimmer"


class TextToSpeechConfig(BaseModel):
    input: str
    model: str
    voice: str = None
    instructions: Optional[str] = None
    response_format: Optional[str] = None


class OpenAiAudioClient:

    def __init__(self, client):
        self.client = client

    def ask(
        self,
        text: str = None,
        **kwargs,
    ) -> AskModelEngineResponse:

        # add the config for TTS
        audio_config = self._create_audio_config(text, **kwargs)
        response = self._create_audio(audio_config)

        return response

    def _create_audio(self, audio_config) -> AskModelEngineResponse:
        final_response = {}
        input_tokens = 0
        output_tokens = 0
        if isinstance(audio_config, BaseModel):
            audio_config = audio_config.model_dump(exclude_none=True)
        try:
            with self.client.client.audio.speech.with_streaming_response.create(
                **audio_config
            ) as response:
                audio_bytes = response.read()

            # adding audio bytes as final response
            final_response.update({"audio_bytes": audio_bytes})

            model_engine_response = AskModelEngineResponse(
                response=final_response,
                response_tokens=output_tokens,
                prompt_tokens=input_tokens,
                messageType="AUDIO",
            )
            return model_engine_response
        except Exception as e:
            print(f"Error generating audio: {e}")
            raise

    def _create_audio_config(self, text: str, **kwargs):
        """
        Create the configuration for the OpenAI Text to Speech generation request.
        """
        model = self.client.model_name
        voice = kwargs.pop("voice", VoiceOption.ALLOY)
        instructions = kwargs.pop("instructions", None)
        response_format = kwargs.pop("response_format", ResponseFormat.MP3)

        if voice is not None and voice not in VoiceOption.values():
            voice = VoiceOption.ALLOY

        if (
            response_format is not None
            and response_format not in ResponseFormat.values()
        ):
            response_format = ResponseFormat.MP3

        return TextToSpeechConfig(
            input=text,
            model=model,
            voice=voice,
            instructions=instructions,
            response_format=response_format,
        )
