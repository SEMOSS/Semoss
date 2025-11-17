import base64
from typing import Optional
from pydantic import BaseModel
from google.genai import types
from ...constants import (
    AskModelEngineResponse,
)


class TTSConfig(BaseModel):
    text: str
    model: str
    voice: str
    instructions: Optional[str] = None


class GoogleGenAiAudioClient:

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
        audio_bytes = b""
        # TODO: track tokens
        input_tokens = 0
        output_tokens = 0

        if audio_config.instructions:
            # Example: "Say cheerfully: " + "Hello world"
            prompt = f'{audio_config.instructions}: "{audio_config.text}"'
        else:
            prompt = audio_config.text

        try:
            if stream:
                response = self.client.client.models.generate_content_stream(
                    model=audio_config.model,
                    contents=[prompt],
                    config=types.GenerateContentConfig(
                        response_modalities=["AUDIO"],
                        speech_config=types.SpeechConfig(
                            voice_config=types.VoiceConfig(
                                prebuilt_voice_config=types.PrebuiltVoiceConfig(
                                    voice_name=audio_config.voice
                                )
                            )
                        ),
                    ),
                )
                for event in response:
                    if hasattr(event, "candidates") and event.candidates:
                        candidate = event.candidates[0]
                        if hasattr(candidate, "content") and hasattr(
                            candidate.content, "parts"
                        ):
                            for part in candidate.content.parts:
                                audio_part = part.inline_data
                                if audio_part:
                                    audio_bytes += audio_part.data

            else:
                response = self.client.client.models.generate_content(
                    model=audio_config.model,
                    contents=[prompt],
                    config=types.GenerateContentConfig(
                        response_modalities=["AUDIO"],
                        speech_config=types.SpeechConfig(
                            voice_config=types.VoiceConfig(
                                prebuilt_voice_config=types.PrebuiltVoiceConfig(
                                    voice_name=audio_config.voice
                                )
                            )
                        ),
                    ),
                )

                # Extract audio bytes
                audio_part = response.candidates[0].content.parts[0].inline_data
                audio_bytes = audio_part.data

            audio_b64 = base64.b64encode(audio_bytes).decode("ascii")

            return AskModelEngineResponse(
                response=audio_b64,
                response_tokens=output_tokens,
                prompt_tokens=input_tokens,
                messageType="CHAT",
            )
        except Exception as e:
            raise Exception(f"Error generating audio with Gemini TTS: {e}")

    def _create_audio_config(self, text: str, **kwargs) -> TTSConfig:
        """
        Create the configuration for the Gemini Text to Speech generation request.
        """
        model = self.client.model_name
        voice = kwargs.pop("voice", "Puck")
        instructions = kwargs.pop("instructions", None)

        return TTSConfig(
            text=text,
            model=model,
            voice=voice,
            instructions=instructions,
        )
