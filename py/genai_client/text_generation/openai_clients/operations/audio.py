from typing import Literal, Optional
from pydantic import BaseModel, Field
from ....constants import AskModelEngineResponse
from ....utils import StringEnum


class AudioAction(StringEnum):
    TRANSCRIBE = "transcribe"
    TRANSLATE = "translate"
    TTS = "text_to_speech"
    STT = "speech_to_text"


class Audio:
    def __init__(self, client):
        self.client = client

    def ask(
        self,
        question: str = None,
        **kwargs,
    ):
        self.model_name = self.client.model_name
        self.audio_action = kwargs.pop("audio_action", None)
        if self.audio_action is None:
            raise ValueError("audio_action must be specified for audio operations.")

    def tts(
        self,
        text: str,
        **kwargs,
    ):
        try:
            response = self.client.client.audio.speech.create(
                model=self.model_name, voice=kwargs.get("voice"), **kwargs
            )
            model_engine_response = AskModelEngineResponse(
                raw_response=response, generated_text=response["audio_url"]
            )
        except Exception as e:
            print(f"Error generating speech: {e}")
            raise
