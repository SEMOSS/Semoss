from TTS.api import TTS
from typing import Optional
import torch
from .abstract_speech_generation import AbstractSpeechGenClient


class SpeechGenClient(AbstractSpeechGenClient):
    def __init__(
        self,
        model_name: str = "tts_models/multilingual/multi-dataset/xtts_v2",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.use_gpu = True if torch.cuda.is_available() else False
        self.model_name = model_name

    def generate_speech(
        self,
        prompt: str,
        voice_file_path: str,
        output_dir: str,
        file_name: str = "output",
        **kwargs,
    ) -> str:
        tts = TTS(self.model_name, gpu=self.use_gpu)
        tts.tts_to_file(
            text=prompt,
            file_path=output_dir + file_name + ".mp3",
            speaker_wav=voice_file_path,
            language="en",
        )
        return f"{output_dir}/{file_name}.wav"
