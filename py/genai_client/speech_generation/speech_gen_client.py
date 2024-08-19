from TTS.api import TTS
from TTS.utils.manage import ModelManager
from typing import Optional
import torch
from .abstract_speech_generation import AbstractSpeechGenClient


class SpeechGenClient(AbstractSpeechGenClient):
    def __init__(
        self,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.use_gpu = True if torch.cuda.is_available() else False

    def generate_speech(
        self,
        prompt: str,
        speaker_file_path: str,
        output_dir: str,
        model_name: Optional[str] = "tts_models/multilingual/multi-dataset/xtts_v2",
        file_name: Optional[str] = "output",
        **kwargs,
    ) -> dict:
        """_summary_
        Generate speech from text using a pre-trained text-to-speech model
        Args:
            prompt (str): _description_ The text to be converted to speech
            speaker_file_path (str): _description_ The path to the speaker file
            output_dir (str): _description_ The directory to save the output file
            file_name (str, optional): _description_. Defaults to "output". The name of the output file
        Returns:
            dict: _description_ A dictionary containing the file path of the generated speech file
        """

        tts = TTS(model_name, gpu=self.use_gpu)
        file_path = output_dir + "/" + file_name + ".mp3"
        tts.tts_to_file(
            text=prompt,
            file_path=file_path,
            speaker_wav=speaker_file_path,
            language="en",
        )

        response = {
            "file_path": file_path,
        }
        return response

    def get_spectrogram_models(self) -> list:
        """_summary_
        Get a list of available text-to-speech spectrogram models
        Returns:
            list: _description_ A list of available text-to-speech spectrogram models
        """
        model_manager = ModelManager()

        models = model_manager.list_models()

        return {
            "models": models,
        }
