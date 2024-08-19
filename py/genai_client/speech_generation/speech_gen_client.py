from TTS.api import TTS
from TTS.utils.manage import ModelManager
from typing import Optional
import torch
from .abstract_speech_generation import AbstractSpeechGenClient


class SpeechGenClient(AbstractSpeechGenClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.use_gpu = True if torch.cuda.is_available() else False

    def generate_speech(
        self,
        prompt: str,
        speaker_file_path: str,
        output_dir: str,
        model: Optional[str] = "tts_models/multilingual/multi-dataset/xtts_v2",
    ) -> dict:
        """_summary_
        Generate speech from a given text prompt and speaker audio file
        Args:
            prompt (str): _description_ The text to be converted to speech
            speaker_file_path (str): _description_ The path to the speaker's audio file
            output_dir (str): _description_ The directory to save the output file
            model_name (Optional[str], optional): _description_. Spectrogram model name. Defaults to "tts_models/multilingual/multi-dataset/xtts_v2".
        Returns:
            dict: _description_ A dictionary containing the file path of the generated speech file
        """

        model_manager = ModelManager()
        models = model_manager.list_models()
        # Making sure the model exists in the list of available models
        if model not in models:
            raise ValueError(f"Model {model} not found. Available models are: {models}")

        tts = TTS(model, gpu=self.use_gpu)

        file_path = (
            output_dir
            if output_dir.endswith((".mp3", ".wav"))
            else f"{output_dir}/output.mp3"
        )

        if "/multilingual/" in model:
            tts.tts_to_file(
                text=prompt,
                file_path=file_path,
                speaker_wav=speaker_file_path,
                language="en",
            )
        else:
            tts.tts_to_file(
                text=prompt,
                file_path=file_path,
                speaker_wav=speaker_file_path,
            )

        return {"file_path": file_path}

    def get_spectrogram_models(self) -> list:
        """_summary_
        Get a list of available text-to-speech spectrogram models
        Returns:
            list: _description_ A list of available text-to-speech spectrogram models
        """
        model_manager = ModelManager()
        models = model_manager.list_models()
        return {"models": models}
