from typing import Dict, Optional
from gaas_server_proxy import ServerProxy


class SpeechEngine(ServerProxy):
    def __init__(self, engine_id: str = None, insight_id: Optional[str] = None):
        super().__init__()
        self.engine_id = engine_id
        self.insight_id = insight_id
        print(f"Image Engine {engine_id} is initialized")

    def get_model_type(self, insight_id: Optional[str] = None):
        if insight_id is None:
            insight_id = self.insight_id

        epoc = super().get_next_epoc()
        return super().call(
            epoc=epoc,
            engine_type="Model",
            engine_id=self.engine_id,
            method_name="getModelType",
            method_args=[],
            method_arg_types=[],
            insight_id=insight_id,
        )

    def generate_speech(
        self,
        prompt: str,
        speaker_space: str,
        speaker_file_path: str,
        space: Optional[str] = "insight",
        file_path: Optional[str] = "/speech",
        insight_id: Optional[str] = None,
        param_dict: Optional[Dict] = {},
    ):
        if insight_id is None:
            insight_id = self.insight_id

        epoc = super().get_next_epoc()

        param_dict["space"] = space
        param_dict["filePath"] = file_path
        param_dict["speakerSpace"] = speaker_space
        param_dict["speakerFilePath"] = speaker_file_path

        return super().call(
            epoc=epoc,
            engine_type="Model",
            engine_id=self.engine_id,
            methodName="generateSpeech",
            method_args=[prompt, insight_id, param_dict],
            method_arg_types=["str", "str", "dict"],
            insight_id=insight_id,
        )

    def get_spectrogram_models(self, insight_id: Optional[str] = None):
        if insight_id is None:
            insight_id = self.insight_id
        epoc = super().get_next_epoc()

        return super().call(
            epoc=epoc,
            engine_type="Model",
            engine_id=self.engine_id,
            methodName="getSpectrogramModels",
            method_args=[],
            method_arg_types=[],
            insight_id=insight_id,
        )
