from typing import Dict, Optional
from gaas_server_proxy import ServerProxy


class ImageEngine(ServerProxy):

    def __init__(
        self,
        engine_id: Optional[str] = None,
        insight_id: Optional[str] = None,
    ):
        super().__init__()
        self.engine_id = engine_id
        self.insight_id = insight_id
        print(f"Image Engine {engine_id} is initialized")

    def get_model_type(
        self,
        insight_id: Optional[str] = None
    ):
        if insight_id is None:
            insight_id = self.insight_id

        epoc = super().get_next_epoc()
        return super().call(
            epoc=epoc,
            engine_type='Model',
            engine_id=self.engine_id,
            method_name='getModelType',
            method_args=[],
            method_arg_types=[],
            insight_id=insight_id,
        )

    def generate_image(
        self,
        prompt: str,
        space: Optional[str] = "insight",
        file_path: Optional[str] = "/images",
        insight_id: Optional[str] = None,
        param_dict: Optional[Dict] = {}
    ) -> str:
        if insight_id is None:
            insight_id = self.insight_id

        epoc = super().get_next_epoc()

        param_dict["space"] = space
        param_dict["filePath"] = file_path

        return super().call(
            epoc=epoc,
            engine_type='Model',
            engine_id=self.engine_id,
            method_name='generateImage',
            method_args=[prompt, insight_id, param_dict],
            method_arg_types=[
                'java.lang.String',
                'prerna.om.Insight',
                'java.util.Map'
            ],
            insight_id=insight_id
        )
