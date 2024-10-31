from typing import Optional, Dict, Any, List
import asyncio
import logging
import random
import string
from .remote_client_2 import RemoteClient2, ModelDeploymentConfig, ModelStatus

logger = logging.getLogger(__name__)


class NERClient(RemoteClient2):
    def __init__(
        self,
        host: str,
        deployer_endpoint: str,
        model_repo_name: str,
        model_name: str,
        model_id: str,
        is_dev: bool = False,
    ):

        config = ModelDeploymentConfig(
            deployer_endpoint=deployer_endpoint,
            model_repo_name=model_repo_name,
            model_name=model_name,
            model_id=model_id,
            is_dev=is_dev,
        )

        super().__init__(host=host, config=config)

    def initalize(self):
        model_status = self.initialize_model()
        return model_status

    def predict(
        self,
        text: str,
        entities: List[str],
        mask_entities: List[str],
        prefix: Optional[str] = "",
        **kwargs,
    ) -> Dict[str, Any]:
        # Check if model is available
        model_status = self.initialize_model()
        print(f"Model Status: {model_status.status}")

        if model_status.status != "active":
            return {
                "status": model_status.status,
                "message": model_status.message,
                "retry_suggested": True,
            }

        response = asyncio.run(self._predict_call(text, entities))
        if not response:
            return None

        masked_data = self._mask_entities(text, response, mask_entities)

        return {
            "status": "success",
            "output": masked_data["masked_text"],
            "raw_output": response,
            "mask_values": masked_data["mask_values"],
            "input": text,
            "entities": entities,
        }

    async def _predict_call(
        self, text: str, labels: List[str]
    ) -> Optional[List[Dict[str, Any]]]:
        request_payload = {
            "model": self.config.model_name,
            "text": text,
            "labels": labels,
        }

        response_data = await self.gaas_request(request_payload)
        if not response_data:
            return None

        return response_data.get("entities")

    def _generate_mask(self, length: int = 6) -> str:
        """Generate a random mask string."""
        random_str = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=length)
        )
        return f"m_{random_str}"

    def _mask_entities(
        self, text: str, entities: List[Dict[str, Any]], mask_entities: List[str]
    ) -> Dict[str, Any]:
        """
        Mask entities in the text based on the mask_entities list.

        Args:
            text (str): Original text
            entities (List[Dict[str, Any]]): Detected entities
            mask_entities (List[str]): List of entity types to mask

        Returns:
            Dict[str, Any]: Dictionary containing masked text and mapping
        """
        entities = sorted(entities, key=lambda x: x["start"], reverse=True)

        mask_values = {}
        new_text = text

        for entity in entities:
            if entity["label"] in mask_entities:
                orig_text = entity["text"]
                start = entity["start"]
                end = entity["end"]

                # Generate or retrieve mask
                if orig_text in mask_values:
                    mask_text = mask_values[orig_text]
                else:
                    mask_text = self._generate_mask()
                    mask_values[orig_text] = mask_text
                    mask_values[mask_text] = orig_text

                new_text = new_text[:start] + mask_text + new_text[end:]

        return {"masked_text": new_text, "mask_values": mask_values}
