from typing import Optional, Dict, Any, List
import asyncio
import logging
import httpx
import random
import string
from .remote_client import RemoteClient

logger = logging.getLogger(__name__)


class NamedEntityRecognitionRemoteClient(RemoteClient):
    def __init__(
        self,
        endpoint: str,
        model_name: str,
        timeout: Optional[float] = 300.0,
    ):
        super().__init__(endpoint, model_name, timeout)

    def predict(
        self,
        text: str,
        entities: List[str],
        mask_entities: List[str],
        prefix: Optional[str] = "",
        **kwargs,
    ) -> Dict[str, Any]:
        response = asyncio.run(self._predict_call(text, entities))

        masked_data = self._mask_entities(text, response, mask_entities)

        return {
            "output": masked_data["masked_text"],
            "raw_output": response,
            "mask_values": masked_data["mask_values"],
            "input": text,
            "entities": entities,
        }

    async def _predict_call(self, text: str, labels: List[str]) -> List[Dict[str, Any]]:
        request_payload = {"model": self.model_name, "text": text, "labels": labels}
        try:
            response_data = await self.gaas_request(request_payload)
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP Status Error: {e}")
            return None
        return response_data["entities"]

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
