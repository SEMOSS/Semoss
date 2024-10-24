from typing import Optional, Dict, Any, List
import asyncio
import logging
import httpx
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
        labels: List[str],
        prefix: Optional[str] = "",
        **kwargs,
    ) -> List[Dict[str, Any]]:
        response = asyncio.run(self.predict_call(text, labels))
        return {"response": response}

    async def predict_call(self, text: str, labels: List[str]) -> List[Dict[str, Any]]:
        request_payload = {"model": self.model_name, "text": text, "labels": labels}
        try:
            response_data = await self.gaas_request(request_payload)
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP Status Error: {e}")
            return None
        return response_data["entities"]
