from typing import Optional, Dict, Any
import json
import logging
import httpx

logger = logging.getLogger(__name__)


class RemoteClient:
    def __init__(
        self,
        endpoint: str,
        model_name: str,
        timeout: Optional[float] = 300.0,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.model_name = model_name
        self.timeout = timeout
        self.client = httpx.AsyncClient(timeout=self.timeout)

    async def close(self):
        """
        Gracefully close the HTTP client.
        """
        await self.client.aclose()
        logger.info("HTTP client closed.")

    async def gaas_request(self, request_payload: Dict[str, Any]):
        """
        Send a POST request to the /generate endpoint and handle Server-Sent Events (SSE).
        Args:
            request_payload (Dict[str, Any]): The payload to send in the POST request.
        Returns:
            Tuple[Optional[Dict[str, Any]], Optional[str]]: The response data and base64 image string.
        """
        url = f"{self.endpoint}"
        headers = {"Accept": "text/event-stream"}

        try:
            async with self.client.stream(
                "POST", url, json=request_payload, headers=headers
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line.startswith("data:"):
                        data_str = line[5:].strip()
                        if data_str:
                            data = json.loads(data_str)
                            status = data.get("status")
                            message = data.get("message")

                            print(f"Status Update: {status} - {message}")

                            if status == "complete":
                                return data

                            elif status in ["error", "cancelled", "timeout"]:
                                logger.error(f"Job {status}: {message}")
                                return None, None

            return None, None
        except httpx.HTTPError as e:
            print(f"HTTP request failed: {e}")
            return None, None
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON response: {e}")
            return None, None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None, None
