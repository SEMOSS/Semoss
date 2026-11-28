"""Prompt counts from a serving endpoint's tokenize route (vLLM ``POST /tokenize``)."""

import logging
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


class EndpointTokenizer:
    """Counts a chat request with the chat template and tools applied.

    Returns None on failure so callers estimate; a 404/405 stops further calls.
    """

    def __init__(self, url: str, model: str, api_key: Optional[str] = None):
        self.url = url
        self.model = model
        self.headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.available = True

    def count_chat(
        self, messages: List[Dict[str, Any]], tools: Optional[List[Any]] = None
    ) -> Optional[int]:
        if not self.available:
            return None
        body: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "add_generation_prompt": True,
        }
        if tools:
            body["tools"] = tools
        try:
            response = httpx.post(self.url, json=body, headers=self.headers, timeout=10.0)
            if response.status_code in (404, 405):
                self.available = False
                return None
            response.raise_for_status()
            return int(response.json()["count"])
        except Exception as exc:
            logger.warning("tokenize call to %s failed: %s", self.url, exc)
            return None
