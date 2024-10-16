import os
import datetime
from typing import Optional, Dict, Any, List
import base64
import asyncio
import json
import logging
import httpx
from .remote_client import RemoteClient
from ..constants import (
    AskModelEngineResponse,
    MAX_TOKENS,
    MAX_INPUT_TOKENS,
    FULL_PROMPT,
)

logger = logging.getLogger(__name__)


class TextGenRemoteClient(RemoteClient):
    def __init__(self, endpoint, model_name, timeout=300.0):
        super().__init__(endpoint, model_name, timeout)

    def ask(
        self,
        question: str = None,
        context: Optional[str] = None,
        history: Optional[List] = [],
        template_name: Optional[str] = None,
        max_new_tokens: Optional[int] = 2048,
        prefix: Optional[str] = "",
        do_sample: bool = False,
        operation: Optional[str] = None,
        repetition_penalty: Optional[float] = None,
        return_full_text: bool = False,
        seed: Optional[int] = None,
        stop_sequences: Optional[List[str]] = None,
        temperature: Optional[float] = None,
        top_k: Optional[int] = None,
        top_p: Optional[float] = None,
        truncate: Optional[int] = None,
        typical_p: Optional[float] = None,
        watermark: bool = False,
        **kwargs,
    ):
        model_engine_response = AskModelEngineResponse()

        operation = operation.lower()
        # once I have the typical ask method, we will default to that
        if operation == None:
            model_engine_response.response = (
                "This model does not support the operation: None"
            )
            return model_engine_response
        if operation == "instruct":
            # The entrypoint method needs to be synchronous, so ayncio.run is my best option here
            response = asyncio.run(
                self.instruct(
                    task=question,
                    temp=temperature,
                    prob=top_p,
                    max_tokens=max_new_tokens,
                )
            )
            if response == None:
                model_engine_response.response = "Failed to generate."
                return model_engine_response

            model_engine_response.response = response
            return model_engine_response
        else:
            model_engine_response.response = f"Operation {operation} is not supported."
            return model_engine_response

    async def instruct(
        self,
        task: str,
        temp: Optional[float] = 0.1,
        prob: Optional[float] = 0.2,
        max_tokens: Optional[int] = 2048,
    ):
        """
        Send a request to the /generate endpoint with the given task and parameters.
        Args:
            task (str): The task to perform.
            temp (float): The temperature to use for sampling.
            prob (float): The probability to use for nucleus sampling.
            max_tokens (int): The maximum number of tokens to generate.
        Returns:

        """
        request_payload = {
            "model": self.model_name,
            "task": task,
            "operation": "instruct",
            "temp": temp,
            "prob": prob,
            "max_tokens": max_tokens,
        }

        try:
            return await self.gaas_request(request_payload)
        except Exception as e:
            print(f"Failed to generate: {e}")
            return None
