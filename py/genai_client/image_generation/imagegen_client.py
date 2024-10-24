import os
import datetime
from typing import Optional, Dict, Any
import base64
import asyncio
import json
import logging
import httpx

from genai_client.image_generation.abstract_image_generation_client import (
    AbstractImageGenerationClient,
)

logger = logging.getLogger(__name__)


class ImageGenClient(AbstractImageGenerationClient):
    def __init__(
        self,
        endpoint: str,
        model_name: Optional[str] = "PixArt-alpha/PixArt-XL-2-1024-MS",
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

                                base64_uncompressed = data.get("image")
                                return data, base64_uncompressed

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

    def generate_image(
        self,
        prompt: str,
        output_dir: str,
        file_name: str = "generated_image",
        consistency_decoder: Optional[bool] = False,
        negative_prompt: Optional[str] = None,
        guidance_scale: Optional[float] = 7.5,
        num_inference_steps: Optional[int] = 50,
        height: Optional[int] = 512,
        width: Optional[int] = 512,
        seed: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Synchronously generate an image by running the asynchronous generate_image_remote method.

        Args:
            prompt (str): The text prompt for image generation.
            output_dir (str): Directory to save the generated image.
            file_name (str, optional): Base name for the generated image file.
            consistency_decoder (bool, optional): Consistency decoder flag.
            negative_prompt (str, optional): Negative prompt to guide image generation.
            guidance_scale (float, optional): Guidance scale for image generation.
            num_inference_steps (int, optional): Number of inference steps.
            height (int, optional): Height of the generated image.
            width (int, optional): Width of the generated image.
            seed (int, optional): Seed for reproducibility.

        Returns:
            Optional[Dict[str, Any]]: Details of the generated image or None if failed.
        """
        return asyncio.run(
            self.generate_image_remote(
                prompt,
                output_dir,
                file_name,
                consistency_decoder,
                negative_prompt,
                guidance_scale,
                num_inference_steps,
                height,
                width,
                seed,
            )
        )

    async def generate_image_remote(
        self,
        prompt: str,
        output_dir: str,
        file_name: str = "generated_image",
        consistency_decoder: Optional[bool] = False,
        negative_prompt: Optional[str] = None,
        guidance_scale: Optional[float] = 7.5,
        num_inference_steps: Optional[int] = 50,
        height: Optional[int] = 512,
        width: Optional[int] = 512,
        seed: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Asynchronously generate an image by sending a request to the FastAPI server.

        Args:
            prompt (str): The text prompt for image generation.
            output_dir (str): Directory to save the generated image.
            file_name (str, optional): Base name for the generated image file.
            consistency_decoder (bool, optional): Consistency decoder flag.
            negative_prompt (str, optional): Negative prompt to guide image generation.
            guidance_scale (float, optional): Guidance scale for image generation.
            num_inference_steps (int, optional): Number of inference steps.
            height (int, optional): Height of the generated image.
            width (int, optional): Width of the generated image.
            seed (int, optional): Seed for reproducibility.

        Returns:
            Optional[Dict[str, Any]]: Details of the generated image or None if failed.
        """
        request_payload = {
            "prompt": prompt,
            "consistency_decoder": consistency_decoder,
            "negative_prompt": negative_prompt,
            "guidance_scale": guidance_scale,
            "num_inference_steps": num_inference_steps,
            "height": height,
            "width": width,
            "seed": seed,
            "file_name": file_name,
            "model": self.model_name,
        }

        try:
            response_data, base64_uncompressed = await self.gaas_request(
                request_payload
            )
            print(response_data)
            if response_data is None:
                print("Image generation failed or was cancelled.")
                logger.error("Image generation failed or was cancelled.")
                return None

            # Decode the base64 image
            base64_image = base64.b64decode(base64_uncompressed)

            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)

            # Generate a unique filename
            unique_identifier = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            file_path = os.path.join(output_dir, f"{file_name}-{unique_identifier}.png")

            # Save the image to the specified path
            with open(file_path, "wb") as image_file:
                image_file.write(base64_image)

            logger.info(f"Image saved at: {file_path}")

            return {
                "file_path": file_path,
                "generation_time": response_data.get("generation_time"),
                "seed": response_data.get("seed"),
                "prompt": response_data.get("prompt"),
                "negative_prompt": response_data.get("negative_prompt"),
                "guidance_scale": response_data.get("guidance_scale"),
                "num_inference_steps": response_data.get("num_inference_steps"),
                "height": response_data.get("height"),
                "width": response_data.get("width"),
                "model_name": response_data.get("model_name"),
                "vae_model_name": response_data.get("vae_model_name"),
                "base64Image": base64_uncompressed,
            }
        except Exception as e:
            logger.error(f"Failed to generate image: {e}")
            return None

    async def __aenter__(self):
        """
        Enable asynchronous context management.
        """
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """
        Ensure the HTTP client is closed when exiting the context.
        """
        await self.close()
