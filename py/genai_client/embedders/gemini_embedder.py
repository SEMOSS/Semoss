from typing import List, Dict, Any, Optional
import base64
import mimetypes
import logging
import requests

from .abstract_embedder import AbstractEmbedder
from ..constants import EmbeddingsModelEngineResponse

logger = logging.getLogger(__name__)

def _encode_file(file_path: str):
    """Encode a local file to base64 and determine MIME type."""
    mime_type, _ = mimetypes.guess_type(file_path)
    mime_type = mime_type or "application/octet-stream"

    with open(file_path, "rb") as f:
        encoded_data = base64.b64encode(f.read()).decode("utf-8")

    return mime_type, encoded_data


class GeminiMultimodalEmbedder(AbstractEmbedder):
    """
    Embedder for Google Gemini Embedding 2.0 multimodal model.
    Supports text, image, video, and audio inputs.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_key = kwargs.get("api_key")
        self.endpoint = kwargs.get("endpoint")
        self.task_type = kwargs.get("task_type", "RETRIEVAL_DOCUMENT")

    def _get_tokenizer(self, init_args: Dict):
        # Gemini uses its own tokenizer; use a basic fallback for client-side counting
        from ..tokenizers.word_count_tokenizer import WordCountTokenizer
        return WordCountTokenizer()

    def embeddings_call(
        self, strings_to_embed: List[str], **kwargs: Any
    ) -> EmbeddingsModelEngineResponse:
        """Delegate text-only calls to multimodal."""
        inputs = [{"type": "text", "content": s} for s in strings_to_embed]
        return self.multimodal_embeddings_call(inputs, **kwargs)

    def image_embeddings_call(
        self, images_to_embed: List[str], **kwargs: Any
    ) -> EmbeddingsModelEngineResponse:
        """Delegate image-only calls to multimodal."""
        inputs = [{"type": "image", "content": img} for img in images_to_embed]
        return self.multimodal_embeddings_call(inputs, **kwargs)

    def multimodal_embeddings_call(
        self, inputs_to_embed: List[Dict[str, Any]], **kwargs: Any
    ) -> EmbeddingsModelEngineResponse:
        """
        Call the Gemini Embedding 2.0 API with multimodal inputs.

        Args:
            inputs_to_embed: List of dicts with "type" and "content" keys
            **kwargs: Additional parameters (task_type, output_dimensionality, etc.)

        Returns:
            EmbeddingsModelEngineResponse with embedding vectors
        """
        if not inputs_to_embed:
            raise ValueError("inputs_to_embed cannot be empty.")

        logger.debug("Processing %d multimodal inputs for Gemini embedding", len(inputs_to_embed))

        # Implementing actual Gemini API call
        # Build the request payload from inputs_to_embed
        parts = []
        for item in inputs_to_embed:
            input_type = item.get("type")
            content = item.get("content")

            if input_type == "text":
                parts.append({"text": content})

            elif input_type in ["image", "audio"]:
                mime_type, encoded_data = _encode_file(content)
                parts.append({
                    "inlineData": {
                        "mimeType": mime_type,
                        "data": encoded_data
                    }
                })

            elif input_type == "video":
                mime_type, _ = mimetypes.guess_type(content)
                mime_type = mime_type or "video/mp4"
                parts.append({
                    "fileData": {
                        "mimeType": mime_type,
                        "fileUri": content
                    }
                })

            else:
                raise ValueError(f"Unsupported modality type: {input_type}")

        # Send to Gemini embedding endpoint
        payload = {
            "model": "models/gemini-embedding-001",
            "content": {"parts": parts},
            "taskType": kwargs.get("task_type", self.task_type)
        }

        headers = {
            "Content-Type": "application/json",
            "x-goog-api-key": self.api_key
        }

        response = requests.post(
            self.endpoint,
            json=payload,
            headers=headers,
            timeout=60
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Gemini API call failed: {response.status_code} - {response.text}"
            )

        # Parse response and return EmbeddingsModelEngineResponse
        response_json = response.json()

        embeddings = [
            emb.get("values", [])
            for emb in response_json.get("embeddings", [])
        ]

        usage = response_json.get("usageMetadata", {})
        prompt_tokens = usage.get("promptTokenCount", 0)
        total_tokens = usage.get("totalTokenCount", 0)

        return EmbeddingsModelEngineResponse(
            embeddings=embeddings,
            prompt_tokens=prompt_tokens,
            response_tokens=total_tokens
        )