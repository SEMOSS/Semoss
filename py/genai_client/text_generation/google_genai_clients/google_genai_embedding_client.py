from typing import List
from pathlib import Path
import base64
import mimetypes
from google.genai import types
from .google_genai_client import GoogleGenAiTextClient
from ...constants import EmbeddingsModelEngineResponse


class GoogleGenAiEmbeddingsClient(GoogleGenAiTextClient):

    def embeddings_call(self, strings_to_embed: List[str], prefix: str = "", **kwargs):
        if self.client is None:
            raise ValueError("Google Gen AI Embeddings client is not initialized.")

        contents: List[types.Content] = []
        for s in strings_to_embed:
            part = types.Part.from_text(text=s)
            contents.append(types.Content(role="user", parts=[part]))

        config = types.EmbedContentConfig()

        response = self.client.models.embed_content(
            model=self.model_name,
            contents=contents,
            config=config,
        )

        vectors: List[List[float]] = []
        for emb in getattr(response, "embeddings", []) or []:
            vec = (
                getattr(emb, "values", None)
                or getattr(emb, "embedding", None)
                or getattr(emb, "text_embedding", None)
            )
            if vec is None:
                raise RuntimeError(f"Unexpected embedding payload: {emb}")
            vectors.append(list(vec))

        return EmbeddingsModelEngineResponse(
            response=vectors,
            prompt_tokens=0,
            response_tokens=0,
        )

    def _make_image_part(self, item: str) -> types.Part:
        """
        Accepts:
          - 'gs://bucket/path.jpg'
          - local file path (e.g., '/tmp/cat.png')
          - data URL ('data:image/png;base64,....')
          - raw base64 string (no data: header) -> assumes PNG unless we can guess
        Returns a google.genai.types.Part suitable for embed_content.
        """
        # GCS URI
        if item.startswith("gs://"):
            guessed_mime, _ = mimetypes.guess_type(item)
            return types.Part.from_uri(
                file_uri=item, mime_type=guessed_mime or "image/jpeg"
            )

        # data URL
        if item.startswith("data:"):
            header, b64 = item.split(",", 1)
            mime = header.split(";")[0].replace("data:", "") or "image/png"
            return types.Part.from_bytes(data=base64.b64decode(b64), mime_type=mime)

        # local file path
        p = Path(item)
        if p.exists() and p.is_file():
            mime = mimetypes.guess_type(p.name)[0] or "image/png"
            with p.open("rb") as f:
                return types.Part.from_bytes(data=f.read(), mime_type=mime)

        # raw base64 fallback
        try:
            data = base64.b64decode(item, validate=True)
            return types.Part.from_bytes(data=data, mime_type="image/png")
        except Exception:
            raise ValueError(f"Unsupported image reference: {item}")

    def image_embeddings_call(self, images_to_embed: List[str], **kwargs):
        if self.client is None:
            raise ValueError("Google Gen AI Embeddings client is not initialized.")

        # Build one Content per image (important: list of *Contents*, not list of Parts)
        contents: List[types.Content] = []
        for img in images_to_embed:
            part = self._make_image_part(img)
            contents.append(types.Content(role="user", parts=[part]))

        config = types.EmbedContentConfig()

        response = self.client.models.embed_content(
            model=self.model_name,
            contents=contents,
            config=config,
        )

        vectors: List[List[float]] = []
        for emb in getattr(response, "embeddings", []) or []:
            vec = (
                getattr(emb, "values", None)
                or getattr(emb, "embedding", None)
                or getattr(emb, "image_embedding", None)
            )
            if vec is None:
                raise RuntimeError(f"Unexpected embedding payload: {emb}")
            vectors.append(list(vec))

        return EmbeddingsModelEngineResponse(
            response=response,
            prompt_tokens=0,
            response_tokens=0,
        )
