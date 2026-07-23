from typing import List, Optional, Dict, Any, Union, Tuple
import re
import base64
from genai_client.constants import EmbeddingsModelEngineResponse
from genai_client.tokenizers.abstract_tokenizer import AbstractTokenizer
from ..constants import (
    MAX_TOKENS,
    EmbeddingsModelEngineResponse2,
    MultiModalEmbeddingItem,
    MultiModalEmbeddingsResponse,
)
from .abstract_embedder import AbstractEmbedder
from google.genai import types
from google.genai.types import EmbedContentConfig
from google.genai import Client as GoogleGenAIClient
from ..clients.google_clients import (
    GoogleClient,
    GoogleClientConfig,
    GoogleClientType,
)
from ..text_generation.model_engine_exception import ModelEngineException, ErrorDetails
from ..utils import (
    classify_url,
    URLClassification,
    is_standard_web_url,
    fetch_and_encode_image,
    fetch_and_encode_media,
    sniff_image_mime,
    sniff_video_mime,
)


class GoogleGenAiEmbedder(AbstractEmbedder):
    def __init__(
        self,
        model_name: str,
        service_account_credentials: Optional[Dict] = None,
        service_account_key_file: Optional[str] = None,
        region: Optional[str] = None,
        project: Optional[str] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            model_name=model_name,
            max_tokens=kwargs.pop(MAX_TOKENS, None),
            **kwargs,
        )
        self.client_config = GoogleClientConfig(
            type=GoogleClientType.GOOGLE,
            service_account_credentials=service_account_credentials,
            service_account_key_file=service_account_key_file,
            region=region,
            project=project,
            api_key=api_key,
            base_url=base_url,
        )
        self.client = GoogleClient(config=self.client_config).genai_client
        self.model = model_name

    def multi_modal_embeddings(
        self,
        text: Optional[List[str]] = None,
        image: Optional[List[str]] = None,
        video: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> Dict:
        """Public entrypoint mirroring embeddings()/image_embeddings(): returns a dict."""
        response = self.multi_modal_embeddings_call(
            text=text, image=image, video=video, **kwargs
        )
        return response.model_dump()

    def multi_modal_embeddings_call(
        self,
        text: Optional[List[str]] = None,
        image: Optional[List[str]] = None,
        video: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> Union[MultiModalEmbeddingsResponse, ErrorDetails]:
        # multimodalembedding@001 (Vertex) is not served by embed_content -- it
        # needs the Vertex prediction API with {text, image, video} instances.
        if self._uses_vertex_multimodal_api():
            try:
                return self._vertex_multimodal_embeddings_call(
                    text, image, video, **kwargs
                )
            except Exception as e:
                return ModelEngineException(
                    error=e, client="google", model=self.model_name
                ).parse_error()

        text_items, text_parts, text_pending = self._normalize_modality(
            text, lambda s: s
        )
        image_items, image_parts, image_pending = self._normalize_modality(
            image, self._build_image_part
        )
        video_items, video_parts, video_pending = self._normalize_modality(
            video, self._build_video_part
        )

        # One embeddable content per input item, ordered text -> images -> videos
        # so the returned embeddings can be sliced back onto each modality.
        contents = text_parts + image_parts + video_parts

        embedding_config = EmbedContentConfig(
            task_type=kwargs.get("task_type"),
            output_dimensionality=kwargs.get("output_dimensionality"),
            title=kwargs.get("title"),
        )

        try:
            # Nothing survived normalization (or nothing was passed): return the
            # per-modality slots as-is so caller still sees the positioned errors.
            if not contents:
                return MultiModalEmbeddingsResponse(
                    text=text_items, image=image_items, video=video_items
                )

            response = self.client.models.embed_content(
                model=self.model,
                contents=contents,
                config=embedding_config,
            )
            results = response.embeddings or []

            # Slice the flat results back out in the order they were sent.
            n_text, n_image = len(text_parts), len(image_parts)
            prompt_tokens = 0
            prompt_tokens += self._assign_embeddings(
                text_items, text_pending, results[:n_text]
            )
            prompt_tokens += self._assign_embeddings(
                image_items, image_pending, results[n_text : n_text + n_image]
            )
            prompt_tokens += self._assign_embeddings(
                video_items, video_pending, results[n_text + n_image :]
            )

            billable_character_count_meta = (
                response.metadata.billable_character_count
                if response.metadata and response.metadata.billable_character_count
                else 0
            )

            return MultiModalEmbeddingsResponse(
                text=text_items,
                image=image_items,
                video=video_items,
                prompt_tokens=int(prompt_tokens),
                metadata={"billable_character_count": billable_character_count_meta},
            )
        except Exception as e:
            return ModelEngineException(
                error=e, client="google", model=self.model_name
            ).parse_error()

    def _normalize_modality(self, inputs, builder):
        """Build a Part (or pass a string through) for each input, preserving order.

        Returns a tuple of:
          - items: one MultiModalEmbeddingItem per input, in order, pre-filled
            with an error at any position whose builder raised (embedding still
            None, to be filled after the call).
          - parts: the successfully-built parts/strings, ready to send.
          - pending: original indices aligned 1:1 with ``parts`` so results can
            be written back to the right slot.
        """
        items: List[MultiModalEmbeddingItem] = []
        parts: List[Any] = []
        pending: List[int] = []
        for i, raw in enumerate(inputs or []):
            item = MultiModalEmbeddingItem(position=i)
            try:
                parts.append(builder(raw))
                pending.append(i)
            except Exception as e:
                item.error = str(e)
            items.append(item)
        return items, parts, pending

    def _assign_embeddings(self, items, pending, results) -> int:
        """Write embedding results back onto their pending slots; return token count."""
        prompt_tokens = 0
        for pos, result in zip(pending, results):
            item = items[pos]
            item.embedding = result.values if result.values else []
            if result.statistics:
                prompt_tokens += (
                    result.statistics.token_count
                    if result.statistics.token_count
                    else 0
                )
                item.truncated = result.statistics.truncated
        return prompt_tokens

    # ------------------------------------------------------------------
    # Vertex multimodalembedding@001 path
    #
    # This model is served by the Vertex prediction API, not embed_content.
    # Each call embeds a single instance ({text | image | video}), so we make
    # one call per input item and slot the result back at its position.
    # ------------------------------------------------------------------

    def _uses_vertex_multimodal_api(self) -> bool:
        """True when the model must go through Vertex's MultiModalEmbeddingModel."""
        return "multimodalembedding" in (self.model or "").lower()

    def _get_vision_model(self):
        """Lazily init the Vertex vision SDK and load the multimodal embedding model."""
        if getattr(self, "_vision_model", None) is None:
            from ..clients.client_initializer import google_initializer
            from vertexai.vision_models import MultiModalEmbeddingModel

            google_initializer(
                region=self.client_config.region,
                service_account_credentials=self.client_config.service_account_credentials,
                service_account_key_file=self.client_config.service_account_key_file,
                project=self.client_config.project,
            )
            self._vision_model = MultiModalEmbeddingModel.from_pretrained(self.model)
        return self._vision_model

    def _vertex_multimodal_embeddings_call(
        self,
        text: Optional[List[str]] = None,
        image: Optional[List[str]] = None,
        video: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> MultiModalEmbeddingsResponse:
        from vertexai.vision_models import Image as VertexImage, Video as VertexVideo

        model = self._get_vision_model()
        dimension = kwargs.get("output_dimensionality") or kwargs.get("dimension")

        text_items = self._embed_vertex_modality(
            text,
            resolver=lambda t: t if isinstance(t, str) and t.strip() else self._raise(
                "Text input is empty or not a string"
            ),
            embedder=lambda payload: model.get_embeddings(
                contextual_text=payload, dimension=dimension
            ).text_embedding,
        )
        image_items = self._embed_vertex_modality(
            image,
            resolver=lambda i: VertexImage(image_bytes=self._resolve_image_bytes(i)[0]),
            embedder=lambda payload: model.get_embeddings(
                image=payload, dimension=dimension
            ).image_embedding,
        )
        video_items = self._embed_vertex_modality(
            video,
            resolver=lambda v: VertexVideo(video_bytes=self._resolve_video_bytes(v)[0]),
            embedder=lambda payload: self._first_video_embedding(
                model.get_embeddings(video=payload, dimension=dimension)
            ),
        )

        return MultiModalEmbeddingsResponse(
            text=text_items,
            image=image_items,
            video=video_items,
            prompt_tokens=0,
            metadata={"model": self.model, "api": "vertex_multimodal_embedding"},
        )

    def _embed_vertex_modality(self, inputs, resolver, embedder):
        """One MultiModalEmbeddingItem per input; errors (resolve or API) slot in place."""
        items = []
        for i, raw in enumerate(inputs or []):
            item = MultiModalEmbeddingItem(position=i)
            try:
                payload = resolver(raw)
                values = embedder(payload)
                item.embedding = list(values) if values else []
            except Exception as e:
                item.error = str(e)
            items.append(item)
        return items

    @staticmethod
    def _first_video_embedding(response) -> List[float]:
        """Whole-clip vector: take the first returned video segment embedding."""
        segments = response.video_embeddings or []
        return list(segments[0].embedding) if segments else []

    @staticmethod
    def _raise(message: str):
        raise ValueError(message)

    def _resolve_image_bytes(self, img: str) -> Tuple[bytes, str]:
        """Resolve an image input to (raw_bytes, mime_type).

        Supports three input shapes (Files API / GCS URIs are intentionally
        not supported):
          1. data URL base64 (``data:image/png;base64,...``)
          2. raw base64 (no ``data:`` prefix; mime is sniffed from the bytes)
          3. remote http(s) URL (downloaded inline)

        Raises ValueError if the input can't be resolved to image bytes with a
        known mime type.
        """
        if not isinstance(img, str) or not img.strip():
            raise ValueError("Image input is empty or not a string")

        classification = classify_url(img)

        # 1) data:image/<subtype>;base64,<payload>
        if classification == URLClassification.BASE64_IMAGE:
            header, _, payload = img.partition(",")
            mime_match = re.match(r"^data:(image/[a-zA-Z0-9.+-]+);base64$", header)
            image_bytes = base64.b64decode(payload, validate=True)
            mime_type = (
                mime_match.group(1) if mime_match else sniff_image_mime(image_bytes)
            )
            if not mime_type:
                raise ValueError("Could not determine mime type from data URL")
            return image_bytes, mime_type

        # 2) remote http(s) URL -> download inline bytes (no Files API / GCS)
        if classification == URLClassification.WEB_URL:
            b64_data, media_type = fetch_and_encode_image(img)
            image_bytes = base64.b64decode(b64_data)
            mime_type = (
                media_type
                if media_type and media_type.startswith("image/")
                else sniff_image_mime(image_bytes)
            )
            if not mime_type:
                raise ValueError(f"Could not determine mime type for image URL: {img}")
            return image_bytes, mime_type

        # 3) raw base64 with no data URL header -> decode and sniff the mime
        try:
            image_bytes = base64.b64decode(img, validate=True)
        except ValueError as e:
            raise ValueError(
                "Image is not a data URL, http(s) URL, or valid base64"
            ) from e
        mime_type = sniff_image_mime(image_bytes)
        if not mime_type:
            raise ValueError("Could not determine mime type from raw base64 bytes")
        return image_bytes, mime_type

    def _build_image_part(self, img: str) -> types.Part:
        """Normalize an image input into a Google GenAI Part (embed_content path)."""
        image_bytes, mime_type = self._resolve_image_bytes(img)
        return types.Part.from_bytes(data=image_bytes, mime_type=mime_type)

    def _resolve_video_bytes(self, vid: str) -> Tuple[bytes, str]:
        """Resolve a video input to (raw_bytes, mime_type).

        Supports the same three input shapes as _resolve_image_bytes (Files API /
        GCS URIs are intentionally not supported):
          1. data URL base64 (``data:video/mp4;base64,...``)
          2. raw base64 (no ``data:`` prefix; mime is sniffed from the bytes)
          3. remote http(s) URL (downloaded inline)

        Note: classify_url() only recognizes image data URLs, so the data URL
        case is detected here via the ``data:`` prefix directly.

        Raises ValueError if the input can't be resolved to video bytes with a
        known mime type.
        """
        if not isinstance(vid, str) or not vid.strip():
            raise ValueError("Video input is empty or not a string")

        # 1) data:video/<subtype>;base64,<payload>
        if vid.startswith("data:"):
            header, _, payload = vid.partition(",")
            mime_match = re.match(r"^data:(video/[a-zA-Z0-9.+-]+);base64$", header)
            try:
                video_bytes = base64.b64decode(payload, validate=True)
            except ValueError as e:
                raise ValueError("Invalid base64 payload in video data URL") from e
            mime_type = (
                mime_match.group(1) if mime_match else sniff_video_mime(video_bytes)
            )
            if not mime_type:
                raise ValueError("Could not determine mime type from video data URL")
            return video_bytes, mime_type

        # 2) remote http(s) URL -> download inline bytes (no Files API / GCS)
        if is_standard_web_url(vid):
            b64_data, content_type = fetch_and_encode_media(vid)
            video_bytes = base64.b64decode(b64_data)
            mime_type = (
                content_type
                if content_type and content_type.startswith("video/")
                else sniff_video_mime(video_bytes)
            )
            if not mime_type:
                raise ValueError(f"Could not determine mime type for video URL: {vid}")
            return video_bytes, mime_type

        # 3) raw base64 with no data URL header -> decode and sniff the mime
        try:
            video_bytes = base64.b64decode(vid, validate=True)
        except ValueError as e:
            raise ValueError(
                "Video is not a data URL, http(s) URL, or valid base64"
            ) from e
        mime_type = sniff_video_mime(video_bytes)
        if not mime_type:
            raise ValueError("Could not determine mime type from raw base64 bytes")
        return video_bytes, mime_type

    def _build_video_part(self, vid: str) -> types.Part:
        """Normalize a video input into a Google GenAI Part (embed_content path)."""
        video_bytes, mime_type = self._resolve_video_bytes(vid)
        return types.Part.from_bytes(data=video_bytes, mime_type=mime_type)

    def embeddings_call(
        self, strings_to_embed: List[str], **kwargs
    ) -> Union[EmbeddingsModelEngineResponse2, ErrorDetails]:
        embedding_config = EmbedContentConfig(
            task_type=kwargs.get("task_type"),
            output_dimensionality=kwargs.get("output_dimensionality"),
            title=kwargs.get("title"),
        )
        try:
            response = self.client.models.embed_content(
                model=self.model,
                contents=[string for string in strings_to_embed],
                config=embedding_config,
            )
            embedding_results = response.embeddings or []
            prompt_tokens = 0
            embeddings = []
            truncation_meta = []
            billable_character_count_meta = (
                response.metadata.billable_character_count
                if response.metadata and response.metadata.billable_character_count
                else 0
            )

            for e in embedding_results:
                embeddings.append(e.values) if e.values else embeddings.append([])
                if e.statistics:
                    prompt_tokens += (
                        e.statistics.token_count if e.statistics.token_count else 0
                    )
                    truncation_meta.append(e.statistics.truncated)

            return EmbeddingsModelEngineResponse2(
                response=embeddings,
                prompt_tokens=int(prompt_tokens),
                metadata={
                    "truncated": truncation_meta,
                    "billable_character_count": billable_character_count_meta,
                },
            )
        except Exception as e:
            return ModelEngineException(
                error=e, client="google", model=self.model_name
            ).parse_error()

    def _get_tokenizer(self, init_args: Dict) -> AbstractTokenizer:
        return super()._get_tokenizer(init_args)

    def image_embeddings_call(
        self, images_to_embed, **kwargs: Any
    ) -> EmbeddingsModelEngineResponse:
        return super().image_embeddings_call(images_to_embed, **kwargs)
