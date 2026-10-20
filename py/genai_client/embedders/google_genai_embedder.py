from typing import List, Optional, Dict, Any, Union, Tuple
import re, base64
from google.genai import types
from genai_client.tokenizers.abstract_tokenizer import AbstractTokenizer
from ..constants import (
    MAX_TOKENS,
    EmbeddingsModelEngineResponse2,
    MultiModalEmbeddingItem,
    MultiModalEmbeddingsResponse,
)
from .abstract_embedder import AbstractEmbedder
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

    def embeddings_call(
        self, strings_to_embed: List[str], **kwargs
    ) -> Union[EmbeddingsModelEngineResponse2, ErrorDetails]:
        embedding_config = types.EmbedContentConfig(
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

    def multi_modal_embeddings(
        self,
        text: Optional[List[str]] = None,
        image: Optional[List[str]] = None,
        video: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> Dict:
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
        text_items, text_parts, text_pending = self._normalize_modality(
            text, lambda s: s
        )
        image_items, image_parts, image_pending = self._normalize_modality(
            image, self._build_image_part
        )
        video_items, video_parts, video_pending = self._normalize_modality(
            video, self._build_video_part
        )

        embedding_config = types.EmbedContentConfig(
            task_type=kwargs.get("task_type"),
            output_dimensionality=kwargs.get("output_dimensionality"),
            title=kwargs.get("title"),
        )

        try:
            # One embed_content call per item, not one batched call for the whole
            # request: several Gemini embedding models (e.g. gemini-embedding-2)
            # merge every bare part in a batched `contents` list into a single
            # joint embedding instead of returning one embedding per item, which
            # silently drops all but one result. Sending exactly one content per
            # call avoids that regardless of which Google embedding model is
            # configured.
            t_tokens, t_bcc = self._embed_items_individually(
                text_items, text_pending, text_parts, embedding_config
            )
            i_tokens, i_bcc = self._embed_items_individually(
                image_items, image_pending, image_parts, embedding_config
            )
            v_tokens, v_bcc = self._embed_items_individually(
                video_items, video_pending, video_parts, embedding_config
            )

            return MultiModalEmbeddingsResponse(
                text=text_items,
                image=image_items,
                video=video_items,
                prompt_tokens=int(t_tokens + i_tokens + v_tokens),
                metadata={"billable_character_count": t_bcc + i_bcc + v_bcc},
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

    def _embed_items_individually(
        self, items, pending, parts, embedding_config
    ) -> Tuple[int, int]:
        """Embed each part with its own embed_content call, writing the result back
        onto its pending slot.

        A failure on one item's API call (quota, a rejected payload, etc.) is
        recorded as that item's error and does not affect the other items.

        Returns (prompt_tokens, billable_character_count) accumulated across calls.
        """
        prompt_tokens = 0
        billable_character_count = 0
        for pos, part in zip(pending, parts):
            item = items[pos]
            try:
                response = self.client.models.embed_content(
                    model=self.model,
                    contents=[part],
                    config=embedding_config,
                )
                results = response.embeddings or []
                if not results:
                    raise ValueError("No embedding returned for input")
                result = results[0]
                item.embedding = result.values if result.values else []
                if result.statistics:
                    prompt_tokens += (
                        result.statistics.token_count
                        if result.statistics.token_count
                        else 0
                    )
                    item.truncated = result.statistics.truncated
                if response.metadata and response.metadata.billable_character_count:
                    billable_character_count += (
                        response.metadata.billable_character_count
                    )
            except Exception as e:
                item.error = str(e)
        return prompt_tokens, billable_character_count

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

    def _get_tokenizer(self, init_args: Dict) -> AbstractTokenizer:
        return super()._get_tokenizer(init_args)
