import base64
from datetime import datetime
import uuid, time
from typing import Any, Dict, List, Tuple, TYPE_CHECKING
from google.genai.types import (
    GenerateVideosConfig,
    Image,
    VideoGenerationReferenceImage,
)
from ...constants import AskModelEngineResponse2
from genai_client.text_generation.model_engine_exception import ModelEngineException

if TYPE_CHECKING:
    from .google_genai_client import GoogleGenAiTextClient
from ...message_builders.semoss_base.semoss_models import (
    SEMOSSMessage,
    SEMOSSMessagePartType,
    SEMOSSMessagePartType,
)


class GoogleGenAiVideoClient:
    """Client for Google Gen AI Video generation models."""

    parent_client: "GoogleGenAiTextClient"

    def __init__(self, parent_client):
        self.parent_client = parent_client

    def ask_call(
        self,
        semoss_messages: list[SEMOSSMessage],
    ):
        if self.parent_client is None or self.parent_client.google_client is None:
            raise ValueError("Google Gen AI Video client is not initialized.")

        prompt, reference_images, param_map = self._build_from_semoss_messages(
            semoss_messages
        )
        video_config = self._create_video_config(
            reference_images=reference_images, param_map=param_map
        )
        try:
            operation = self.parent_client.google_client.models.generate_videos(
                model=self.parent_client.model_settings.model_name,
                prompt=prompt,
                config=video_config,
            )

            timeout_seconds = 600
            poll_interval = 15
            elapsed = 0
            while not operation.done:
                if elapsed >= timeout_seconds:
                    raise TimeoutError(
                        f"Veo generation did not complete within {timeout_seconds}s"
                    )
                time.sleep(poll_interval)
                elapsed += poll_interval
                operation = self.parent_client.google_client.operations.get(operation)

            if operation.error:
                raise RuntimeError(f"Veo operation failed: {operation.error}")

            if operation.response is None:
                raise ValueError("Operation completed but response is None.")

            videos = operation.response.generated_videos
            semoss_parts: List[Dict[str, Any]] = []
            for generated_video in videos or []:
                video = generated_video.video
                if not video:
                    continue
                semoss_parts.append(
                    {
                        "type": "MEDIA",
                        "media_info": self._create_media_info(
                            raw_bytes=video.video_bytes,
                            mime_type=video.mime_type or "video/mp4",
                        ),
                    }
                )

            if not semoss_parts:
                raise ValueError("No videos were generated in the response.")

            return AskModelEngineResponse2(
                response="",
                prompt_tokens=0,
                response_tokens=0,
                messageType="CHAT",
                io="OUTPUT",
                parts=semoss_parts,
            )

        except Exception as e:
            return ModelEngineException(
                error=e,
                client="google",
                model=self.parent_client.model_settings.model_name,
            ).parse_error()

    def _create_video_config(
        self, reference_images: list[VideoGenerationReferenceImage], param_map: dict
    ) -> GenerateVideosConfig:
        acceptable_params = self._extract_acceptable_params(param_map)
        config = GenerateVideosConfig(
            reference_images=reference_images, **acceptable_params
        )
        return config

    def _build_from_semoss_messages(
        self, semoss_messages: list[SEMOSSMessage]
    ) -> Tuple[str, list[VideoGenerationReferenceImage], dict]:
        last_message = semoss_messages[-1]
        param_map = last_message.param_map
        prompt = ""
        ref_images = []

        for part in last_message.parts or []:
            if getattr(part, "type", None) == SEMOSSMessagePartType.TEXT:
                prompt += getattr(part, "text", "")
            elif getattr(part, "type", None) == SEMOSSMessagePartType.MEDIA:
                media_content = getattr(part, "media_content", [])
                for media in media_content:
                    if not media.is_image():
                        continue
                    ref_images.append(
                        VideoGenerationReferenceImage(
                            image=Image(
                                image_bytes=media.get_bytes(),
                                mime_type=media.mime_type,
                            ),
                        )
                    )

        return prompt, ref_images, param_map

    def _extract_acceptable_params(self, kwargs: dict) -> dict:
        """Take only the keys from kwargs that are acceptable for GenerateVideosConfig."""

        acceptable_keys = set(GenerateVideosConfig.model_fields.keys())
        return {k: v for k, v in kwargs.items() if k in acceptable_keys}

    @staticmethod
    def _create_media_info(raw_bytes: bytes, mime_type: str) -> Dict[str, Any]:
        base64_data = base64.b64encode(raw_bytes).decode("utf-8")

        file_format = mime_type.split("/")[-1]
        file_name = (
            f"genVideo_{datetime.now().strftime('%Y%m%d_%H%M%S')}_"
            f"{uuid.uuid4().hex[:8]}.{file_format}"
        )
        return {
            "fileName": file_name,
            "base64Data": base64_data,
            "fileFormat": file_format,
            "mimeType": mime_type,
            "mediaInputType": "FILE",
        }
