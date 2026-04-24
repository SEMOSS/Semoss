import base64
import io
import logging
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from smss_thread_local import get_smss_stream

logger = logging.getLogger("SocketServer")

from .openai_image_models import (
    _TASK_PARAMS,
    ImageEditConfig,
    ImageGenerationConfig,
    OpenAIImageTaskType,
)
from ...constants import AskModelEngineResponse, AskModelEngineResponse2

if TYPE_CHECKING:
    from .openai_client import OpenAiClient
from ...message_builders.semoss_base.semoss_models import (
    SEMOSSMediaInputType,
    SEMOSSMessage,
    SEMOSSMessagePartType,
)
from ...message_builders.semoss_base.semoss_streaming_util import StreamUtil
from ...utils import string_to_bool
from ..model_engine_exception import ErrorDetails, ModelEngineException


class OpenAiImageClient:
    """OpenAI image generation client (gpt-image-1, gpt-image-2, dall-e-*).

    Mirrors the structure of `BedrockImageClient`: dispatches by task type,
    builds a typed request via pydantic, and returns an
    `AskModelEngineResponse2` whose `parts` carry the generated MEDIA so
    Java can persist the file.

    Conversation history: unlike Bedrock (which only forwards the last
    INPUT text), this client concatenates the text of every prior INPUT
    turn into a single prompt, and forwards any base64 images attached to
    the conversation as edit inputs when present.
    """
    client: "OpenAiClient"

    def __init__(self, client):
        # `client` is the parent OpenAiClient; we reach through it for
        # `client.client` (the openai SDK) and `client.model_settings`.
        self.client = client

    def ask(
        self, semoss_messages: List[SEMOSSMessage], prefix: str = "", **kwargs
    ) -> Dict[str, Any]:
        response = self.ask_call(semoss_messages, prefix=prefix, **kwargs)
        if isinstance(response, AskModelEngineResponse2):
            return response.model_dump(exclude_none=True)
        if isinstance(response, AskModelEngineResponse):
            return response.to_dict()
        if isinstance(response, ErrorDetails):
            return response.model_dump()
        raise ValueError("Invalid response type from ask_call.")

    def ask_call(
        self,
        semoss_messages: List[SEMOSSMessage],
        prefix: str = "",
        **kwargs,
    ) -> Union[AskModelEngineResponse2, ErrorDetails]:
        try:
            param_map = self._clean_param_map(kwargs)

            model_settings = self.client.model_settings
            if getattr(model_settings, "global_param_override", None):
                param_map.update(model_settings.global_param_override)

            prompt = self._build_prompt(semoss_messages)
            if not prompt:
                raise ValueError("No text prompt found in the input messages.")
            param_map["prompt"] = prompt

            input_images = self._extract_input_images(semoss_messages)

            task_type = self._resolve_task_type(
                param_map.pop("image_action", None), input_images
            )

            param_map.setdefault("model", model_settings.model_name)

            # Default stream on to match chat-completion/responses defaults.
            stream = self._resolve_bool(param_map.pop("stream", True), default=True)
            param_map["stream"] = stream

            if task_type == OpenAIImageTaskType.GENERATE:
                api_response = self._generate_image(param_map)
            elif task_type == OpenAIImageTaskType.EDIT:
                api_response = self._edit_image(param_map, input_images)
            else:
                raise NotImplementedError(
                    f"Image action '{task_type}' is not implemented yet."
                )

            if stream:
                return self._handle_streaming(api_response, param_map, prefix=prefix)
            return self._build_response(api_response, param_map)
        except Exception as e:
            return ModelEngineException(
                error=e,
                client="openai",
                model=self.client.model_settings.model_name,
            ).parse_error()

    # --------------------------- task dispatch ---------------------------

    def _generate_image(self, param_map: Dict[str, Any]):
        _, config_cls = _TASK_PARAMS[OpenAIImageTaskType.GENERATE]
        config: ImageGenerationConfig = config_cls.model_validate(param_map)
        return self.client.client.images.generate(
            **config.model_dump(exclude_none=True)
        )

    def _edit_image(self, param_map: Dict[str, Any], input_images: List[bytes]):
        if not input_images:
            raise ValueError("Edit requires at least one input image in the conversation.")

        _, config_cls = _TASK_PARAMS[OpenAIImageTaskType.EDIT]
        config: ImageEditConfig = config_cls.model_validate(
            {"image": input_images, **param_map}
        )

        request = config.model_dump(exclude_none=True)
        request["image"] = self._to_file_objects(input_images)
        if request.get("mask") is not None:
            request["mask"] = self._to_file_objects(request["mask"])
        return self.client.client.images.edit(**request)

    @staticmethod
    def _resolve_task_type(
        raw_action: Optional[str], input_images: List[bytes]
    ) -> OpenAIImageTaskType:
        if raw_action is None:
            return (
                OpenAIImageTaskType.EDIT
                if input_images
                else OpenAIImageTaskType.GENERATE
            )
        if isinstance(raw_action, OpenAIImageTaskType):
            return raw_action
        normalized = str(raw_action).strip().upper()
        # Backwards compat: prior callers passed image_action="create".
        if normalized == "CREATE":
            normalized = OpenAIImageTaskType.GENERATE.value
        try:
            return OpenAIImageTaskType(normalized)
        except ValueError as e:
            raise ValueError(f"Unsupported image_action: {raw_action}") from e

    # --------------------------- response shaping ---------------------------

    def _build_response(
        self, api_response: Any, param_map: Dict[str, Any]
    ) -> AskModelEngineResponse2:
        response_format = param_map.get("response_format")
        mime_type = self._mime_type_from_param(param_map)

        parts: List[Dict[str, Any]] = []
        for image in getattr(api_response, "data", []) or []:
            url = getattr(image, "url", None)
            b64 = getattr(image, "b64_json", None)

            if response_format == "url" and url:
                parts.append(
                    {
                        "type": "MEDIA",
                        "media_info": {
                            "url": url,
                            "mediaInputType": "URL",
                            "mimeType": mime_type,
                        },
                    }
                )
            elif b64:
                parts.append(
                    {
                        "type": "MEDIA",
                        "media_info": self._create_media_info(
                            mime_type=mime_type, base64_data=b64
                        ),
                    }
                )

        prompt_tokens, response_tokens = self._extract_usage(api_response)

        return AskModelEngineResponse2(
            response="",
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            messageType="CHAT",
            io="OUTPUT",
            parts=parts,
        )

    def _handle_streaming(
        self,
        stream_response: Any,
        param_map: Dict[str, Any],
        prefix: str = "",
    ) -> AskModelEngineResponse2:
        """Consume an OpenAI image stream, forwarding partial images to the
        SEMOSS stream and returning the completed image in the final
        AskModelEngineResponse2.

        Event types emitted by the SDK (both generate and edit):
          - image_generation.partial_image / image_edit.partial_image
              -> b64_json, partial_image_index, output_format, ...
          - image_generation.completed / image_edit.completed
              -> b64_json, output_format, usage
        """
        smss_stream = get_smss_stream()
        parts: List[Dict[str, Any]] = []
        prompt_tokens = 0
        response_tokens = 0
        final_emitted = False

        for event in stream_response:
            event_type = getattr(event, "type", "") or ""
            b64 = getattr(event, "b64_json", None)
            if not b64:
                continue

            mime_type = self._mime_type_from_event(event, param_map)

            if event_type.endswith(".partial_image"):
                media_info = self._create_media_info(
                    mime_type=mime_type, base64_data=b64
                )
                partial_idx = getattr(event, "partial_image_index", None)
                data = StreamUtil.create_media_chunk(
                    media_info=media_info, partial_image_index=partial_idx
                )
                if smss_stream is not None:
                    smss_stream(data, stream_type="media")
                logger.info("%spartial_image_index=%s", prefix, partial_idx)

            elif event_type.endswith(".completed"):
                media_info = self._create_media_info(
                    mime_type=mime_type, base64_data=b64
                )
                data = StreamUtil.create_media_chunk(media_info=media_info)
                if smss_stream is not None:
                    smss_stream(data, stream_type="media", interim=False)
                logger.info("%simage_completed", prefix)

                parts.append({"type": "MEDIA", "media_info": media_info})
                prompt_tokens, response_tokens = self._extract_usage(event)
                final_emitted = True

        if not final_emitted:
            # Stream closed without a completed event; still signal finish so
            # downstream consumers don't hang.
            if smss_stream is not None:
                finish = StreamUtil.create_finish_reason_chunk("stop")
                smss_stream(finish, stream_type="media", interim=False)

        return AskModelEngineResponse2(
            response="",
            prompt_tokens=prompt_tokens,
            response_tokens=response_tokens,
            messageType="CHAT",
            io="OUTPUT",
            parts=parts,
        )

    def _mime_type_from_event(self, event: Any, param_map: Dict[str, Any]) -> str:
        fmt = getattr(event, "output_format", None) or param_map.get("output_format")
        if not fmt:
            return "image/png"
        return f"image/{str(fmt).lower()}"

    @staticmethod
    def _resolve_bool(value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            try:
                return string_to_bool(value)
            except ValueError:
                return default
        return bool(value)

    def _extract_usage(self, api_response: Any) -> tuple[int, int]:
        usage = getattr(api_response, "usage", None)
        if usage is None:
            return 0, 0
        return (
            getattr(usage, "input_tokens", 0) or 0,
            getattr(usage, "output_tokens", 0) or 0,
        )

    # --------------------------- message extraction ---------------------------

    @staticmethod
    def _build_prompt(semoss_messages: List[SEMOSSMessage]) -> Optional[str]:
        """Concatenate text from every INPUT turn so the model gets the
        full conversation context, not just the latest user message."""
        chunks: List[str] = []
        for msg in semoss_messages:
            if getattr(msg, "io", None) != "INPUT":
                continue
            parts = getattr(msg, "parts", None)
            if parts:
                for part in parts:
                    if getattr(part, "type", None) == SEMOSSMessagePartType.TEXT:
                        if part.text:
                            chunks.append(part.text)
            else:
                content = getattr(msg, "content", None)
                if content:
                    chunks.append(content)
        if not chunks:
            return None
        return "\n".join(chunks)

    @staticmethod
    def _extract_input_images(semoss_messages: List[SEMOSSMessage]) -> List[bytes]:
        """Decode every base64 image attached to the conversation."""
        images: List[bytes] = []
        for msg in semoss_messages:
            parts = getattr(msg, "parts", None)
            if not parts:
                continue
            for part in parts:
                if getattr(part, "type", None) != SEMOSSMessagePartType.MEDIA:
                    continue
                media = part.media_info
                if media.type != SEMOSSMediaInputType.BASE64 or not media.data:
                    continue
                if media.mime_type and not media.mime_type.startswith("image"):
                    continue
                try:
                    images.append(base64.b64decode(media.data))
                except Exception:
                    continue
        return images

    # --------------------------- helpers ---------------------------

    @staticmethod
    def _to_file_objects(image_bytes):
        if isinstance(image_bytes, list):
            return [OpenAiImageClient._wrap_bytes(b, i) for i, b in enumerate(image_bytes)]
        return OpenAiImageClient._wrap_bytes(image_bytes, 0)

    @staticmethod
    def _wrap_bytes(data: bytes, idx: int):
        bio = io.BytesIO(data)
        bio.name = f"image_{idx}.png"
        return bio

    @staticmethod
    def _mime_type_from_param(param_map: Dict[str, Any]) -> str:
        fmt = (param_map.get("output_format") or "png").lower()
        return f"image/{fmt}"

    @staticmethod
    def _create_media_info(mime_type: str, base64_data: str) -> Dict[str, Any]:
        file_format = mime_type.split("/")[-1]
        file_name = (
            f"genImage_{datetime.now().strftime('%Y%m%d_%H%M%S')}_"
            f"{uuid.uuid4().hex[:8]}.{file_format}"
        )
        return {
            "fileName": file_name,
            "base64Data": base64_data,
            "fileFormat": file_format,
            "mimeType": mime_type,
            "mediaInputType": "FILE",
        }

    @staticmethod
    def _clean_param_map(param_map: Dict[str, Any]) -> Dict[str, Any]:
        params = param_map.copy()
        for key in (
            "message_json",
            "tools",
            "tool_choice",
            "stream_options",
            "system_prompt",
            "history",
            "use_history",
            "model_name",
            "max_tokens",
            "max_new_tokens",
            "max_completion_tokens",
            "max_output_tokens",
            "thinking",
            "thinking_budget",
            "chat_type",
        ):
            params.pop(key, None)
        return params
