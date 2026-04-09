import json
from datetime import datetime
from typing import Any, Dict, List
import uuid

from pydantic_core import ErrorDetails

from .bedrock_image_gen_models import (
    _TASK_PARAMS,
    BedrockImageGenTaskType,
    ImageGenerationConfig,
)

from .bedrock_client import BedrockClient
from ...message_builders.bedrock.bedrock_message_builder import BedrockMessageBuilder
from ...message_builders.semoss_base.semoss_models import SEMOSSMessagePartType
from ..model_engine_exception import ModelEngineException
from ...constants import AskModelEngineResponse2


class BedrockImageClient(BedrockClient):

    def ask_call(
        self, prefix: str = "", **kwargs
    ) -> AskModelEngineResponse2 | ErrorDetails:
        if self.client is None:
            raise RuntimeError("Bedrock client is not initialized.")

        try:
            semoss_messages = self.build_semoss_messages(
                model_settings=self.model_settings, **kwargs
            )
            try:
                bedrock_request = BedrockMessageBuilder().build_messages(
                    semoss_messages
                )
            except Exception as e:
                raise ValueError(f"Error building Bedrock messages: {str(e)}") from e

            param_map = bedrock_request.get("additionalModelRequestFields", {})

            if (
                hasattr(self.model_settings, "global_param_override")
                and self.model_settings.global_param_override
            ):
                param_map.update(self.model_settings.global_param_override)

            prompt = self._extract_last_input_text(semoss_messages)
            if not prompt:
                raise ValueError("No text prompt found in the input messages.")
            param_map["text"] = prompt

            task_type = param_map.pop("taskType", BedrockImageGenTaskType.TEXT_IMAGE)
            if isinstance(task_type, str):
                try:
                    task_type = BedrockImageGenTaskType(task_type)
                except ValueError as e:
                    raise ValueError(f"Unsupported task type: {task_type}") from e

            body = self.build_request_body(task_type=task_type, param_map=param_map)

            response = self.client.invoke_model(
                body=json.dumps(body),
                accept="application/json",
                contentType="application/json",
                modelId=self.model_id,
            )
            response_body = json.loads(response.get("body").read())

            error = response_body.get("error")
            if error is not None:
                raise Exception(f"Image generation error. Error is {error}")

            raw_images = response_body.get("images", [])
            mime_type = "image/png"

            parts = []
            for raw_b64 in raw_images:
                media_info = self._create_media_info(
                    mime_type=mime_type, base64_data=raw_b64
                )
                parts.append({"type": "MEDIA", "media_info": media_info})

            return AskModelEngineResponse2(
                response="",
                response_tokens=0,
                prompt_tokens=0,
                messageType="CHAT",
                io="OUTPUT",
                parts=parts,
            )

        except Exception as e:
            return ModelEngineException(
                error=e, client="bedrock", model=self.model_id
            ).parse_error()

    @staticmethod
    def _extract_last_input_text(semoss_messages: List) -> str | None:
        """Walk messages in reverse to find the last INPUT message's text part."""
        for msg in reversed(semoss_messages):
            if getattr(msg, "io", None) != "INPUT":
                continue
            # Check parts first (new format)
            parts = getattr(msg, "parts", None)
            if parts:
                for part in reversed(parts):
                    if getattr(part, "type", None) == SEMOSSMessagePartType.TEXT:
                        return part.text
            # Fall back to legacy content field
            content = getattr(msg, "content", None)
            if content:
                return content
        return None

    def _create_media_info(self, mime_type: str, base64_data: str) -> Dict:
        """
        Create a MessageInputMedia-shaped dict for Java to persist into the room folder.
        """
        file_format = mime_type.split("/")[-1]

        file_name = f"genImage_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.{file_format}"

        return {
            "fileName": file_name,
            "base64Data": base64_data,
            "fileFormat": file_format,
            "mimeType": mime_type,
            "mediaInputType": "FILE",
        }

    def build_request_body(
        self,
        task_type: BedrockImageGenTaskType = BedrockImageGenTaskType.TEXT_IMAGE,
        param_map: Dict[str, Any] = None,
    ) -> Dict[str, Any]:

        param_map = param_map or {}
        param_key, param_cls = _TASK_PARAMS[task_type]

        body: Dict[str, Any] = {
            "taskType": task_type.value,
            param_key: param_cls.model_validate({**param_map}).model_dump(
                exclude_none=True
            ),
        }

        if task_type != BedrockImageGenTaskType.BACKGROUND_REMOVAL:
            body["imageGenerationConfig"] = ImageGenerationConfig.model_validate(
                param_map
            ).model_dump(exclude_none=True)

        return body
