import base64
import json
from datetime import datetime
from typing import Dict, List
import uuid

from pydantic_core import ErrorDetails

from .bedrock_client import BedrockClient
from .nova_canvas_models import build_nova_canvas_body, NovaCanvasTaskType
from ...message_builders.bedrock.bedrock_message_builder import BedrockMessageBuilder
from ...message_builders.semoss_base.semoss_models import SEMOSSMessagePartType
from ..model_engine_exception import ModelEngineException
from ...constants import AskModelEngineResponse2


class BedrockImageClient(BedrockClient):

    def ask_call(self, prefix: str = "", **kwargs) -> AskModelEngineResponse2 | ErrorDetails:
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

            # Extract text prompt from the last input message's text part
            prompt = self._extract_last_input_text(semoss_messages)
            if not prompt:
                raise ValueError("No text prompt found in the input messages.")

            task_type = param_map.pop("taskType", None) or param_map.pop("task_type", NovaCanvasTaskType.TEXT_IMAGE.value)
            body = build_nova_canvas_body(
                task_type=task_type,
                text=prompt,
                param_map=param_map,
            )

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
                image_bytes = base64.b64decode(raw_b64.encode("ascii"))
                media_info = self._create_media_info(mime_type=mime_type, image_bytes=image_bytes)
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
    
    def _create_media_info(self, mime_type: str, image_bytes: bytes) -> Dict:
        """
        Create a MessageInputMedia-shaped dict for Java to persist into the room folder.
        """
        file_format = mime_type.split("/")[-1]

        base64_data = base64.b64encode(image_bytes).decode("utf-8")
        file_name = f"genImage_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.{file_format}"

        return {
            "fileName": file_name,
            "base64Data": base64_data,
            "fileFormat": file_format,
            "mimeType": mime_type,
            "mediaInputType": "FILE",
        }