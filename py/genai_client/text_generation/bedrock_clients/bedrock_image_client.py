import base64
import json
from datetime import datetime
from typing import Dict
import uuid

from pydantic_core import ErrorDetails

from .bedrock_client import BedrockClient
from botocore.exceptions import ClientError
from ...message_builders.bedrock.bedrock_message_builder import BedrockMessageBuilder
from ..model_engine_exception import ModelEngineException
from ...constants import AskModelEngineResponse2

class BedrockImageClient(BedrockClient):

    def ask_call(self, prefix: str = "", **kwargs,) -> AskModelEngineResponse2 | ErrorDetails:
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

            # body = {}
            body = json.dumps({ #TODO: test body, remove
                "taskType": "TEXT_IMAGE",
                "textToImageParams": {
                    "text": "A consultant grimacing at a computer screen, surrounded by paperwork, in a dimly lit office.",
                },
                "imageGenerationConfig": {
                    "numberOfImages": 1,
                    "height": 1024,
                    "width": 1024,
                    "cfgScale": 8.0,
                    "seed": 42,
                }
            })
            accept = "application/json"
            content_type = "application/json"
            model_id = self.model_id
            response = self.client.invoke_model(body=body, accept=accept, contentType=content_type, modelId=model_id)
            response_body = json.loads(response.get("body").read())
            mime_type = f"image/png"

            base64_image = response_body.get("images")[0]
            base64_bytes = base64_image.encode('ascii')
            image_bytes = base64.b64decode(base64_bytes)

            finish_reason = response_body.get("error")

            if finish_reason is not None:
                raise Exception(f"Image generation error. Error is {finish_reason}")

        except Exception as e:
            return ModelEngineException(
                error=e, client="bedrock", model=self.model_id
            ).parse_error()

        parts = []

        image_data = []
        image_data.append(self._create_media_info(mime_type=mime_type, image_bytes=image_bytes))

        for media_info in image_data or []:
                parts.append({"type": "MEDIA", "media_info": media_info})

        # messageType = "IMAGE"
        return AskModelEngineResponse2(
            response="",
            response_tokens=0,
            prompt_tokens=0,
            messageType="CHAT", # Get image working
            io="OUTPUT",
            parts=parts,
        )
    
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