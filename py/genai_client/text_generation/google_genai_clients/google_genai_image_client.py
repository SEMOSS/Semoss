from typing import List, Optional, Dict, Literal
import base64
from pydantic import BaseModel, Field
from google.genai import types
from ...utils import classify_url
from ...constants import AskModelEngineResponse
from ...message_builders.google_genai.google_genai_models import GoogleRoles as Roles
from ...message_builders.google_genai.google_genai_builder import (
    GoogleGenAIMessageBuilder,
)
from .google_genai_client import GoogleGenAiTextClient
from ...utils import StringEnum


class Models(StringEnum):
    IMAGEN_3 = "imagen-3.0-generate-002"


class QualityLegacy(StringEnum):
    HD = "hd"
    STANDARD = "standard"


class OutputFormat(StringEnum):
    PNG = "png"
    JPEG = "jpeg"
    WEBP = "webp"


class Imagen3Sizes(StringEnum):
    AUTO = "auto"
    SQUARE = "1024x1024"
    LANDSCAPE = "1536X1024"
    PORTRAIT = "1024x1536"


class Imagen3Config(BaseModel):
    prompt: str
    model: Literal["imagen-3.0-generate-002"] = "imagen-3.0-generate-002"
    config: Optional[types.GenerateImagesConfig] = (None,)
    background: Optional[str] = (None,)
    output_compression: Optional[str] = (None,)
    quality: Optional[QualityLegacy] = None
    size: Optional[Imagen3Sizes] = None
    user: Optional[str] = None

    class Config:
        use_enum_values = True


class GoogleGenAiImageClient(GoogleGenAiTextClient):

    def ask_call(
        self,
        question: str = None,
        **kwargs,
    ):
        if self.client is None:
            raise ValueError("Google Gen AI Image client is not initialized.")

        image_config = self._create_image_config(question, **kwargs)

        response = self._create_image(image_config)

        return response

    def _create_image_url(self, mime_type, image_bytes):
        """Creating Image URL from bytes."""
        image_url = (
            f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('utf-8')}"
        )

        return image_url

    def _create_image(self, image_config) -> AskModelEngineResponse:
        if isinstance(image_config, BaseModel):
            image_config = image_config.model_dump(exclude_none=True)
        try:
            response = self.client.models.generate_images(**image_config)

            image_data = [
                self._create_image_url(
                    mime_type=generated_image.image.mime_type,
                    image_bytes=generated_image.image.image_bytes,
                )
                for generated_image in response.generated_images
            ]

            if self.model_name == Models.IMAGEN_3:
                # TODO: Calculate tokens for Imagen3
                input_tokens = 0
                output_tokens = 0

            model_engine_response = AskModelEngineResponse(
                response=image_data,
                response_tokens=output_tokens,
                prompt_tokens=input_tokens,
                messageType="IMAGE",
            )
            return model_engine_response
        except Exception as e:
            print(f"Error generating image: {e}")
            raise

    def _create_image_config(self, question: str, **kwargs):
        """
        Create the configuration for the OpenAI image generation request.
        """
        # If the model is not an OpenAI model, I have to trust that the correct arguments are passed
        if self.model_name not in Models.values():
            return kwargs

        background = kwargs.pop("background", None)
        model = self.model_name
        number_of_images = kwargs.pop("number_of_images", 1)
        output_compression = kwargs.pop("output_compression", None)
        quality = kwargs.pop("quality", None)
        size = kwargs.pop("size", None)
        user = kwargs.pop("user", None)

        if size is not None and size not in Imagen3Sizes.values():
            size = Imagen3Sizes.AUTO

        return Imagen3Config(
            model=model,
            prompt=question,
            config=types.GenerateImagesConfig(
                number_of_images=number_of_images,
            ),
            background=background,
            output_compression=output_compression,
            quality=quality,
            size=size,
            user=user,
        )
