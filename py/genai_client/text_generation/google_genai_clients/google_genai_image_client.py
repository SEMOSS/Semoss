from typing import Optional, Literal
import base64
from pydantic import BaseModel
from google.genai import types
from ...constants import AskModelEngineResponse
from ...message_builders.google_genai.google_genai_models import GoogleRoles as Roles
from ...message_builders.google_genai.google_genai_builder import (
    GoogleGenAIMessageBuilder,
)
from .google_genai_client import GoogleGenAiTextClient
from ...utils import StringEnum


class Models(StringEnum):
    IMAGEN_3 = "imagen-3.0-generate-002"


class OutputFormat(StringEnum):
    PNG = "image/png"
    JPEG = "image/jpeg"


class AspectRatio(StringEnum):
    AUTO = "1:1"
    SQUARE = "3:4"
    RECTANGLE = "4:3"
    PORTRAIT = "9:16"
    LANDSCAPE = "16:9"


class PersonGeneration(StringEnum):
    AUTO = "ALLOW_ADULT"
    DONT_ALLOW = "DONT_ALLOW"
    ALLOW_ALL = "ALLOW_ALL"


class Imagen3Config(BaseModel):
    prompt: str
    model: Literal["imagen-3.0-generate-002"] = "imagen-3.0-generate-002"
    config: Optional[types.GenerateImagesConfig] = None

    class Config:
        use_enum_values = True


class GoogleGenAiImageClient(GoogleGenAiTextClient):

    def ask_call(
        self,
        question: str = None,
        # context: str = None,
        # use_history: bool = True,
        # history: List[Dict] = None,
        **kwargs,
    ):
        if self.client is None:
            raise ValueError("Google Gen AI Image client is not initialized.")

        # self.ask_settings = self.get_ask_settings(
        #     history, use_history, context, **kwargs
        # )

        # Handling new history format through message_json
        # if self.ask_settings.semoss_messages:
        #     msg_history = self._handle_semoss_msgs()

        # # Handling full prompt from Elsa...
        # elif self.ask_settings.full_prompt:
        #     msg_history = self._handle_full_prompt_msgs(**kwargs)

        # # Handling standard ask with question and legacy history
        # else:
        #     msg_history = self._handle_standard_ask(
        #         question=question,
        #         **kwargs,
        #     )
        if not question:
            raise ValueError("A prompt must be provided for image generation.")
        image_config = self._create_image_config(question, **kwargs)

        response = self._create_image(image_config)

        return response

    def _create_image_url(self, mime_type, image_bytes):
        """Creating base64 string URL for generated image from bytes."""
        image_url = (
            f"data:{mime_type};base64,{base64.b64encode(image_bytes).decode('utf-8')}"
        )

        return image_url

    def _create_image(self, image_config) -> AskModelEngineResponse:
        if isinstance(image_config, BaseModel):
            image_config = image_config.model_dump(exclude_none=True)
        try:
            response = self.client.models.generate_images(
                **image_config
            )  # Generating the images

            image_data = [
                self._create_image_url(
                    mime_type=generated_image.image.mime_type,
                    image_bytes=generated_image.image.image_bytes,
                )
                for generated_image in response.generated_images
            ]

            # TODO: Calculate tokens for input_tokens
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
            raise ValueError(f"Error generating image: {e}")

    def _create_image_config(self, question, **kwargs):
        """
        Create the configuration for the Google's image generation request.
        """
        try:
            if self.model_name not in Models.values():
                return kwargs

            model = self.model_name
            number_of_images = kwargs.pop("number_of_images", 1)
            negative_prompt = kwargs.pop("negative_prompt", None)
            output_format = kwargs.pop("output_format", OutputFormat.PNG)
            aspect_ratio = kwargs.pop("aspect_ratio", AspectRatio.AUTO)
            person_generation = kwargs.pop("person_generation", PersonGeneration.AUTO)

            if aspect_ratio is not None and aspect_ratio not in AspectRatio.values():
                aspect_ratio = AspectRatio.AUTO

            if (
                person_generation is not None
                and person_generation not in PersonGeneration.values()
            ):
                person_generation = PersonGeneration.AUTO

            if output_format not in [OutputFormat.PNG, OutputFormat.JPEG]:
                output_format = OutputFormat.PNG

            return Imagen3Config(
                model=model,
                prompt=question,
                config=types.GenerateImagesConfig(
                    number_of_images=number_of_images,
                    negative_prompt=negative_prompt,
                    aspect_ratio=aspect_ratio,
                    output_mime_type=output_format,
                    personGeneration=person_generation,
                ),
            )
        except Exception as e:
            print(f"Error creating image config: {e}")
            raise ValueError(f"Error creating image config: {e}")
