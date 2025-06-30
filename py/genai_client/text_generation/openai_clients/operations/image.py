from typing import Literal, Optional
from pydantic import BaseModel, Field
from ....constants import AskModelEngineResponse
from ....utils import StringEnum


class Models(StringEnum):
    GPT_IMAGE_1 = "gpt-image-1"
    DALL_E_2 = "dall-e-2"
    DALL_E_3 = "dall-e-3"


# These are the available endpoints for image generation
class ImageAction(StringEnum):
    CREATE = "create"
    EDIT = "edit"
    VARIATION = "variation"


class OutputFormat(StringEnum):
    PNG = "png"
    JPEG = "jpeg"
    WEBP = "webp"


# Moderation is only available for gpt-image-1
class Moderation(StringEnum):
    LOW = "low"
    AUTO = "auto"


# Transparent requires webp or png..
class Background(StringEnum):
    TRANSPARENT = "transparent"
    OPAQUE = "opaque"
    AUTO = "auto"


class Quality(StringEnum):
    AUTO = "auto"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class QualityLegacy(StringEnum):
    HD = "hd"
    STANDARD = "standard"


# This param only available for legacy models (dall-e-2, dall-e-3)
class ResponseFormat(StringEnum):
    URL = "url"
    B64 = "b64_json"


# Only available for dall-e-3
class Style(StringEnum):
    VIVID = "vivid"
    NATURAL = "natural"


class GPT1Sizes(StringEnum):
    AUTO = "auto"
    SQUARE = "1024x1024"
    LANDSCAPE = "1536X1024"
    PORTRAIT = "1024x1536"


class Dalle2Sizes(StringEnum):
    SMALL = "256x256"
    MEDIUM = "512x512"
    LARGE = "1024x1024"


class Dalle3Sizes(StringEnum):
    SQUARE = "1024x1024"
    LANDSCAPE = "1792x1024"
    PORTRAIT = "1024x1792"


# FOR IMAGE CREATION
class GPTImage1Config(BaseModel):
    """
    Configuration for OpenAI Image Generation Request.
    """

    prompt: str
    model: Literal["gpt-image-1"] = "gpt-image-1"
    background: Optional[Background] = None
    moderation: Optional[Moderation] = Moderation.AUTO
    n: int = Field(default=1, ge=1, le=10)
    output_compression: Optional[int] = Field(default=None, ge=1, le=100)
    output_format: Optional[OutputFormat] = OutputFormat.PNG
    quality: Optional[Quality] = Quality.AUTO
    size: Optional[GPT1Sizes] = None
    user: Optional[str] = None

    class Config:
        use_enum_values = True


class Dalle2Config(BaseModel):
    prompt: str
    model: Literal["dall-e-2"] = "dall-e-2"
    n: Optional[int] = Field(default=1, ge=1, le=10)
    size: Optional[Dalle2Sizes] = None
    user: Optional[str] = None
    response_format: Optional[ResponseFormat] = ResponseFormat.B64

    class Config:
        use_enum_values = True


class Dalle3Config(BaseModel):
    prompt: str
    model: Literal["dall-e-3"] = "dall-e-3"
    n: Optional[int] = Field(default=1, ge=1, le=1)
    quality: Optional[QualityLegacy] = None
    size: Optional[Dalle3Sizes] = None
    user: Optional[str] = None
    style: Optional[Style] = None
    response_format: Optional[ResponseFormat] = ResponseFormat.B64

    class Config:
        use_enum_values = True


class Image:

    def __init__(self, client):
        self.client = client

    def ask(
        self,
        question: str = None,
        **kwargs,
    ) -> AskModelEngineResponse:
        model_name = self.client.model_name
        # TODO: Implment the edit & varation actions
        image_action = kwargs.pop("image_action", ImageAction.CREATE)
        if model_name == Models.DALL_E_3 and image_action == ImageAction.EDIT:
            raise ValueError(
                "DALL-E 3 does not support image editing. Use DALL-E 2 or Image-GPT-1 for this."
            )

        image_config = self._create_image_config(question, **kwargs)
        if image_action == ImageAction.CREATE:
            response = self._create_image(image_config)

        return response

    def _create_image(self, image_config) -> AskModelEngineResponse:
        if isinstance(image_config, BaseModel):
            image_config = image_config.model_dump(exclude_none=True)
        try:
            response = self.client.client.images.generate(**image_config)
            response_format = image_config.get("response_format", ResponseFormat.B64)
            if response_format == ResponseFormat.URL:
                image_data = [image.url for image in response.data]
            else:
                image_data = [image.b64_json for image in response.data]

            if self.client.model_name == Models.GPT_IMAGE_1:
                input_tokens = response.usage.input_tokens
                output_tokens = response.usage.output_tokens
            else:
                # TODO: Calculate tokens for DALL-E 2 and DALL-E 3
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
        if self.client.model_name not in Models.values():
            return kwargs

        background = kwargs.pop("background", None)
        model = self.client.model_name
        moderation = kwargs.pop("moderation", Moderation.AUTO)
        n = kwargs.pop("n", 1)
        output_compression = kwargs.pop("output_compression", None)
        output_format = kwargs.pop("output_format", OutputFormat.PNG)
        quality = kwargs.pop("quality", None)
        size = kwargs.pop("size", None)
        user = kwargs.pop("user", None)

        if model == "gpt-image-1":

            if background == "transparent" and output_format not in [
                OutputFormat.PNG,
                OutputFormat.WEBP,
            ]:
                output_format = OutputFormat.WEBP

            if output_compression is not None and output_format not in [
                OutputFormat.JPEG,
                OutputFormat.WEBP,
            ]:
                output_format = OutputFormat.WEBP

            if size is not None and size not in GPT1Sizes.values():
                size = GPT1Sizes.AUTO

            return GPTImage1Config(
                prompt=question,
                background=background,
                moderation=moderation,
                n=n,
                output_compression=output_compression,
                output_format=output_format,
                quality=quality,
                size=size,
                user=user,
            )

        elif model == "dall-e-2":
            response_format = kwargs.pop("response_format", ResponseFormat.B64)

            if size is not None and size not in Dalle2Sizes.values():
                size = Dalle2Sizes.MEDIUM

            return Dalle2Config(
                prompt=question,
                n=n,
                size=size,
                user=user,
                response_format=response_format,
            )

        elif model == "dall-e-3":
            response_format = kwargs.pop("response_format", ResponseFormat.B64)
            style = kwargs.pop("style", None)

            if size is not None and size not in Dalle3Sizes.values():
                size = Dalle3Sizes.SQUARE

            if quality not in QualityLegacy.values():
                quality = None

            # Only one image is allowed to be generated for DALL-E 3
            n = 1

            return Dalle3Config(
                prompt=question,
                n=n,
                quality=quality,
                size=size,
                user=user,
                style=style,
                response_format=response_format,
            )
