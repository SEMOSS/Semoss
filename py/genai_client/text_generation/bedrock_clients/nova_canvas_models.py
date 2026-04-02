from typing import Optional, Dict, Any
from enum import Enum
from pydantic import BaseModel, model_validator


class NovaCanvasTaskType(str, Enum):
    TEXT_IMAGE = "TEXT_IMAGE"
    # Future task types:
    # IMAGE_VARIATION = "IMAGE_VARIATION"
    # INPAINTING = "INPAINTING"
    # OUTPAINTING = "OUTPAINTING"
    # COLOR_GUIDED_GENERATION = "COLOR_GUIDED_GENERATION"
    # BACKGROUND_REMOVAL = "BACKGROUND_REMOVAL"


class NovaCanvasStyle(str, Enum):
    THREE_D_ANIMATED = "3D_ANIMATED_FAMILY_FILM"
    DESIGN_SKETCH = "DESIGN_SKETCH"
    FLAT_VECTOR = "FLAT_VECTOR_ILLUSTRATION"
    GRAPHIC_NOVEL = "GRAPHIC_NOVEL_ILLUSTRATION"
    MAXIMALISM = "MAXIMALISM"
    MIDCENTURY_RETRO = "MIDCENTURY_RETRO"
    PHOTOREALISM = "PHOTOREALISM"
    SOFT_DIGITAL_PAINTING = "SOFT_DIGITAL_PAINTING"


class NovaCanvasQuality(str, Enum):
    STANDARD = "standard"
    PREMIUM = "premium"


class ImageGenerationConfig(BaseModel):
    width: int = 1024
    height: int = 1024
    quality: NovaCanvasQuality = NovaCanvasQuality.STANDARD
    cfgScale: float = 8.0
    seed: Optional[int] = None
    numberOfImages: int = 1


class TextToImageParams(BaseModel):
    text: str
    negativeText: Optional[str] = None
    style: Optional[NovaCanvasStyle] = None


class TextToImageBody(BaseModel):
    taskType: NovaCanvasTaskType = NovaCanvasTaskType.TEXT_IMAGE
    textToImageParams: TextToImageParams
    imageGenerationConfig: ImageGenerationConfig = ImageGenerationConfig()

    @staticmethod
    def from_params(
        text: str,
        param_map: Dict[str, Any] = None,
    ) -> "TextToImageBody":
        if param_map is None:
            param_map = {}

        text_params = TextToImageParams(
            text=text,
            negativeText=param_map.get("negativeText") or param_map.get("negative_text"),
            style=param_map.get("style"),
        )

        config_kwargs = {}
        if "width" in param_map:
            config_kwargs["width"] = int(param_map["width"])
        if "height" in param_map:
            config_kwargs["height"] = int(param_map["height"])
        if "quality" in param_map:
            config_kwargs["quality"] = param_map["quality"]
        if "cfgScale" in param_map or "cfg_scale" in param_map:
            config_kwargs["cfgScale"] = float(param_map.get("cfgScale") or param_map.get("cfg_scale"))
        if "seed" in param_map:
            config_kwargs["seed"] = int(param_map["seed"])
        if "numberOfImages" in param_map or "number_of_images" in param_map:
            config_kwargs["numberOfImages"] = int(
                param_map.get("numberOfImages") or param_map.get("number_of_images")
            )

        return TextToImageBody(
            textToImageParams=text_params,
            imageGenerationConfig=ImageGenerationConfig(**config_kwargs),
        )

    def to_json(self) -> str:
        return self.model_dump_json(exclude_none=True)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(exclude_none=True)


def build_nova_canvas_body(
    task_type: str,
    text: str,
    param_map: Dict[str, Any] = None,
) -> Dict[str, Any]:
    task = NovaCanvasTaskType(task_type)

    if task == NovaCanvasTaskType.TEXT_IMAGE:
        return TextToImageBody.from_params(text=text, param_map=param_map).to_dict()

    raise ValueError(f"Unsupported Nova Canvas task type: {task_type}")
