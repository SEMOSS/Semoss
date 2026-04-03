from typing import List, Optional, Dict, Any
from enum import Enum
from pydantic import BaseModel


class NovaCanvasTaskType(str, Enum):
    """
    Task types for Nova Canvas image generation.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    TEXT_IMAGE = "TEXT_IMAGE"
    COLOR_GUIDED_GENERATION = "COLOR_GUIDED_GENERATION"
    IMAGE_VARIATION = "IMAGE_VARIATION"
    INPAINTING = "INPAINTING"
    OUTPAINTING = "OUTPAINTING"
    BACKGROUND_REMOVAL = "BACKGROUND_REMOVAL"
    VIRTUAL_TRY_ON = "VIRTUAL_TRY_ON"


class ImageGenerationConfig(BaseModel):
    """
    Common image generation parameters for Nova Canvas.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    width: Optional[int] = None
    height: Optional[int] = None
    quality: Optional[str] = None
    cfgScale: Optional[float] = None
    seed: Optional[int] = None
    numberOfImages: int = 1


class TextToImageParams(BaseModel):
    """
    Parameters specific to text-to-image generation in Nova Canvas.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    text: str
    negativeText: str
    style: str
    conditionImage: Optional[str] = None
    controlMode: Optional[str] = None
    controlStrength: Optional[float] = None


class ColorGuidedGenerationParams(BaseModel):
    """
    Parameters specific to color-guided image generation in Nova Canvas.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    colors: List[str] # list of color hex codes (e.g. ["#FF0000", "#00FF00", "#0000FF"])
    referenceImage: str # base64-encoded reference image
    text: str
    negativeText: str


class ImageVariationParams(BaseModel):
    """
    Parameters specific to image variation generation in Nova Canvas.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    images: List[str] # list of base64-encoded images to be varied
    similarityStrength: float
    text: str
    negativeText: str

class InpaintingParams(BaseModel):
    """
    Parameters specific to inpainting in Nova Canvas.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    image: str # base64-encoded image to be inpainted
    maskPrompt: str
    maskImage: str # base64-encoded mask image where white pixels indicate areas to be inpainted
    text: str
    negativeText: str


class OutpaintingParams(BaseModel):
    """
    Parameters specific to outpainting in Nova Canvas.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    image: str # base64-encoded image to be outpainted
    maskPrompt: str
    maskImage: str # base64-encoded mask image where white pixels indicate areas to be inpainted
    outPaintingMode: str
    text: str
    negativeText: str

class VirtualTryOnParams(BaseModel):
    """
    Parameters specific to virtual try-on in Nova Canvas.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    sourceImage: str # base64-encoded image
    referenceImage: str # base64-encoded image
    maskType: str
    imageBasedMask: Dict[str, Any] # dictionary containing image-based mask parameters
    garmentBasedMask: Dict[str, Any] # dictionary containing garment-based mask parameters
    promptBasedMask: Dict[str, Any] # dictionary containing prompt-based mask parameters
    maskExclusions: Dict[str, Any] # dictionary containing mask exclusion parameters
    mergeStyle: str
    returnMask: bool


class BackgroundRemovalParams(BaseModel):
    """
    Parameters specific to background removal in Nova Canvas.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    image: str # base64-encoded image for background removal


_TASK_PARAMS: Dict[NovaCanvasTaskType, tuple] = {
    NovaCanvasTaskType.TEXT_IMAGE: ("textToImageParams", TextToImageParams),
    NovaCanvasTaskType.COLOR_GUIDED_GENERATION: ("colorGuidedGenerationParams", ColorGuidedGenerationParams),
    NovaCanvasTaskType.IMAGE_VARIATION: ("imageVariationParams", ImageVariationParams),
    NovaCanvasTaskType.INPAINTING: ("inPaintingParams", InpaintingParams),
    NovaCanvasTaskType.OUTPAINTING: ("outPaintingParams", OutpaintingParams),
    NovaCanvasTaskType.BACKGROUND_REMOVAL: ("backgroundRemovalParams", BackgroundRemovalParams),
    NovaCanvasTaskType.VIRTUAL_TRY_ON: ("virtualTryOnParams", VirtualTryOnParams),
}


def build_nova_canvas_body(
    task_type: str,
    param_map: Dict[str, Any] = None,
) -> Dict[str, Any]:
    task = NovaCanvasTaskType(task_type)
    param_map = param_map or {}

    if task not in _TASK_PARAMS:
        raise ValueError(f"Unsupported Nova Canvas task type: {task_type}")

    param_key, param_cls = _TASK_PARAMS[task]

    body: Dict[str, Any] = {
        "taskType": task.value,
        param_key: param_cls.model_validate({**param_map}).model_dump(exclude_none=True),
    }

    if task != NovaCanvasTaskType.BACKGROUND_REMOVAL:
        body["imageGenerationConfig"] = ImageGenerationConfig.model_validate(param_map).model_dump(exclude_none=True)

    return body
