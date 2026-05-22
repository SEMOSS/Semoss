from typing import List, Optional, Dict, Any, Tuple
from enum import Enum
from pydantic import BaseModel, Field


class BedrockImageGenTaskType(str, Enum):
    """
    Task types for Bedrock models image generation.
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
    Common image generation parameters for Bedrock models.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    width: Optional[int] = Field(default=None, alias="imageWidth")
    height: Optional[int] = Field(default=None, alias="imageHeight")
    quality: Optional[str] = None
    cfgScale: Optional[float] = None
    seed: Optional[int] = None
    numberOfImages: int = 1


class TextToImageParams(BaseModel):
    """
    Parameters specific to text-to-image generation in Bedrock models.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    text: str
    negativeText: str = None
    style: Optional[str] = None
    conditionImage: Optional[str] = None
    controlMode: Optional[str] = None
    controlStrength: Optional[float] = None


class ColorGuidedGenerationParams(BaseModel):
    """
    Parameters specific to color-guided image generation in Bedrock models.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    colors: List[str] # list of color hex codes (e.g. ["#FF0000", "#00FF00", "#0000FF"])
    referenceImage: str # base64-encoded reference image
    text: str
    negativeText: str


class ImageVariationParams(BaseModel):
    """
    Parameters specific to image variation generation in Bedrock models.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    images: List[str] # list of base64-encoded images to be varied
    similarityStrength: float
    text: str
    negativeText: str

class InpaintingParams(BaseModel):
    """
    Parameters specific to inpainting in Bedrock models.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    image: str # base64-encoded image to be inpainted
    maskPrompt: str
    maskImage: str # base64-encoded mask image where white pixels indicate areas to be inpainted
    text: str
    negativeText: str
    returnMask: Optional[bool] = None


class OutpaintingParams(BaseModel):
    """
    Parameters specific to outpainting in Bedrock models.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    image: str # base64-encoded image to be outpainted
    maskPrompt: str
    maskImage: str # base64-encoded mask image where white pixels indicate areas to be inpainted
    outPaintingMode: str
    text: str
    negativeText: str
    returnMask: Optional[bool] = None

class VirtualTryOnParams(BaseModel):
    """
    Parameters specific to virtual try-on in Bedrock models.
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
    Parameters specific to background removal in Bedrock models.
    See https://docs.aws.amazon.com/nova/latest/userguide/image-gen-req-resp-structure.html for full usage
    """
    image: str # base64-encoded image for background removal


_TASK_PARAMS: Dict[BedrockImageGenTaskType, Tuple[str, BaseModel]] = {
    BedrockImageGenTaskType.TEXT_IMAGE: ("textToImageParams", TextToImageParams),
    BedrockImageGenTaskType.COLOR_GUIDED_GENERATION: ("colorGuidedGenerationParams", ColorGuidedGenerationParams),
    BedrockImageGenTaskType.IMAGE_VARIATION: ("imageVariationParams", ImageVariationParams),
    BedrockImageGenTaskType.INPAINTING: ("inPaintingParams", InpaintingParams),
    BedrockImageGenTaskType.OUTPAINTING: ("outPaintingParams", OutpaintingParams),
    BedrockImageGenTaskType.BACKGROUND_REMOVAL: ("backgroundRemovalParams", BackgroundRemovalParams),
    BedrockImageGenTaskType.VIRTUAL_TRY_ON: ("virtualTryOnParams", VirtualTryOnParams),
}
