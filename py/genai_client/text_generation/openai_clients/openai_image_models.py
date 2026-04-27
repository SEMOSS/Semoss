from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from pydantic import BaseModel, ConfigDict


class OpenAIImageTaskType(str, Enum):
    """
    Task types for OpenAI image generation models (gpt-image-1, gpt-image-2,
    dall-e-* legacy). See
    https://developers.openai.com/api/docs/guides/image-generation?api=image
    """

    GENERATE = "GENERATE"
    EDIT = "EDIT"


class _BaseImageConfig(BaseModel):
    """Parameters common to images.generate and images.edit."""

    model: Optional[str] = None
    prompt: str
    n: Optional[int] = None
    size: Optional[str] = None
    quality: Optional[str] = None
    background: Optional[str] = None
    moderation: Optional[str] = None
    output_format: Optional[str] = None
    output_compression: Optional[int] = None
    response_format: Optional[str] = None
    user: Optional[str] = None
    stream: Optional[bool] = None
    partial_images: Optional[int] = None

    # `model` is a real OpenAI parameter; opt out of pydantic's `model_` namespace warning
    # and drop unknown keys instead of raising on forward-compat params.
    model_config = ConfigDict(protected_namespaces=(), extra="ignore")


class ImageGenerationConfig(_BaseImageConfig):
    """Parameters accepted by `client.images.generate(...)`."""


class ImageEditConfig(_BaseImageConfig):
    """Parameters accepted by `client.images.edit(...)`."""

    image: Union[Any, List[Any]]
    mask: Optional[Any] = None
    input_fidelity: Optional[str] = None


_TASK_PARAMS: Dict[OpenAIImageTaskType, Tuple[str, Type[BaseModel]]] = {
    OpenAIImageTaskType.GENERATE: ("generate", ImageGenerationConfig),
    OpenAIImageTaskType.EDIT: ("edit", ImageEditConfig),
}
