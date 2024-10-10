from abc import ABC, abstractmethod
from typing import Any


class AbstractImageGenerationClient(ABC):
    def __init__(self, **kwargs):
        pass

    # update this to return the response object
    @abstractmethod
    def generate_image(self, *args: Any) -> str:
        pass
