from abc import ABC, abstractmethod
from typing import Any


class AbstractSpeechGenerationClient(ABC):
    def __init__(self, **kwargs):
        pass

    # update this to return the response object
    @abstractmethod
    def generate_speech(self, *args: Any) -> str:
        pass
