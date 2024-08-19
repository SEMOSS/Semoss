from abc import ABC, abstractmethod
from typing import Any


class AbstractSpeechGenClient(ABC):
    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def generate_speech(self, *args: Any) -> str:
        pass
