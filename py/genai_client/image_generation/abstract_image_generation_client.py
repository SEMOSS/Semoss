from abc import ABC, abstractmethod
from typing import Any, Dict


class AbstractImageGenerationClient(ABC):
    def __init__(self, **kwargs):
        pass

    # update this to return the response object
    @abstractmethod
    def generate_image(self, *args: Any) -> str:
        pass

    def ask(self, *args: Any, **kwargs: Any) -> Dict:
        return self.ask_call(*args, **kwargs)

    @abstractmethod
    def ask_call(self, *args: Any, **kwargs: Any):
        pass
