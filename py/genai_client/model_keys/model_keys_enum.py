from enum import Enum


class ModelKeysEnum(Enum):
    api_key = "api_key", "API key being used for the specific model inference"
    api_base = "api_base", "The base URL for the model inference"
    model = "model", "The name of the model"

    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    # ignore the first param since it's already set by __new__
    def __init__(self, _: str, description: str = None):
        self._description_ = description

    def __str__(self):
        return self.value

    # this makes sure that the description is read-only
    @property
    def description(self):
        return self._description_
