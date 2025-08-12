from typing import Any


def __getattr__(name: str) -> Any:
    if name == "LocalNER":
        from .local_ner import LocalNER

        return LocalNER

    raise AttributeError(f"Module '{__name__}' has no attribute '{name}'")
