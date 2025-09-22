from typing import Any


def __getattr__(name: str) -> Any:
    if name == "semantic_router":
        from .semantic_routing import semantic_router

        return semantic_router
    else:
        raise AttributeError(f"module {__name__} has no attribute {name}")


__all__ = ["semantic_routing"]
