from typing import Any

# register classes and methods
def __getattr__(name: str) -> Any:
    if name == "FAISSDatabase":
        from .faiss.faiss_database import FAISSDatabase
        return FAISSDatabase
    elif name == "extract_text":
        from .utils.extraction.text_extractor import extract_text
        return extract_text
    elif name == "split_text":
        from .utils.text_splitting import split_text
        return split_text