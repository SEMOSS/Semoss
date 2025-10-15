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
    elif name == "create_collection":
        from .utils.chroma_connection import create_collection

        return create_collection
    elif name == "add_document_collection":
        from .utils.chroma_connection import add_document_collection

        return add_document_collection
    elif name == "delete_document_collection":
        from .utils.chroma_connection import delete_document_collection

        return delete_document_collection
    elif name == "search_document_collection":
        from .utils.chroma_connection import search_document_collection

        return search_document_collection