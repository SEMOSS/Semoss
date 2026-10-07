"""Shared attachment type detection; providers choose their own wire format."""

import base64
import codecs
import mimetypes
from pathlib import PurePath
from typing import Iterable, Optional, Tuple

_MIME_ALIASES = {
    "image/jpg": "image/jpeg",
    "text/x-web-markdown": "text/markdown",
    "text/x-markdown": "text/markdown",
    "text/md": "text/markdown",
    "application/markdown": "text/markdown",
    "application/x-markdown": "text/markdown",
}
_TEXT_APPLICATION_TYPES = {
    "application/json",
    "application/ld+json",
    "application/json5",
    "application/x-json5",
    "application/x-ndjson",
    "application/jsonl",
    "application/xml",
    "application/yaml",
    "application/x-yaml",
    "application/toml",
    "application/x-toml",
    "application/javascript",
    "application/x-javascript",
    "application/typescript",
    "application/sql",
    "application/x-sql",
    "application/x-sh",
    "application/x-shellscript",
    "application/x-python",
    "application/x-httpd-php",
    "application/graphql",
}
# Some of these extensions are missing from platform-specific MIME registries.
_TEXT_EXTENSIONS = {
    "txt",
    "text",
    "log",
    "rst",
    "jsonl",
    "ndjson",
    "yaml",
    "yml",
    "toml",
    "ini",
    "cfg",
    "conf",
    "sql",
    "py",
    "js",
    "ts",
    "tsx",
    "jsx",
    "sh",
    "bash",
    "zsh",
    "java",
    "c",
    "h",
    "cpp",
    "hpp",
    "cs",
    "go",
    "rs",
    "rb",
    "php",
    "kt",
    "swift",
    "r",
    "lua",
}
_EXTENSION_MIMES = {
    "md": "text/markdown",
    "markdown": "text/markdown",
    "csv": "text/csv",
    "tsv": "text/tab-separated-values",
    "html": "text/html",
    "htm": "text/html",
    "json": "application/json",
    "xml": "application/xml",
}


def resolve_mime_type(
    mime_type: Optional[str],
    file_name: Optional[str] = None,
    file_format: Optional[str] = None,
) -> str:
    """Canonicalize metadata, inferring only absent or generic MIME types.

    Explicit binary types take precedence over a misleading text extension.
    Unknown files stay binary; decoding successfully is not a text detector.
    """
    mime = (mime_type or "").split(";", 1)[0].strip().lower()
    if mime in {"", "application/octet-stream", "binary/octet-stream"}:
        extension = PurePath(file_name or "").suffix.lstrip(".").lower()
        extension = extension or (file_format or "").lstrip(".").lower()
        if extension in _EXTENSION_MIMES:
            mime = _EXTENSION_MIMES[extension]
        elif extension in _TEXT_EXTENSIONS:
            mime = "text/plain"
        elif extension:
            guessed, encoding = mimetypes.guess_type(f"attachment.{extension}")
            if guessed and not encoding:
                mime = guessed
    if mime in {"jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff"}:
        mime = f"image/{mime}"
    return _MIME_ALIASES.get(mime, mime) or "application/octet-stream"


def is_text_mime_type(mime_type: str) -> bool:
    mime = resolve_mime_type(mime_type)
    # RTF needs document parsing, even when advertised under text/*.
    return (mime.startswith("text/") and mime not in {"text/rtf", "text/richtext"}) or (
        mime in _TEXT_APPLICATION_TYPES
    )


def normalize_text_mime_type(mime_type: str, preserve: Iterable[str] = ()) -> str:
    """Use plain text unless the serving API has special handling for this type."""
    mime = resolve_mime_type(mime_type)
    return "text/plain" if is_text_mime_type(mime) and mime not in preserve else mime


def prepare_base64_media(media, default_mime: Optional[str] = None) -> Tuple[str, str]:
    """Return the base64 payload and MIME without changing shared history."""
    data = media.data
    if not data:
        raise ValueError("Base64 media content requires 'data' field.")
    mime = media.mime_type
    if data.startswith("data:") and ";base64," in data:
        header, data = data.split(";base64,", 1)
        if not mime or resolve_mime_type(mime) == "application/octet-stream":
            mime = header[5:]
    mime = resolve_mime_type(
        mime,
        getattr(media, "file_name", None),
        getattr(media, "format", None),
    )
    if mime == "application/octet-stream" and not media.mime_type and default_mime:
        mime = default_mime
    return data, mime


def decode_base64_text(data: str) -> str:
    """Decode text losslessly, accepting UTF-8 and BOM-marked UTF-16/32."""
    raw = base64.b64decode(data, validate=True)
    if raw.startswith((codecs.BOM_UTF32_LE, codecs.BOM_UTF32_BE)):
        return raw.decode("utf-32")
    if raw.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        return raw.decode("utf-16")
    return raw.decode("utf-8-sig")


def bedrock_document_format(mime_type: str) -> str:
    """Converse expects a document extension, not a MIME subtype."""
    mime = resolve_mime_type(mime_type)
    formats = {
        "application/pdf": "pdf",
        "text/markdown": "md",
        "text/html": "html",
        "text/csv": "csv",
        "application/msword": "doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
        "application/vnd.ms-excel": "xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    }
    if mime in formats:
        return formats[mime]
    return "txt" if is_text_mime_type(mime) else mime.split("/")[-1]
