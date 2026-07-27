from typing import Optional, Tuple
from enum import Enum
import re, json, base64, requests
from urllib.parse import urlparse
from enum import Enum
from pathlib import Path
from functools import wraps


def deprecated(reason: str = "", version: str = ""):
    """Lightweight marker decorator, akin to Java's @Deprecated.

    Records the reason/version on the decorated function or class for
    documentation and introspection but has no runtime behavior (no warning,
    no wrapping). Avoids pulling in the third-party ``deprecated``/``wrapt``
    packages.
    """

    def _decorator(obj):
        try:
            obj.__deprecated__ = {"reason": reason, "version": version}
        except (AttributeError, TypeError):
            pass
        return obj

    return _decorator


class StringEnum(Enum):
    """Base enum that can be compared directly with strings"""

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        return super().__eq__(other)

    def __hash__(self):
        return hash(self.value)

    @classmethod
    def values(cls):
        """Return list of all enum values"""
        return [member.value for member in cls]


def is_base64_image_url(url: str) -> bool:
    """
    Determine if a URL is a base64 encoded image data URL.
    Args:
        url (str): The URL to check
    Returns:
        bool: True if the URL is a base64 encoded image, False otherwise
    """
    if not isinstance(url, str):
        return False

    data_url_pattern = r"^data:image/[a-zA-Z]+;base64,"

    if re.match(data_url_pattern, url):
        try:
            base64_part = url.split(",", 1)[1]
            base64.b64decode(base64_part, validate=True)
            return True
        except (IndexError, ValueError):
            return False

    return False


def is_standard_web_url(url: str) -> bool:
    """
    Determine if a URL is a standard web URL (http/https).
    Args:
        url (str): The URL to check
    Returns:
        bool: True if the URL is a standard web URL, False otherwise
    """
    if not isinstance(url, str):
        return False

    try:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


class URLClassification(StringEnum):
    BASE64_IMAGE = "base64_image"
    WEB_URL = "web_url"
    UNKNOWN = "unknown"


def classify_url(url: str) -> str:
    """
    Classify a URL as either 'base64_image', 'web_url', or 'unknown'.
    Args:
        url (str): The URL to classify
    Returns:
        str: Classification of the URL
    """
    if is_base64_image_url(url):
        return URLClassification.BASE64_IMAGE
    elif is_standard_web_url(url):
        return URLClassification.WEB_URL
    else:
        return URLClassification.UNKNOWN


def get_image_extension(url_or_base64: str) -> Optional[str]:
    """
    Extract the file extension from a base64 encoded image or web URL.

    Args:
        url_or_base64 (str): Either a base64 data URL or a standard web URL

    Returns:
        Optional[str]: The file extension (e.g., 'jpg', 'png', 'gif') or None if not found

    Examples:
        >>> get_image_extension("data:image/jpeg;base64,/9j/4AAQ...")
        'jpg'
        >>> get_image_extension("https://example.com/image.png")
        'png'
        >>> get_image_extension("https://example.com/image.PNG?size=large")
        'png'
    """
    if not isinstance(url_or_base64, str):
        return None

    classification = classify_url(url_or_base64)

    if classification == URLClassification.BASE64_IMAGE:
        return extract_extension_from_base64(url_or_base64)
    elif classification == URLClassification.WEB_URL:
        return _extract_extension_from_web_url(url_or_base64)
    else:
        return None


def extract_extension_from_base64(data_url: str) -> Optional[str]:
    """
    Extract file extension from a base64 data URL.

    Args:
        data_url (str): Base64 data URL (e.g., "data:image/jpeg;base64,...")

    Returns:
        Optional[str]: File extension or None if not found
    """
    try:
        mime_match = re.match(r"^data:image/([a-zA-Z]+);base64,", data_url)
        if not mime_match:
            return None

        mime_subtype = mime_match.group(1).lower()

        mime_to_extension = {
            "jpeg": "jpeg",
            "jpg": "jpeg",
            "png": "png",
            "gif": "gif",
            "webp": "webp",
            "bmp": "bmp",
            "tiff": "tiff",
            "tif": "tiff",
            "svg+xml": "svg",
            "svg": "svg",
            "ico": "ico",
            "x-icon": "ico",
        }

        return mime_to_extension.get(mime_subtype)

    except Exception:
        return None


def _extract_extension_from_web_url(url: str) -> Optional[str]:
    """
    Extract file extension from a web URL.

    Args:
        url (str): Web URL (e.g., "https://example.com/image.jpg")

    Returns:
        Optional[str]: File extension or None if not found
    """
    try:
        parsed = urlparse(url)
        path = parsed.path

        if "." in path:
            extension = path.split(".")[-1].lower()

            extension = extension.split("?")[0].split("#")[0]

            valid_extensions = {
                "jpg",
                "jpeg",
                "png",
                "gif",
                "webp",
                "bmp",
                "tiff",
                "tif",
                "svg",
                "ico",
            }

            if extension in valid_extensions:
                return "jpeg" if extension == "jpg" else extension

        return None

    except Exception:
        return None


def fetch_and_encode_image(url: str) -> Tuple[str, str]:
    """Fetch image from URL and return base64 data with media type"""
    response = requests.get(url)
    response.raise_for_status()

    content_type = response.headers.get("content-type", "")
    if content_type.startswith("image/"):
        media_type = content_type
    else:
        extension = get_image_extension(url)
        media_type = f"image/{extension.lower()}"

    image_data = base64.b64encode(response.content).decode("utf-8")

    return image_data, media_type


def sniff_image_mime(data: bytes) -> Optional[str]:
    """Best-effort detection of an image mime type from magic bytes.

    Useful for raw base64 strings or remotely fetched bytes that arrive with
    no declared mime type. Returns None if the format is unrecognized.
    """
    if not data:
        return None
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data.startswith(b"BM"):
        return "image/bmp"
    if data[:4] in (b"II*\x00", b"MM\x00*"):
        return "image/tiff"
    header = data[:512].lstrip()
    if header.startswith(b"<?xml") or header.startswith(b"<svg"):
        return "image/svg+xml"
    return None


def sniff_video_mime(data: bytes) -> Optional[str]:
    """Best-effort detection of a video mime type from magic bytes.

    Useful for raw base64 strings or remotely fetched bytes that arrive with
    no declared mime type. Returns None if the format is unrecognized. The
    returned strings match the mime types Gemini documents for video input.
    """
    if not data or len(data) < 12:
        return None
    # ISO Base Media Format: 'ftyp' box at offset 4, major brand at 8..12
    # (covers MP4/M4V, QuickTime/MOV, and 3GPP).
    if data[4:8] == b"ftyp":
        brand = data[8:12]
        if brand[:2] == b"qt":
            return "video/mov"
        if brand[:3] in (b"3gp", b"3g2"):
            return "video/3gpp"
        return "video/mp4"
    # Matroska / WebM (EBML header)
    if data.startswith(b"\x1a\x45\xdf\xa3"):
        return "video/webm"
    # AVI (RIFF container with an 'AVI ' form type)
    if data[:4] == b"RIFF" and data[8:12] == b"AVI ":
        return "video/avi"
    # MPEG program stream / video stream start codes
    if data[:4] in (b"\x00\x00\x01\xba", b"\x00\x00\x01\xb3"):
        return "video/mpeg"
    # Flash Video
    if data.startswith(b"FLV"):
        return "video/x-flv"
    # ASF / WMV
    if data.startswith(b"\x30\x26\xb2\x75\x8e\x66\xcf\x11"):
        return "video/wmv"
    return None


def fetch_and_encode_media(url: str) -> Tuple[str, Optional[str]]:
    """Fetch arbitrary media from a URL and return (base64_data, content_type).

    Unlike ``fetch_and_encode_image``, this does not assume an image mime type:
    it returns the server-reported content-type (parameters like ``;charset``
    stripped) or None, leaving format detection to the caller.
    """
    response = requests.get(url)
    response.raise_for_status()

    content_type = response.headers.get("content-type", "") or None
    if content_type:
        content_type = content_type.split(";")[0].strip() or None

    media_data = base64.b64encode(response.content).decode("utf-8")

    return media_data, content_type


def image_to_base64(image_path: str):
    """
    Convert an image file to a base64 encoded string.

    Args:
        image_path (str or Path): Path to the image file

    Returns:
        str: Base64 encoded string of the image

    Raises:
        FileNotFoundError: If the image file doesn't exist
        IOError: If there's an error reading the file
    """
    try:
        path = Path(image_path)

        if not path.exists():
            raise FileNotFoundError(f"Image file not found: {image_path}")

        with open(path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read())
            # Convert bytes to string
            return encoded_string.decode("utf-8")

    except Exception as e:
        raise IOError(f"Error reading image file: {e}")


def string_to_bool(value) -> bool:
    """
    Convert a string representation of a boolean to a boolean value.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        value = value.lower()
        if value in [
            "true",
            "t",
            "yes",
            "y",
            "1",
        ]:
            return True
        else:
            return False
    else:
        instance_type = type(value)
        raise ValueError(
            f"Invalid value type: {instance_type}. Expected str, int, or bool."
        )


def validate_with(model_cls):
    def decorator(init):
        @wraps(init)
        def wrapper(self, *args, **kwargs):
            cfg = model_cls(**kwargs)
            return init(self, **cfg.model_dump())

        return wrapper

    return decorator
