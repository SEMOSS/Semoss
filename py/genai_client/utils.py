from enum import Enum
import re
import base64
from urllib.parse import urlparse


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
