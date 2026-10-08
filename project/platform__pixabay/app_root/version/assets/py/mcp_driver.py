"""SEMOSS MCP tools for searching Pixabay images.

Configuration
-------------
Set ``API_KEY`` in ``platform__pixabay.smss.local`` beside this project's SMSS
file to keep credentials outside Git. The driver reads that local override,
then the project's SMSS file, then the ``PIXABAY_API_KEY`` environment variable
on every call. Get a key from https://pixabay.com/api/docs/.

Pixabay requires API responses to be cached for 24 hours. This driver stores a
small JSON cache under the current app/runtime root and never writes the API key
to that cache.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from smssutil import mcp_metadata, smss_get_runtime_var


_PIXABAY_API_URL = "https://pixabay.com/api/"
_CACHE_TTL_SECONDS = 24 * 60 * 60
_ALLOWED_IMAGE_TYPES = {"all", "photo", "illustration", "vector"}
_ALLOWED_ORIENTATIONS = {"all", "horizontal", "vertical"}
_ALLOWED_ORDERS = {"popular", "latest"}
_ALLOWED_CATEGORIES = {
    "backgrounds",
    "fashion",
    "nature",
    "science",
    "education",
    "feelings",
    "health",
    "people",
    "religion",
    "places",
    "animals",
    "industry",
    "computer",
    "food",
    "sports",
    "transportation",
    "travel",
    "buildings",
    "business",
    "music",
}
_ALLOWED_COLORS = {
    "grayscale",
    "transparent",
    "red",
    "orange",
    "yellow",
    "green",
    "turquoise",
    "blue",
    "lilac",
    "pink",
    "white",
    "gray",
    "black",
    "brown",
}
_ALLOWED_LANGUAGES = {
    "cs",
    "da",
    "de",
    "en",
    "es",
    "fr",
    "id",
    "it",
    "hu",
    "nl",
    "no",
    "pl",
    "pt",
    "ro",
    "sk",
    "fi",
    "sv",
    "tr",
    "vi",
    "th",
    "bg",
    "ru",
    "el",
    "ja",
    "ko",
    "zh",
}


def _read_smss_api_key(smss_path: Path) -> str:
    """Read a single-line API_KEY property without exposing other settings."""
    api_key = ""
    try:
        with smss_path.open(encoding="utf-8-sig") as smss_file:
            for line in smss_file:
                # Accept the single-line SMSS property formats API_KEY<tab>value,
                # API_KEY value, API_KEY=value, and API_KEY:value.
                match = re.match(
                    r"^[ \t\f]*API_KEY(?:[ \t\f]*[=:][ \t\f]*|[ \t\f]+|$)(.*)$",
                    line.rstrip("\r\n"),
                )
                if match:
                    # Match Java properties' last-entry-wins behavior.
                    api_key = match.group(1).strip()
    except (OSError, UnicodeError):
        # Some deployments only expose app assets to the Python runtime.
        return ""
    return api_key


def _get_api_key() -> str:
    """Read this project's local override, SMSS, then runtime environment."""
    # Resolve from the driver itself so a caller's app context or working
    # directory cannot select a different project's SMSS file.
    project_dir = Path(__file__).resolve().parents[4]
    smss_path = project_dir.with_name(project_dir.name + ".smss")
    for path in (smss_path.with_name(smss_path.name + ".local"), smss_path):
        api_key = _read_smss_api_key(path)
        if api_key:
            return api_key
    return os.getenv("PIXABAY_API_KEY", "").strip()


def _runtime_root() -> Path:
    """Return a writable location associated with the active app/runtime."""
    root = smss_get_runtime_var("APP_ROOT") or smss_get_runtime_var("ROOT")
    if root:
        return Path(root)
    return Path(tempfile.gettempdir()) / "semoss_pixabay_mcp"


def _cache_path() -> Path:
    return _runtime_root() / ".cache" / "pixabay_api_cache.json"


def _read_cache() -> Dict[str, Dict[str, Any]]:
    path = _cache_path()
    if not path.exists():
        return {}
    try:
        content = json.loads(path.read_text(encoding="utf-8"))
        return content if isinstance(content, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _write_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    path = _cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
    os.replace(temporary, path)


def _cache_key(params_without_key: Dict[str, Any]) -> str:
    canonical = json.dumps(params_without_key, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_choice(name: str, value: str, choices: set[str]) -> str:
    normalized = value.strip().lower()
    if normalized not in choices:
        allowed = ", ".join(sorted(choices))
        raise ValueError(f"{name} must be one of: {allowed}")
    return normalized


def _validate_colors(colors: str) -> str:
    if not colors.strip():
        return ""
    values = [value.strip().lower() for value in colors.split(",") if value.strip()]
    invalid = sorted(set(values) - _ALLOWED_COLORS)
    if invalid:
        raise ValueError(
            "Unsupported color value(s): "
            + ", ".join(invalid)
            + ". Allowed values: "
            + ", ".join(sorted(_ALLOWED_COLORS))
        )
    return ",".join(dict.fromkeys(values))


def _select_image_fields(hit: Dict[str, Any]) -> Dict[str, Any]:
    """Return useful fields while preserving Pixabay attribution links."""
    return {
        "id": hit.get("id"),
        "page_url": hit.get("pageURL"),
        "type": hit.get("type"),
        "tags": hit.get("tags"),
        "preview_url": hit.get("previewURL"),
        "preview_width": hit.get("previewWidth"),
        "preview_height": hit.get("previewHeight"),
        "webformat_url": hit.get("webformatURL"),
        "webformat_width": hit.get("webformatWidth"),
        "webformat_height": hit.get("webformatHeight"),
        "large_image_url": hit.get("largeImageURL"),
        "image_width": hit.get("imageWidth"),
        "image_height": hit.get("imageHeight"),
        "image_size_bytes": hit.get("imageSize"),
        "views": hit.get("views"),
        "downloads": hit.get("downloads"),
        "collections": hit.get("collections"),
        "likes": hit.get("likes"),
        "comments": hit.get("comments"),
        "creator_id": hit.get("user_id"),
        "creator_name": hit.get("user"),
        "creator_profile_url": hit.get("userImageURL"),
    }


def _fetch_pixabay(params_without_key: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    key = _cache_key(params_without_key)
    now = time.time()
    cache = _read_cache()
    entry = cache.get(key)

    if isinstance(entry, dict):
        cached_at = entry.get("cached_at", 0)
        payload = entry.get("payload")
        if isinstance(cached_at, (int, float)) and now - cached_at < _CACHE_TTL_SECONDS:
            if isinstance(payload, dict):
                return {"payload": payload, "cached": True, "cached_at": cached_at}

    request_params = dict(params_without_key)
    request_params["key"] = api_key
    request = Request(
        _PIXABAY_API_URL + "?" + urlencode(request_params),
        headers={
            "Accept": "application/json",
            "User-Agent": "SEMOSS-Pixabay-MCP/1.0",
        },
        method="GET",
    )

    try:
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        try:
            detail = error.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            detail = ""
        message = f"Pixabay API returned HTTP {error.code}."
        if detail:
            message += f" Response: {detail}"
        raise RuntimeError(message) from error
    except URLError as error:
        raise RuntimeError(f"Could not reach the Pixabay API: {error.reason}") from error

    if not isinstance(payload, dict) or not isinstance(payload.get("hits"), list):
        raise RuntimeError("Pixabay returned an unexpected response format.")

    # Drop stale entries so the cache file remains bounded, then save this response.
    fresh_cache = {
        cache_key: cache_entry
        for cache_key, cache_entry in cache.items()
        if isinstance(cache_entry, dict)
        and isinstance(cache_entry.get("cached_at"), (int, float))
        and now - cache_entry["cached_at"] < _CACHE_TTL_SECONDS
    }
    fresh_cache[key] = {"cached_at": now, "payload": payload}
    try:
        _write_cache(fresh_cache)
    except OSError:
        # A cache write problem should not discard an otherwise valid API response.
        pass

    return {"payload": payload, "cached": False, "cached_at": now}


@mcp_metadata(
    {
        "execution": "auto",
        "loadingMessage": "Searching Pixabay for images...",
        "displayLocation": "inline",
        "resourceURI": None,
    }
)
def search_pixabay_images(
    query: str,
    image_type: str = "all",
    orientation: str = "all",
    category: str = "",
    min_width: int = 0,
    min_height: int = 0,
    colors: str = "",
    editors_choice: bool = False,
    safesearch: bool = True,
    order: str = "popular",
    page: int = 1,
    per_page: int = 20,
    language: str = "en",
) -> str:
    """Search Pixabay for images and return image URLs, metadata, and attribution links.

    Args:
        query: URL-search-style keywords, up to 100 characters.
        image_type: all, photo, illustration, or vector.
        orientation: all, horizontal, or vertical.
        category: Optional Pixabay category such as nature, people, or business.
        min_width: Minimum image width in pixels.
        min_height: Minimum image height in pixels.
        colors: Optional comma-separated colors, for example "blue,green".
        editors_choice: Only return images selected by Pixabay editors.
        safesearch: Only return images suitable for all ages.
        order: Sort by popular or latest.
        page: Results page, beginning at 1.
        per_page: Results per page, from 3 through 200.
        language: Pixabay query language code, such as en, es, de, or ja.
    """
    api_key = ""
    try:
        api_key = _get_api_key()
        if not api_key:
            raise RuntimeError(
                "No Pixabay API key is configured. Add API_KEY to this project's "
                "SMSS or SMSS.local file and ensure the Python runtime can read it, or set the "
                "PIXABAY_API_KEY environment variable."
            )

        normalized_query = query.strip()
        if not normalized_query:
            raise ValueError("query must not be empty")
        if len(normalized_query) > 100:
            raise ValueError("query must be 100 characters or fewer")
        if min_width < 0 or min_height < 0:
            raise ValueError("min_width and min_height must be zero or greater")
        if page < 1:
            raise ValueError("page must be at least 1")
        if per_page < 3 or per_page > 200:
            raise ValueError("per_page must be between 3 and 200")

        normalized_image_type = _validate_choice(
            "image_type", image_type, _ALLOWED_IMAGE_TYPES
        )
        normalized_orientation = _validate_choice(
            "orientation", orientation, _ALLOWED_ORIENTATIONS
        )
        normalized_order = _validate_choice("order", order, _ALLOWED_ORDERS)
        normalized_language = _validate_choice(
            "language", language, _ALLOWED_LANGUAGES
        )
        normalized_category = category.strip().lower()
        if normalized_category and normalized_category not in _ALLOWED_CATEGORIES:
            raise ValueError(
                "category must be empty or one of: "
                + ", ".join(sorted(_ALLOWED_CATEGORIES))
            )

        params: Dict[str, Any] = {
            "q": normalized_query,
            "image_type": normalized_image_type,
            "orientation": normalized_orientation,
            "min_width": min_width,
            "min_height": min_height,
            "editors_choice": str(editors_choice).lower(),
            "safesearch": str(safesearch).lower(),
            "order": normalized_order,
            "page": page,
            "per_page": per_page,
            "lang": normalized_language,
        }
        if normalized_category:
            params["category"] = normalized_category
        normalized_colors = _validate_colors(colors)
        if normalized_colors:
            params["colors"] = normalized_colors

        fetched = _fetch_pixabay(params, api_key)
        payload = fetched["payload"]
        images: List[Dict[str, Any]] = [
            _select_image_fields(hit)
            for hit in payload.get("hits", [])
            if isinstance(hit, dict)
        ]

        return json.dumps(
            {
                "success": True,
                "query": normalized_query,
                "total": payload.get("total"),
                "total_hits": payload.get("totalHits"),
                "page": page,
                "per_page": per_page,
                "returned": len(images),
                "cached": fetched["cached"],
                "images": images,
                "usage_note": (
                    "Show the Pixabay source and creator when displaying results. "
                    "Returned URLs are suitable for temporary search previews; download "
                    "an image before permanent use instead of permanently hotlinking it."
                ),
            },
            ensure_ascii=False,
        )
    except Exception as error:
        # urllib exceptions may contain the full request URL, so scrub the secret key.
        safe_traceback = traceback.format_exc().replace(api_key, "[REDACTED]") if api_key else traceback.format_exc()
        return json.dumps(
            {
                "success": False,
                "error": str(error).replace(api_key, "[REDACTED]") if api_key else str(error),
                "traceback": safe_traceback,
            },
            ensure_ascii=False,
        )
