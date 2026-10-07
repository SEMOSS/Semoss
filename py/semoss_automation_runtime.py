"""Python execution boundary for SEMOSS Automation nodes.

Java owns authorization, graph ordering, persisted source selection, and run
history. This module owns the Python-specific scope and execution behavior so
Java never parses or rewrites Python source. ``developer.python`` remains
arbitrary code executed for an authorized Automation project; this module is
not a sandbox.
"""

import base64
import json
import re
import xml.etree.ElementTree as ET
from collections.abc import Callable
from typing import Any


_PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")
_DATA_PATH_TOKEN_PATTERN = re.compile(
    r"([^.\[\]]+)|\[(\d+|\"(?:\\.|[^\"])*\"|'(?:\\.|[^'])*')\]"
)
_MISSING = object()


class AutomationScope(dict[str, Any]):
    """Read-only view of one run's inputs, metadata, and prior node outputs."""

    __slots__ = ()

    def _raise_read_only(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "Automation scope is read-only; return a value to pass data forward."
        )

    __setitem__ = _raise_read_only
    __delitem__ = _raise_read_only
    __ior__ = _raise_read_only
    clear = _raise_read_only
    pop = _raise_read_only
    popitem = _raise_read_only
    setdefault = _raise_read_only
    update = _raise_read_only

    def resolve(self, value: Any) -> Any:
        """Resolve generated-node configuration references against this scope."""
        if isinstance(value, dict):
            return {key: self.resolve(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.resolve(item) for item in value]
        if not isinstance(value, str):
            return value

        # A single-line authoring control can leave a trailing DOM line break after
        # inserting a variable pill. Treat that as the same exact binding while
        # preserving intentional spaces in ordinary prompt text.
        exact = _PLACEHOLDER_PATTERN.fullmatch(value.rstrip("\r\n"))
        if exact:
            return self._required(exact.group(1))

        def replace(match: re.Match[str]) -> str:
            replacement = self._required(match.group(1))
            if isinstance(replacement, str):
                return replacement
            return json.dumps(
                replacement,
                ensure_ascii=False,
                separators=(",", ":"),
            )

        return _PLACEHOLDER_PATTERN.sub(replace, value)

    def _required(self, name: str) -> Any:
        if name in self:
            return self[name]

        parts = name.split(".")
        if not parts or parts[0] not in self:
            raise KeyError(
                f"Generated automation configuration references unavailable "
                f"scope value '{name}'."
            )

        value: Any = self[parts[0]]
        for part in parts[1:]:
            if isinstance(value, dict) and part in value:
                value = value[part]
                continue
            if isinstance(value, list) and part.isdigit():
                index = int(part)
                if index < len(value):
                    value = value[index]
                    continue
            raise KeyError(
                f"Generated automation configuration references unavailable "
                f"scope value '{name}'."
            )
        return value

    def resolve_config(self, value: Any) -> Any:
        """Resolve a generated-node configuration while preserving native types."""
        if isinstance(value, str):
            value = json.loads(value or "{}")
        elif value is None:
            value = {}
        return self.resolve(value)


def extract_data_element(
    source: Any,
    path: str,
    source_format: str = "auto",
    missing_value: Any = None,
    null_value: Any = None,
    asset_reader: Callable[[str], str] | None = None,
) -> Any:
    """Return one nested JSON/XML value with distinct missing and null fallbacks."""
    data = _structured_data(source, source_format, asset_reader)
    value = _nested_value(data, _data_path_tokens(path))
    if value is _MISSING:
        return missing_value
    if value is None:
        return null_value
    return value


def _structured_data(
    source: Any,
    source_format: str,
    asset_reader: Callable[[str], str] | None,
) -> Any:
    normalized_format = source_format.strip().lower()
    if normalized_format not in {"auto", "json", "xml"}:
        raise ValueError("Data format must be auto, json, or xml.")
    if isinstance(source, (dict, list)):
        if normalized_format == "xml":
            raise ValueError("XML input must be text or a run-workspace file path.")
        return source
    if not isinstance(source, str) or not source.strip():
        raise ValueError("Data must be a JSON/XML value, encoded content, or file path.")

    candidates = [source]
    decoded = _decoded_text(source)
    if decoded is not None:
        candidates.append(decoded)

    errors: list[Exception] = []
    for candidate in candidates:
        try:
            return _parse_structured_text(candidate, normalized_format)
        except (json.JSONDecodeError, ET.ParseError, UnicodeError, ValueError) as error:
            errors.append(error)
    if asset_reader is not None:
        try:
            return _parse_structured_text(asset_reader(source), normalized_format)
        except (json.JSONDecodeError, ET.ParseError, UnicodeError, ValueError) as error:
            errors.append(error)
    raise ValueError(
        "Unable to read JSON or XML from the supplied value or run-workspace file."
    ) from errors[-1]


def _parse_structured_text(value: str, source_format: str) -> Any:
    text = value.lstrip("\ufeff\r\n\t ")
    if source_format == "json" or source_format == "auto" and text[:1] in "[{":
        return json.loads(text)
    if source_format == "xml" or source_format == "auto" and text.startswith("<"):
        root = ET.fromstring(text)
        return {root.tag: _xml_value(root)}
    if source_format == "auto":
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            root = ET.fromstring(text)
            return {root.tag: _xml_value(root)}
    raise ValueError(f"Input is not valid {source_format.upper()}.")


def _decoded_text(value: str) -> str | None:
    try:
        return base64.b64decode(value, validate=True).decode("utf-8-sig")
    except (ValueError, UnicodeError):
        return None


def _xml_value(element: ET.Element) -> Any:
    children = list(element)
    attributes: dict[str, Any] = {
        f"@{name}": value for name, value in element.attrib.items()
    }
    text = (element.text or "").strip()
    if not children:
        if not attributes:
            return text or None
        if text:
            attributes["#text"] = text
        return attributes

    result = attributes
    for child in children:
        value = _xml_value(child)
        existing = result.get(child.tag, _MISSING)
        if existing is _MISSING:
            result[child.tag] = value
        elif isinstance(existing, list):
            existing.append(value)
        else:
            result[child.tag] = [existing, value]
    if text:
        result["#text"] = text
    return result


def _data_path_tokens(path: str) -> list[str | int]:
    value = path.strip()
    if not value:
        raise ValueError("Value path cannot be blank.")
    if value.startswith("/"):
        return [
            int(token) if token.isdigit() else token.replace("~1", "/").replace("~0", "~")
            for token in value.split("/")[1:]
        ]
    if value == "$":
        return []
    if value.startswith("$."):
        value = value[2:]

    tokens: list[str | int] = []
    cursor = 0
    for match in _DATA_PATH_TOKEN_PATTERN.finditer(value):
        separator = value[cursor : match.start()]
        if separator not in {"", "."}:
            raise ValueError(f"Invalid value path near '{separator}'.")
        raw = match.group(1) or match.group(2)
        if raw is None:
            continue
        if raw.isdigit():
            tokens.append(int(raw))
        elif raw[:1] in {"\"", "'"}:
            tokens.append(json.loads(raw) if raw.startswith("\"") else raw[1:-1])
        else:
            tokens.append(raw)
        cursor = match.end()
    if cursor != len(value) or not tokens:
        raise ValueError("Value path is invalid.")
    return tokens


def _nested_value(value: Any, tokens: list[str | int]) -> Any:
    current = value
    for token in tokens:
        if isinstance(current, dict) and token in current:
            current = current[token]
        elif isinstance(current, list) and isinstance(token, int) and token < len(current):
            current = current[token]
        else:
            return _MISSING
    return current


def execute_node(
    encoded_scope: str,
    encoded_source: str,
    max_output_bytes: int,
    output_variable: str,
    session_globals: dict[str, Any],
) -> Any:
    """Execute one persisted node module with a fresh module namespace."""
    scope = _decode_scope(encoded_scope)
    source = _decode(encoded_source)
    module: dict[str, Any] = {"__name__": "__automation_node__"}
    # Java selects this persisted source only after authorizing the run. Keeping
    # exec here makes that trust boundary explicit and avoids hidden source edits.
    exec(source, module)
    run = module.get("run")
    if not callable(run):
        raise ValueError("Automation node source must define callable run(scope).")
    result = _json_result(run(scope), max_output_bytes)
    _prepare_frame(result, output_variable, session_globals)
    return result


def execute_trigger(
    encoded_scope: str, encoded_source: str, max_output_bytes: int
) -> dict[str, Any]:
    """Execute trigger setup and return only public JSON-compatible globals."""
    scope = _decode_scope(encoded_scope)
    module: dict[str, Any] = {"__name__": "__automation_trigger__"}
    exec(_decode(encoded_source), module)
    run = module.get("run")
    result = run(scope) if callable(run) else None
    globals_result: dict[str, Any] = {}

    for name, value in module.items():
        if name.startswith("_") or callable(value):
            continue
        if _is_json_compatible(value):
            globals_result[name] = value

    if isinstance(result, dict):
        for name, value in result.items():
            if (
                isinstance(name, str)
                and not name.startswith("_")
                and _is_json_compatible(value)
            ):
                globals_result[name] = value

    return _json_result(globals_result, max_output_bytes)


def _decode(value: str) -> str:
    return base64.urlsafe_b64decode(value).decode("utf-8")


def _decode_scope(value: str) -> AutomationScope:
    decoded = json.loads(_decode(value))
    if not isinstance(decoded, dict):
        raise ValueError("Automation scope must be a JSON object.")
    return AutomationScope(decoded)


def _is_json_compatible(value: Any) -> bool:
    try:
        json.dumps(value)
        return True
    except (TypeError, ValueError):
        return False


def _prepare_frame(
    value: Any, output_variable: str, session_globals: dict[str, Any]
) -> None:
    """Retain row output where the standard SEMOSS Python-frame bridge expects it.

    The JSON result remains the Automation value used by history and downstream
    scope. The DataFrame is the run-Insight view used for paged UI inspection.
    """
    session_globals.pop(output_variable, None)
    if not value or not isinstance(value, list):
        return
    if not all(isinstance(row, dict) for row in value):
        return

    import pandas as pd

    session_globals[output_variable] = pd.DataFrame.from_records(value)


def _json_result(value: Any, max_bytes: int) -> Any:
    try:
        serialized = json.dumps(value, allow_nan=False)
    except (TypeError, ValueError, RecursionError) as error:
        raise ValueError(
            "Automation node run(scope) must return a JSON-serializable value."
        ) from error
    byte_count = len(serialized.encode("utf-8"))
    if byte_count > max_bytes:
        raise ValueError(
            "Automation node run(scope) result exceeds the maximum of "
            f"{max_bytes} UTF-8 bytes."
        )
    return json.loads(serialized)
