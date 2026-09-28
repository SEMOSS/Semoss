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
import uuid
from typing import Any


_PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")
_DATA_REFERENCE_MARKER = "__automationDataReference"
_DATA_REFERENCE_SCHEMA_VERSION = 1
_INTERNAL_RESULT_VALUE = "__automation_value__"
_INTERNAL_RESULT_METADATA = "__automation_metadata__"
_VALUE_TYPES = {"value", "collection", "dataset", "file"}
_MISSING = object()


def read_data_page(
    reference: dict[str, Any],
    owner: dict[str, str],
    offset: int,
    limit: int,
    max_output_bytes: int,
    data_store: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return one bounded, client-safe page from a run-owned data reference."""
    _validate_owner(owner)
    if offset < 0:
        raise ValueError("Automation data offset must be zero or greater.")
    if limit < 1:
        raise ValueError("Automation data limit must be at least one.")
    metadata = _data_reference_metadata(reference)
    if metadata is None:
        raise ValueError("Automation node output does not contain a data reference.")

    scope = AutomationScope({}, owner, data_store)
    value = scope._resolve_data_reference(reference)
    page = _data_page(value, metadata["valueType"], offset, limit)
    return _json_result(page, max_output_bytes)


class AutomationScope(dict[str, Any]):
    """Read-only view of one run's inputs, metadata, and prior node outputs."""

    __slots__ = ("_owner", "_data_store")

    def __init__(
        self,
        values: dict[str, Any],
        owner: dict[str, str] | None = None,
        data_store: dict[str, Any] | None = None,
    ) -> None:
        dict.__init__(self, values)
        self._owner = owner if owner is not None else {}
        self._data_store = data_store if data_store is not None else {}

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

    def __getitem__(self, key: str) -> Any:
        return self._resolve_data_reference(dict.__getitem__(self, key))

    def get(self, key: str, default: Any = None) -> Any:
        """Return a scope value while transparently resolving run-owned data."""
        if not dict.__contains__(self, key):
            return default
        return self[key]

    def resolve(self, value: Any) -> Any:
        """Resolve generated-node configuration references against this scope."""
        value = self._resolve_data_reference(value)
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

    def _resolve_data_reference(self, value: Any) -> Any:
        metadata = _data_reference_metadata(value)
        if metadata is None:
            return value
        reference_id = metadata["referenceId"]
        entry = self._data_store.get(reference_id, _MISSING)
        if entry is _MISSING:
            raise KeyError(
                "Automation data is no longer available in this run's execution "
                "workspace. Run the automation again."
            )
        if not isinstance(entry, dict) or entry.get("owner") != self._owner:
            raise PermissionError(
                "Automation data reference does not belong to this project, run, "
                "and execution principal."
            )
        if entry.get("valueType") != metadata["valueType"]:
            raise PermissionError("Automation data reference type does not match its owner.")
        if "value" not in entry:
            raise KeyError("Automation data reference has no retained value.")
        return entry["value"]


def execute_node(
    encoded_scope: str,
    encoded_source: str,
    max_output_bytes: int,
    value_type: str = "unknown",
    inline_max_bytes: int = 0,
    owner: dict[str, str] | None = None,
    data_store: dict[str, Any] | None = None,
) -> Any:
    """Execute one persisted node module with a fresh module namespace."""
    _validate_owner(owner)
    scope = _decode_scope(encoded_scope, owner, data_store)
    source = _decode(encoded_source)
    module: dict[str, Any] = {"__name__": "__automation_node__"}
    # Java selects this persisted source only after authorizing the run. Keeping
    # exec here makes that trust boundary explicit and avoids hidden source edits.
    exec(source, module)
    run = module.get("run")
    if not callable(run):
        raise ValueError("Automation node source must define callable run(scope).")
    return _node_result(
        run(scope),
        max_output_bytes,
        value_type,
        inline_max_bytes,
        owner,
        data_store,
    )


def execute_trigger(
    encoded_scope: str,
    encoded_source: str,
    max_output_bytes: int,
    inline_max_bytes: int = 0,
    owner: dict[str, str] | None = None,
    data_store: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute trigger setup and return only public JSON-compatible globals."""
    _validate_owner(owner)
    scope = _decode_scope(encoded_scope, owner, data_store)
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

    prepared_globals = {
        name: _prepare_node_value(
            value,
            max_output_bytes,
            "unknown",
            inline_max_bytes,
            owner,
            data_store,
        )
        for name, value in globals_result.items()
    }
    return _json_result(prepared_globals, max_output_bytes)


def _decode(value: str) -> str:
    return base64.urlsafe_b64decode(value).decode("utf-8")


def _decode_scope(
    value: str,
    owner: dict[str, str] | None = None,
    data_store: dict[str, Any] | None = None,
) -> AutomationScope:
    decoded = json.loads(_decode(value))
    if not isinstance(decoded, dict):
        raise ValueError("Automation scope must be a JSON object.")
    return AutomationScope(decoded, owner, data_store)


def _is_json_compatible(value: Any) -> bool:
    try:
        json.dumps(value)
        return True
    except (TypeError, ValueError):
        return False


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


def _node_result(
    value: Any,
    max_bytes: int,
    value_type: str,
    inline_max_bytes: int,
    owner: dict[str, str] | None,
    data_store: dict[str, Any] | None,
) -> Any:
    if (
        isinstance(value, dict)
        and _INTERNAL_RESULT_VALUE in value
        and _INTERNAL_RESULT_METADATA in value
    ):
        envelope = dict(value)
        envelope[_INTERNAL_RESULT_VALUE] = _prepare_node_value(
            value[_INTERNAL_RESULT_VALUE],
            max_bytes,
            value_type,
            inline_max_bytes,
            owner,
            data_store,
        )
        return _json_result(envelope, max_bytes)
    return _prepare_node_value(
        value,
        max_bytes,
        value_type,
        inline_max_bytes,
        owner,
        data_store,
    )


def _prepare_node_value(
    value: Any,
    max_bytes: int,
    value_type: str,
    inline_max_bytes: int,
    owner: dict[str, str] | None,
    data_store: dict[str, Any] | None,
) -> Any:
    existing_reference = _reference_for_value(value, owner, data_store)
    if existing_reference is not None:
        return existing_reference

    try:
        serialized = json.dumps(value, allow_nan=False)
    except (TypeError, ValueError, RecursionError) as error:
        raise ValueError(
            "Automation node run(scope) must return a JSON-serializable value."
        ) from error

    byte_count = len(serialized.encode("utf-8"))
    should_reference = inline_max_bytes > 0 and byte_count > inline_max_bytes
    if should_reference:
        if owner is None or data_store is None:
            raise ValueError(
                "Automation node output is missing its execution workspace."
            )
        reference_id = str(uuid.uuid4())
        resolved_value_type = _resolved_value_type(value_type, value)
        data_store[reference_id] = {
            "owner": dict(owner),
            "valueType": resolved_value_type,
            "value": value,
        }
        return _data_reference(reference_id, resolved_value_type)

    if byte_count > max_bytes:
        raise ValueError(
            "Automation node run(scope) result exceeds the maximum of "
            f"{max_bytes} UTF-8 bytes."
        )
    return json.loads(serialized)


def _resolved_value_type(declared_type: str, value: Any) -> str:
    if declared_type in _VALUE_TYPES:
        return declared_type
    if isinstance(value, list):
        return "collection"
    return "value"


def _data_reference(reference_id: str, value_type: str) -> dict[str, Any]:
    return {
        _DATA_REFERENCE_MARKER: {
            "schemaVersion": _DATA_REFERENCE_SCHEMA_VERSION,
            "referenceId": reference_id,
            "valueType": value_type,
        }
    }


def _data_reference_metadata(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    metadata = value.get(_DATA_REFERENCE_MARKER)
    if not isinstance(metadata, dict):
        return None
    if metadata.get("schemaVersion") != _DATA_REFERENCE_SCHEMA_VERSION:
        return None
    reference_id = metadata.get("referenceId")
    value_type = metadata.get("valueType")
    if not isinstance(reference_id, str) or not reference_id:
        return None
    if not isinstance(value_type, str) or not value_type:
        return None
    return metadata


def _reference_for_value(
    value: Any,
    owner: dict[str, str] | None,
    data_store: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if owner is None or data_store is None:
        return None
    for reference_id, entry in data_store.items():
        if not isinstance(entry, dict) or entry.get("owner") != owner:
            continue
        if entry.get("value") is value:
            value_type = entry.get("valueType")
            if isinstance(value_type, str):
                return _data_reference(reference_id, value_type)
    return None


def _data_page(
    value: Any,
    value_type: str,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    table = _table_data(value)
    if table is not None:
        headers, values, rows_are_objects = table
        selected_rows = values[offset : offset + limit]
        page_rows = (
            [[row.get(header) for header in headers] for row in selected_rows]
            if rows_are_objects
            else selected_rows
        )
        return _page_result(
            "table",
            value_type,
            offset,
            limit,
            len(values),
            len(page_rows),
            headers=headers,
            rows=page_rows,
        )

    if isinstance(value, list):
        page_value = value[offset : offset + limit]
        return _page_result(
            "json",
            value_type,
            offset,
            limit,
            len(value),
            len(page_value),
            value=page_value,
        )

    if isinstance(value, dict):
        entries = list(value.items())
        page_entries = entries[offset : offset + limit]
        return _page_result(
            "json",
            value_type,
            offset,
            limit,
            len(entries),
            len(page_entries),
            value=dict(page_entries),
        )

    if isinstance(value, str):
        character_limit = limit * 1000
        page_value = value[offset : offset + character_limit]
        return _page_result(
            "text",
            value_type,
            offset,
            character_limit,
            len(value),
            len(page_value),
            value=page_value,
        )

    return _page_result(
        "json",
        value_type,
        offset,
        limit,
        1,
        1 if offset == 0 else 0,
        value=value if offset == 0 else None,
    )


def _table_data(value: Any) -> tuple[list[str], list[Any], bool] | None:
    if isinstance(value, list) and all(isinstance(row, dict) for row in value):
        headers: list[str] = []
        for row in value:
            for key in row:
                if isinstance(key, str) and key not in headers:
                    headers.append(key)
        return headers, value, True

    candidate = value.get("data") if isinstance(value, dict) else None
    if not isinstance(candidate, dict) and isinstance(value, dict):
        candidate = value
    if not isinstance(candidate, dict):
        return None
    headers = candidate.get("headers")
    rows = candidate.get("values")
    if not isinstance(headers, list) or not all(isinstance(header, str) for header in headers):
        return None
    if not isinstance(rows, list) or not all(isinstance(row, list) for row in rows):
        return None
    return headers, rows, False


def _page_result(
    kind: str,
    value_type: str,
    offset: int,
    limit: int,
    total: int,
    count: int,
    **content: Any,
) -> dict[str, Any]:
    return {
        "available": True,
        "kind": kind,
        "valueType": value_type,
        "offset": offset,
        "limit": limit,
        "count": count,
        "total": total,
        "hasMore": offset + count < total,
        **content,
    }


def _validate_owner(owner: dict[str, str] | None) -> None:
    if not isinstance(owner, dict):
        raise ValueError("Automation execution ownership is required.")
    for field in ("projectId", "runId", "userId"):
        value = owner.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(f"Automation execution ownership requires {field}.")
