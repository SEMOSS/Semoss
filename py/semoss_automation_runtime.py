"""Python execution boundary for SEMOSS Automation nodes.

Java owns authorization, graph ordering, persisted source selection, and run
history. The execution Insight owns Python variables and lazy query tasks for
the lifetime of one run.
"""

import base64
import json
import re
import uuid
from collections.abc import Callable, Iterator
from itertools import islice
from typing import Any


_PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")
# Serialized bridge keys mirrored by AutomationDataReference and
# AutomationConstants on the Java side.
_DATA_REFERENCE_MARKER = "__automationDataReference"
_DATA_REFERENCE_SCHEMA_VERSION = 1
_INTERNAL_RESULT_VALUE = "__automation_value__"
_INTERNAL_RESULT_METADATA = "__automation_metadata__"


class AutomationDataset(list[dict[str, Any]]):
    """Read-only JSON-array view over a query task owned by the run Insight.

    The list base is intentional: Python's standard JSON encoder only treats
    list and tuple instances as JSON arrays. Iteration, indexing, and slicing
    still load bounded pages from Java instead of storing rows in this object.
    """

    __slots__ = ("reference", "run_id", "page_size", "_known_total", "_page")

    def __init__(
        self,
        reference: dict[str, Any],
        run_id: str,
        page_size: int = 1_000,
    ) -> None:
        list.__init__(self)
        self.reference = reference
        self.run_id = run_id
        self.page_size = page_size
        self._known_total: int | None = None
        self._page: dict[str, Any] | None = None

    def _raise_read_only(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("Automation dataset is read-only.")

    __setitem__ = _raise_read_only
    __delitem__ = _raise_read_only
    __iadd__ = _raise_read_only
    __imul__ = _raise_read_only
    append = _raise_read_only
    clear = _raise_read_only
    extend = _raise_read_only
    insert = _raise_read_only
    pop = _raise_read_only
    remove = _raise_read_only
    reverse = _raise_read_only
    sort = _raise_read_only

    def __iter__(self) -> Iterator[dict[str, Any]]:
        offset = 0
        while True:
            page = self._page if offset == 0 and self._page is not None else self._read_page(
                offset, self.page_size
            )
            self._page = None
            self._remember_total(page)
            headers = page.get("headers")
            rows = page.get("rows")
            if not isinstance(headers, list) or not isinstance(rows, list):
                raise ValueError("Automation dataset page has an invalid table shape.")
            for row in rows:
                if not isinstance(row, list) or len(row) != len(headers):
                    raise ValueError("Automation dataset page has an invalid row shape.")
                yield dict(zip(headers, row))
            offset += len(rows)
            if not page.get("hasMore"):
                return
            if not rows:
                raise ValueError("Automation dataset returned an empty non-terminal page.")

    def __len__(self) -> int:
        if self._known_total is None:
            self._page = self._read_page(0, 1)
            self._remember_total(self._page)
        return self._known_total

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            if index.step not in (None, 1) or (index.start or 0) < 0 or (
                index.stop is not None and index.stop < 0
            ):
                return list(self)[index]
            return list(islice(self, index.start or 0, index.stop))
        if index < 0:
            return list(self)[index]
        page = self._read_page(index, 1)
        self._remember_total(page)
        headers = page.get("headers")
        rows = page.get("rows")
        if not isinstance(headers, list) or not isinstance(rows, list) or not rows:
            raise IndexError("Automation dataset index out of range")
        row = rows[0]
        if not isinstance(row, list) or len(row) != len(headers):
            raise ValueError("Automation dataset page has an invalid row shape.")
        return dict(zip(headers, row))

    def __repr__(self) -> str:
        return f"AutomationDataset(rows={len(self)})"

    def __contains__(self, value: object) -> bool:
        return any(item == value for item in self)

    def __reversed__(self) -> Iterator[dict[str, Any]]:
        """Reverse one materialized snapshot instead of issuing one read per row."""
        return reversed(self.materialize())

    def index(self, value: object, start: int = 0, stop: int | None = None) -> int:
        """Search through page iteration instead of issuing one read per row."""
        if start < 0:
            start = max(len(self) + start, 0)
        if stop is not None and stop < 0:
            stop = max(len(self) + stop, 0)
        for index, item in enumerate(islice(self, start, stop), start):
            if item == value:
                return index
        raise ValueError(f"{value!r} is not in AutomationDataset")

    def count(self, value: object) -> int:
        """Count matching rows through bounded page iteration."""
        return sum(1 for item in self if item == value)

    def copy(self) -> list[dict[str, Any]]:
        """Return an explicit in-memory list copy."""
        return list(self)

    def materialize(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Materialize rows explicitly, optionally with a caller-owned bound."""
        if limit is not None and limit < 0:
            raise ValueError("Automation dataset materialization limit cannot be negative.")
        return list(islice(self, limit)) if limit is not None else list(self)

    def _read_page(self, offset: int, limit: int) -> dict[str, Any]:
        from semoss import Insight

        metadata = _reference_metadata(self.reference)
        if metadata is None:
            raise ValueError("Automation dataset reference is invalid.")
        pixel = "GetAutomationRunNodeData(" + ", ".join(
            [
                _pixel_value("runId", self.run_id),
                _pixel_value("referenceId", metadata["referenceId"]),
                _pixel_value("offset", offset),
                _pixel_value("limit", limit),
            ]
        ) + ");"
        response = Insight().run_pixel(pixel, raw=True)
        result = response[0]["pixelReturn"][-1]
        if "ERROR" in result.get("operationType", []):
            raise RuntimeError(result.get("output") or "Unable to read Automation data")
        output = result.get("output")
        if not isinstance(output, dict) or output.get("available") is not True:
            raise ValueError("Automation dataset returned an invalid page.")
        return output

    def _remember_total(self, page: dict[str, Any]) -> None:
        total = page.get("total")
        if not isinstance(total, int) or isinstance(total, bool) or total < 0:
            raise ValueError("Automation dataset page has an invalid total.")
        if self._known_total is not None and self._known_total != total:
            raise ValueError("Automation dataset row count changed during the run.")
        self._known_total = total


class AutomationScope(dict[str, Any]):
    """Read-only view of one run's inputs, metadata, and prior node outputs."""

    __slots__ = ("_run_id", "_state")

    def __init__(
        self, values: dict[str, Any], run_id: str, state: dict[str, Any]
    ) -> None:
        dict.__init__(self, values)
        self._run_id = run_id
        self._state = state

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
        return _resolve_reference(dict.__getitem__(self, key), self._run_id, self._state)

    def get(self, key: str, default: Any = None) -> Any:
        if not dict.__contains__(self, key):
            return default
        return self[key]

    def resolve(self, value: Any) -> Any:
        """Resolve generated-node configuration references against this scope."""
        value = _resolve_reference(value, self._run_id, self._state)
        if isinstance(value, dict):
            return {key: self.resolve(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.resolve(item) for item in value]
        if not isinstance(value, str):
            return value

        exact = _PLACEHOLDER_PATTERN.fullmatch(value.rstrip("\r\n"))
        if exact:
            return self._required(exact.group(1))

        def replace(match: re.Match[str]) -> str:
            replacement = self._required(match.group(1))
            if isinstance(replacement, str):
                return replacement
            return json.dumps(replacement, ensure_ascii=False, separators=(",", ":"))

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
                value = _resolve_reference(value[part], self._run_id, self._state)
                continue
            if isinstance(value, (list, AutomationDataset)) and part.isdigit():
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
        if isinstance(value, str):
            value = json.loads(value or "{}")
        elif value is None:
            value = {}
        return self.resolve(value)


def execute_node(
    encoded_scope: str,
    encoded_source: str,
    max_output_bytes: int,
    inline_max_bytes: int,
    max_value_bytes: int,
    max_run_bytes: int,
    max_retained_values: int,
    run_id: str,
    state: dict[str, Any],
) -> Any:
    """Execute one persisted node module with a fresh module namespace."""
    scope = _decode_scope(encoded_scope, run_id, state)
    module: dict[str, Any] = {"__name__": "__automation_node__"}
    exec(_decode(encoded_source), module)
    run = module.get("run")
    if not callable(run):
        raise ValueError("Automation node source must define callable run(scope).")
    return _prepare_result(
        run(scope),
        max_output_bytes,
        inline_max_bytes,
        max_value_bytes,
        max_run_bytes,
        max_retained_values,
        state,
    )


def execute_trigger(
    encoded_scope: str,
    encoded_source: str,
    max_output_bytes: int,
    inline_max_bytes: int,
    max_value_bytes: int,
    max_run_bytes: int,
    max_retained_values: int,
    run_id: str,
    state: dict[str, Any],
) -> dict[str, Any]:
    """Execute trigger setup and retain oversized globals in the run Insight."""
    scope = _decode_scope(encoded_scope, run_id, state)
    module: dict[str, Any] = {"__name__": "__automation_trigger__"}
    exec(_decode(encoded_source), module)
    run = module.get("run")
    result = run(scope) if callable(run) else None
    globals_result: dict[str, Any] = {}
    for name, value in module.items():
        if not name.startswith("_") and not callable(value) and _is_json_compatible(value):
            globals_result[name] = value
    if isinstance(result, dict):
        for name, value in result.items():
            if isinstance(name, str) and not name.startswith("_") and _is_json_compatible(value):
                globals_result[name] = value
    prepared = {
        name: _prepare_value(
            value,
            inline_max_bytes,
            max_value_bytes,
            max_run_bytes,
            max_retained_values,
            state,
        )
        for name, value in globals_result.items()
    }
    return _json_result(prepared, max_output_bytes)


def prepare_node_result(
    encoded_value: str,
    max_output_bytes: int,
    inline_max_bytes: int,
    max_value_bytes: int,
    max_run_bytes: int,
    max_retained_values: int,
    state: dict[str, Any],
) -> Any:
    """Apply the same retained-value boundary to a Java-completed result."""
    return _prepare_result(
        json.loads(_decode(encoded_value)),
        max_output_bytes,
        inline_max_bytes,
        max_value_bytes,
        max_run_bytes,
        max_retained_values,
        state,
    )


def read_data_page(
    reference: dict[str, Any],
    offset: int,
    limit: int,
    max_output_bytes: int,
    state: dict[str, Any],
) -> dict[str, Any]:
    """Return one bounded page from a Python value owned by this Insight."""
    metadata = _reference_metadata(reference)
    entries = state.get("entries")
    entry = entries.get(metadata["referenceId"]) if metadata and isinstance(entries, dict) else None
    if not isinstance(entry, dict) or "value" not in entry:
        raise ValueError("Automation data is no longer available in this run workspace.")
    return _json_result(
        _data_page(entry["value"], offset, limit, max_output_bytes),
        max_output_bytes,
    )


def _prepare_result(
    value: Any,
    max_output_bytes: int,
    inline_max_bytes: int,
    max_value_bytes: int,
    max_run_bytes: int,
    max_retained_values: int,
    state: dict[str, Any],
) -> Any:
    if (
        isinstance(value, dict)
        and _INTERNAL_RESULT_VALUE in value
        and _INTERNAL_RESULT_METADATA in value
    ):
        envelope = dict(value)
        envelope[_INTERNAL_RESULT_VALUE] = _prepare_value(
            value[_INTERNAL_RESULT_VALUE],
            inline_max_bytes,
            max_value_bytes,
            max_run_bytes,
            max_retained_values,
            state,
        )
        return _json_result(envelope, max_output_bytes)
    return _prepare_value(
        value,
        inline_max_bytes,
        max_value_bytes,
        max_run_bytes,
        max_retained_values,
        state,
    )


def _prepare_value(
    value: Any,
    inline_max_bytes: int,
    max_value_bytes: int,
    max_run_bytes: int,
    max_retained_values: int,
    state: dict[str, Any],
) -> Any:
    if isinstance(value, AutomationDataset):
        return value.reference
    if _reference_metadata(value) is not None:
        return value
    existing = _reference_for(value, state)
    if existing is not None:
        return existing

    serialized, byte_count = _encode_json(value, inline_max_bytes, max_value_bytes)
    if byte_count <= inline_max_bytes:
        return json.loads(serialized)
    return _store(value, byte_count, state, max_run_bytes, max_retained_values)


def _store(
    value: Any,
    byte_count: int,
    state: dict[str, Any],
    max_run_bytes: int,
    max_retained_values: int,
) -> dict[str, Any]:
    entries = state.setdefault("entries", {})
    reverse = state.setdefault("reverse", {})
    if not isinstance(entries, dict) or not isinstance(reverse, dict):
        raise ValueError("Automation retained-data state is invalid.")
    if len(entries) >= max_retained_values:
        raise ValueError("Automation run has too many retained values.")
    current_bytes = sum(
        entry.get("bytes", 0) for entry in entries.values() if isinstance(entry, dict)
    )
    if current_bytes + byte_count > max_run_bytes:
        raise ValueError("Automation run retained data exceeds its memory limit.")
    reference_id = str(uuid.uuid4())
    reference = _reference(reference_id)
    entries[reference_id] = {"value": value, "bytes": byte_count}
    reverse[id(value)] = reference_id
    return reference


def _reference_for(value: Any, state: dict[str, Any]) -> dict[str, Any] | None:
    entries = state.get("entries")
    reverse = state.get("reverse")
    if not isinstance(entries, dict) or not isinstance(reverse, dict):
        return None
    reference_id = reverse.get(id(value))
    entry = entries.get(reference_id)
    if isinstance(entry, dict) and entry.get("value") is value:
        return _reference(reference_id)
    return None


def _resolve_reference(value: Any, run_id: str, state: dict[str, Any]) -> Any:
    metadata = _reference_metadata(value)
    if metadata is None:
        return value
    entries = state.get("entries")
    entry = entries.get(metadata["referenceId"]) if isinstance(entries, dict) else None
    if isinstance(entry, dict) and "value" in entry:
        return entry["value"]
    return AutomationDataset(value, run_id)


def _reference(reference_id: str) -> dict[str, Any]:
    return {
        _DATA_REFERENCE_MARKER: {
            "schemaVersion": _DATA_REFERENCE_SCHEMA_VERSION,
            "referenceId": reference_id,
        }
    }


def _reference_metadata(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    metadata = value.get(_DATA_REFERENCE_MARKER)
    if (
        not isinstance(metadata, dict)
        or metadata.get("schemaVersion") != _DATA_REFERENCE_SCHEMA_VERSION
    ):
        return None
    reference_id = metadata.get("referenceId")
    return metadata if isinstance(reference_id, str) and reference_id else None


def _decode_scope(value: str, run_id: str, state: dict[str, Any]) -> AutomationScope:
    decoded = json.loads(_decode(value))
    if not isinstance(decoded, dict):
        raise ValueError("Automation scope must be a JSON object.")
    return AutomationScope(decoded, run_id, state)


def _decode(value: str) -> str:
    return base64.urlsafe_b64decode(value).decode("utf-8")


def _is_json_compatible(value: Any) -> bool:
    try:
        json.dumps(value)
        return True
    except (TypeError, ValueError):
        return False


def _encode_json(value: Any, inline_max_bytes: int, absolute_max_bytes: int) -> tuple[str, int]:
    encoder = json.JSONEncoder(allow_nan=False)
    chunks: list[str] = []
    byte_count = 0
    try:
        for chunk in encoder.iterencode(value):
            byte_count += len(chunk.encode("utf-8"))
            if byte_count > absolute_max_bytes:
                raise ValueError(
                    "Automation node output exceeds the retained-value limit of "
                    f"{absolute_max_bytes} UTF-8 bytes."
                )
            if byte_count <= inline_max_bytes:
                chunks.append(chunk)
    except (TypeError, ValueError, RecursionError) as error:
        if str(error).startswith("Automation node output exceeds"):
            raise
        raise ValueError(
            "Automation node run(scope) must return a JSON-serializable value."
        ) from error
    return "".join(chunks), byte_count


def _json_result(value: Any, max_bytes: int) -> Any:
    try:
        serialized = json.dumps(value, allow_nan=False)
    except (TypeError, ValueError, RecursionError) as error:
        raise ValueError(
            "Automation node run(scope) must return a JSON-serializable value."
        ) from error
    if len(serialized.encode("utf-8")) > max_bytes:
        raise ValueError(
            "Automation node run(scope) result exceeds the maximum of "
            f"{max_bytes} UTF-8 bytes."
        )
    return json.loads(serialized)


def _pixel_value(name: str, value: Any) -> str:
    return name + "=[" + json.dumps(value) + "]"


def _data_page(
    value: Any, offset: int, limit: int, max_output_bytes: int
) -> dict[str, Any]:
    if offset < 0 or limit < 1:
        raise ValueError("Automation data page bounds are invalid.")
    collection = _record_collection(value)
    if collection is not None:
        collection_key, rows = collection

        def nested_page(selected: list[Any]) -> dict[str, Any]:
            selected_value = dict(value)
            selected_value[collection_key] = selected
            return _page(
                "json",
                offset,
                limit,
                len(selected),
                len(rows),
                value=selected_value,
                collectionKey=collection_key,
            )

        return _bounded_sequence_page(
            rows, offset, limit, max_output_bytes, nested_page
        )
    table = _table_data(value)
    if table is not None:
        headers, rows = table

        def table_page(selected: list[Any]) -> dict[str, Any]:
            if not all(isinstance(row, list) for row in selected):
                raise ValueError("Automation table page has an invalid row shape.")
            return _page(
                "table",
                offset,
                limit,
                len(selected),
                len(rows),
                headers=headers,
                rows=selected,
            )

        return _bounded_sequence_page(
            rows, offset, limit, max_output_bytes, table_page
        )
    if isinstance(value, list):
        def list_page(selected: list[Any]) -> dict[str, Any]:
            if selected and all(isinstance(row, dict) for row in selected):
                headers = _object_headers(selected)
                rows = [[row.get(header) for header in headers] for row in selected]
                return _page(
                    "table",
                    offset,
                    limit,
                    len(rows),
                    len(value),
                    headers=headers,
                    rows=rows,
                )
            return _page(
                "json",
                offset,
                limit,
                len(selected),
                len(value),
                value=selected,
            )

        return _bounded_sequence_page(
            value, offset, limit, max_output_bytes, list_page
        )
    if isinstance(value, dict):
        items = list(value.items())

        def mapping_page(selected: list[Any]) -> dict[str, Any]:
            return _page(
                "json",
                offset,
                limit,
                len(selected),
                len(items),
                value=dict(selected),
            )

        return _bounded_sequence_page(
            items, offset, limit, max_output_bytes, mapping_page
        )
    if isinstance(value, str):
        return _bounded_text_page(value, offset, limit * 1000, max_output_bytes)
    page = _page(
        "json",
        offset,
        limit,
        1 if offset == 0 else 0,
        1,
        value=value if offset == 0 else None,
    )
    if not _page_fits(page, max_output_bytes):
        raise ValueError("Automation data item exceeds the maximum page size.")
    return page


def _table_data(value: Any) -> tuple[list[str], list[Any]] | None:
    candidate = value.get("data") if isinstance(value, dict) else None
    if not isinstance(candidate, dict) and isinstance(value, dict):
        candidate = value
    if not isinstance(candidate, dict):
        return None
    headers = candidate.get("headers")
    rows = candidate.get("values")
    if not isinstance(headers, list) or not all(isinstance(header, str) for header in headers):
        return None
    if not isinstance(rows, list):
        return None
    return headers, rows


def _record_collection(
    value: Any,
) -> tuple[str, list[Any]] | None:
    """Return one unambiguous nested record collection for bounded JSON pages."""
    if not isinstance(value, dict):
        return None
    candidates = [
        (key, item)
        for key, item in value.items()
        if isinstance(key, str)
        and isinstance(item, list)
        and item
    ]
    if len(candidates) != 1:
        return None
    return candidates[0]


def _object_headers(rows: list[Any]) -> list[str]:
    headers: list[str] = []
    for row in rows:
        for key in row:
            if isinstance(key, str) and key not in headers:
                headers.append(key)
    return headers


def _bounded_sequence_page(
    values: list[Any],
    offset: int,
    limit: int,
    max_output_bytes: int,
    build_page: Callable[[list[Any]], dict[str, Any]],
) -> dict[str, Any]:
    available = min(limit, max(len(values) - offset, 0))
    return _largest_fitting_page(
        available,
        max_output_bytes,
        lambda count: build_page(values[offset : offset + count]),
    )


def _bounded_text_page(
    value: str, offset: int, limit: int, max_output_bytes: int
) -> dict[str, Any]:
    def build_page(count: int) -> dict[str, Any]:
        selected = value[offset : offset + count]
        return _page(
            "text",
            offset,
            limit,
            len(selected),
            len(value),
            value=selected,
        )

    available = min(limit, max(len(value) - offset, 0))
    return _largest_fitting_page(available, max_output_bytes, build_page)


def _largest_fitting_page(
    available: int,
    max_output_bytes: int,
    build_page: Callable[[int], dict[str, Any]],
) -> dict[str, Any]:
    if available == 0:
        page = build_page(0)
        if _page_fits(page, max_output_bytes):
            return page
        raise ValueError("Automation data page metadata exceeds the maximum page size.")
    lower = 1
    upper = available
    selected_page: dict[str, Any] | None = None
    while lower <= upper:
        count = (lower + upper) // 2
        page = build_page(count)
        if _page_fits(page, max_output_bytes):
            selected_page = page
            lower = count + 1
        else:
            upper = count - 1
    if selected_page is None:
        raise ValueError("Automation data item exceeds the maximum page size.")
    return selected_page


def _page_fits(page: dict[str, Any], max_output_bytes: int) -> bool:
    return len(json.dumps(page, allow_nan=False).encode("utf-8")) <= max_output_bytes


def _page(
    kind: str,
    offset: int,
    limit: int,
    count: int,
    total: int,
    **content: Any,
) -> dict[str, Any]:
    return {
        "available": True,
        "kind": kind,
        "offset": offset,
        "limit": limit,
        "count": count,
        "total": total,
        "hasMore": offset + count < total,
        **content,
    }
