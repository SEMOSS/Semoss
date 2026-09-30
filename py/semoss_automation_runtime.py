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
from collections.abc import Iterator, Sequence
from itertools import islice
from typing import Any


_PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")
_DATA_REFERENCE_MARKER = "__automationDataReference"
_DATA_REFERENCE_SCHEMA_VERSION = 1
_INTERNAL_RESULT_VALUE = "__automation_value__"
_INTERNAL_RESULT_METADATA = "__automation_metadata__"
_VALUE_TYPES = {"value", "collection", "dataset", "file"}
_MISSING = object()


class RunMemoryDataProvider:
    """Bounded run-local provider backed by an execution Insight's Python state."""

    PROVIDER_ID = "run-memory"

    def __init__(
        self,
        state: dict[str, Any],
        max_value_bytes: int,
        max_owner_bytes: int,
        max_owner_values: int,
    ) -> None:
        providers = state.setdefault("providers", {})
        if not isinstance(providers, dict):
            raise ValueError("Automation provider state must be a dictionary.")
        provider_state = providers.setdefault(self.PROVIDER_ID, {})
        if not isinstance(provider_state, dict):
            raise ValueError("Automation run-memory state must be a dictionary.")
        self._entries = provider_state.setdefault("entries", {})
        if not isinstance(self._entries, dict):
            raise ValueError("Automation run-memory entries must be a dictionary.")
        self._max_value_bytes = max_value_bytes
        self._max_owner_bytes = max_owner_bytes
        self._max_owner_values = max_owner_values

    def store(
        self,
        owner: dict[str, str],
        value: Any,
        value_type: str,
        byte_count: int,
    ) -> dict[str, Any]:
        _validate_owner(owner)
        if byte_count > self._max_value_bytes:
            raise ValueError(
                "Automation node output exceeds the run-local retained-value "
                f"maximum of {self._max_value_bytes} UTF-8 bytes."
            )

        owner_entries = [
            entry
            for entry in self._entries.values()
            if isinstance(entry, dict) and entry.get("owner") == owner
        ]
        if len(owner_entries) >= self._max_owner_values:
            raise ValueError(
                "Automation run exceeds the maximum number of retained values "
                f"({self._max_owner_values})."
            )
        retained_bytes = sum(
            entry.get("byteCount", 0)
            for entry in owner_entries
            if isinstance(entry.get("byteCount"), int)
        )
        if retained_bytes + byte_count > self._max_owner_bytes:
            raise ValueError(
                "Automation run exceeds the retained-data maximum of "
                f"{self._max_owner_bytes} UTF-8 bytes."
            )

        reference_id = str(uuid.uuid4())
        self._entries[reference_id] = {
            "owner": dict(owner),
            "valueType": value_type,
            "byteCount": byte_count,
            "pageDescriptor": _page_descriptor(value),
            "value": value,
        }
        return _data_reference(reference_id, value_type)

    def resolve(self, reference: dict[str, Any], owner: dict[str, str]) -> Any:
        return self._entry(reference, owner)["value"]

    def has_reference(self, reference: dict[str, Any]) -> bool:
        """Return whether this provider knows the reference, regardless of owner."""
        metadata = _data_reference_metadata(reference)
        return metadata is not None and metadata["referenceId"] in self._entries

    def read_page(
        self,
        reference: dict[str, Any],
        owner: dict[str, str],
        offset: int,
        limit: int,
    ) -> dict[str, Any]:
        entry = self._entry(reference, owner)
        return _data_page(
            entry["value"],
            entry["valueType"],
            offset,
            limit,
            entry.get("pageDescriptor"),
        )

    def reference_for(
        self, value: Any, owner: dict[str, str]
    ) -> dict[str, Any] | None:
        for reference_id, entry in self._entries.items():
            if not isinstance(entry, dict) or entry.get("owner") != owner:
                continue
            if entry.get("value") is value:
                value_type = entry.get("valueType")
                if isinstance(value_type, str):
                    return _data_reference(reference_id, value_type)
        return None

    def _entry(
        self, reference: dict[str, Any], owner: dict[str, str]
    ) -> dict[str, Any]:
        _validate_owner(owner)
        metadata = _data_reference_metadata(reference)
        if metadata is None:
            raise ValueError("Automation node output does not contain a data reference.")
        entry = self._entries.get(metadata["referenceId"], _MISSING)
        if entry is _MISSING:
            raise KeyError(
                "Automation data is no longer available in this run's execution "
                "workspace. Run the automation again."
            )
        if not isinstance(entry, dict) or entry.get("owner") != owner:
            raise PermissionError(
                "Automation data reference does not belong to this project, run, "
                "and execution principal."
            )
        if entry.get("valueType") != metadata["valueType"]:
            raise PermissionError(
                "Automation data reference type does not match its owner."
            )
        if "value" not in entry:
            raise KeyError("Automation data reference has no retained value.")
        return entry


class AutomationDataService:
    """Own retained-data policy behind the canonical reference contract."""

    def __init__(self, provider: RunMemoryDataProvider, max_value_bytes: int):
        self._provider = provider
        self.max_value_bytes = max_value_bytes

    def store(
        self,
        owner: dict[str, str],
        value: Any,
        value_type: str,
        byte_count: int,
    ) -> dict[str, Any]:
        return self._provider.store(owner, value, value_type, byte_count)

    def resolve(self, reference: dict[str, Any], owner: dict[str, str]) -> Any:
        if self._provider.has_reference(reference):
            return self._provider.resolve(reference, owner)
        metadata = _data_reference_metadata(reference)
        if metadata is None or metadata["valueType"] != "dataset":
            raise KeyError(
                "Automation data is no longer available in this run's execution "
                "workspace. Run the automation again."
            )
        return AutomationDataset(reference, owner)

    def read_page(
        self,
        reference: dict[str, Any],
        owner: dict[str, str],
        offset: int,
        limit: int,
    ) -> dict[str, Any]:
        return self._provider.read_page(reference, owner, offset, limit)

    def reference_for(
        self, value: Any, owner: dict[str, str]
    ) -> dict[str, Any] | None:
        if isinstance(value, AutomationDataset):
            if value.owner != owner:
                raise PermissionError(
                    "Automation dataset does not belong to this execution owner."
                )
            return value.reference
        return self._provider.reference_for(value, owner)

class AutomationDataset(Sequence[dict[str, Any]]):
    """Lazy row sequence backed by a task in the Automation execution Insight.

    Iteration, length, integer indexes, slices, and list comprehensions work
    without exposing a task identifier to the node author. Rows cross the bridge
    only in bounded pages.
    """

    DEFAULT_PAGE_SIZE = 1_000

    def __init__(
        self, reference: dict[str, Any], owner: dict[str, str], page_size: int = 1_000
    ) -> None:
        _validate_owner(owner)
        metadata = _data_reference_metadata(reference)
        if metadata is None or metadata["valueType"] != "dataset":
            raise ValueError("Automation dataset requires a valid data reference.")
        if (
            not isinstance(page_size, int)
            or isinstance(page_size, bool)
            or page_size < 1
            or page_size > self.DEFAULT_PAGE_SIZE
        ):
            raise ValueError(
                f"Automation dataset page size must be between 1 and {self.DEFAULT_PAGE_SIZE}."
            )
        self.reference = reference
        self.owner = dict(owner)
        self.page_size = page_size
        self._known_total: int | None = None
        self._prefetched_page: dict[str, Any] | None = None

    def __iter__(self) -> Iterator[dict[str, Any]]:
        offset = 0
        while True:
            if offset == 0 and self._prefetched_page is not None:
                page = self._prefetched_page
                self._prefetched_page = None
            else:
                page = self._read_page(offset, self.page_size)
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
            page = self._read_page(0, 1)
            self._remember_total(page)
            self._prefetched_page = page
        return self._known_total

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            if index.step not in (None, 1) or (index.start is not None and index.start < 0) or (
                index.stop is not None and index.stop < 0
            ):
                return list(self)[index]
            start = index.start or 0
            stop = index.stop
            if stop is None:
                return list(islice(self, start, None))
            return list(islice(self, start, stop))

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

    def to_list(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Materialize rows explicitly, optionally with a caller-owned bound."""
        if limit is not None and limit < 0:
            raise ValueError("Automation dataset materialization limit cannot be negative.")
        return list(islice(self, limit)) if limit is not None else list(self)

    def _read_page(self, offset: int, limit: int) -> dict[str, Any]:
        from semoss import Insight

        metadata = _data_reference_metadata(self.reference)
        if metadata is None:
            raise ValueError("Automation dataset reference is invalid.")
        pixel = "GetAutomationTaskData(" + ", ".join(
            [
                _pixel_value("runId", self.owner["runId"]),
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


def create_data_service(
    state: dict[str, Any],
    max_value_bytes: int,
    max_owner_bytes: int,
    max_owner_values: int,
) -> AutomationDataService:
    """Create the run data service over state owned by one execution Insight."""
    if not isinstance(state, dict):
        raise ValueError("Automation data state must be a dictionary.")
    if max_value_bytes < 1 or max_owner_bytes < 1 or max_owner_values < 1:
        raise ValueError("Automation data-service limits must be positive.")
    provider = RunMemoryDataProvider(
        state, max_value_bytes, max_owner_bytes, max_owner_values
    )
    return AutomationDataService(provider, max_value_bytes)


def read_data_page(
    reference: dict[str, Any],
    owner: dict[str, str],
    offset: int,
    limit: int,
    max_output_bytes: int,
    data_service: AutomationDataService,
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

    page = data_service.read_page(reference, owner, offset, limit)
    return _json_result(page, max_output_bytes)


class AutomationScope(dict[str, Any]):
    """Read-only view of one run's inputs, metadata, and prior node outputs."""

    __slots__ = ("_owner", "_data_service")

    def __init__(
        self,
        values: dict[str, Any],
        owner: dict[str, str] | None = None,
        data_service: AutomationDataService | None = None,
    ) -> None:
        dict.__init__(self, values)
        self._owner = owner if owner is not None else {}
        self._data_service = data_service

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
            if isinstance(value, AutomationDataset) and part.isdigit():
                try:
                    value = value[int(part)]
                    continue
                except IndexError:
                    pass
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
        if self._data_service is None:
            raise KeyError(
                "Automation data is no longer available in this run's execution "
                "workspace. Run the automation again."
            )
        return self._data_service.resolve(value, self._owner)


def execute_node(
    encoded_scope: str,
    encoded_source: str,
    max_output_bytes: int,
    value_type: str = "unknown",
    inline_max_bytes: int = 0,
    owner: dict[str, str] | None = None,
    data_service: AutomationDataService | None = None,
) -> Any:
    """Execute one persisted node module with a fresh module namespace."""
    _validate_owner(owner)
    scope = _decode_scope(encoded_scope, owner, data_service)
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
        data_service,
    )


def execute_trigger(
    encoded_scope: str,
    encoded_source: str,
    max_output_bytes: int,
    inline_max_bytes: int = 0,
    owner: dict[str, str] | None = None,
    data_service: AutomationDataService | None = None,
) -> dict[str, Any]:
    """Execute trigger setup and return only public JSON-compatible globals."""
    _validate_owner(owner)
    scope = _decode_scope(encoded_scope, owner, data_service)
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
            data_service,
        )
        for name, value in globals_result.items()
    }
    return _json_result(prepared_globals, max_output_bytes)


def prepare_node_result(
    encoded_value: str,
    max_output_bytes: int,
    value_type: str = "unknown",
    inline_max_bytes: int = 0,
    owner: dict[str, str] | None = None,
    data_service: AutomationDataService | None = None,
) -> Any:
    """Apply the node-value contract to a result completed on the Java side."""
    _validate_owner(owner)
    value = json.loads(_decode(encoded_value))
    return _node_result(
        value,
        max_output_bytes,
        value_type,
        inline_max_bytes,
        owner,
        data_service,
    )


def _decode(value: str) -> str:
    return base64.urlsafe_b64decode(value).decode("utf-8")


def _decode_scope(
    value: str,
    owner: dict[str, str] | None = None,
    data_service: AutomationDataService | None = None,
) -> AutomationScope:
    decoded = json.loads(_decode(value))
    if not isinstance(decoded, dict):
        raise ValueError("Automation scope must be a JSON object.")
    return AutomationScope(decoded, owner, data_service)


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
    data_service: AutomationDataService | None,
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
            data_service,
        )
        return _json_result(envelope, max_bytes)
    return _prepare_node_value(
        value,
        max_bytes,
        value_type,
        inline_max_bytes,
        owner,
        data_service,
    )


def _prepare_node_value(
    value: Any,
    max_bytes: int,
    value_type: str,
    inline_max_bytes: int,
    owner: dict[str, str] | None,
    data_service: AutomationDataService | None,
) -> Any:
    existing_reference = (
        data_service.reference_for(value, owner)
        if owner is not None and data_service is not None
        else None
    )
    if existing_reference is not None:
        return existing_reference

    retained_max_bytes = (
        data_service.max_value_bytes
        if inline_max_bytes > 0 and data_service is not None
        else max_bytes
    )
    serialized, byte_count = _encode_json_value(
        value,
        inline_max_bytes if inline_max_bytes > 0 else max_bytes,
        retained_max_bytes,
    )
    should_reference = inline_max_bytes > 0 and byte_count > inline_max_bytes
    if should_reference:
        if owner is None or data_service is None:
            raise ValueError(
                "Automation node output is missing its execution workspace."
            )
        resolved_value_type = _resolved_value_type(value_type, value)
        return data_service.store(owner, value, resolved_value_type, byte_count)

    if byte_count > max_bytes:
        raise ValueError(
            "Automation node run(scope) result exceeds the maximum of "
            f"{max_bytes} UTF-8 bytes."
        )
    if serialized is None:
        raise ValueError("Automation node output could not be serialized inline.")
    return json.loads(serialized)


def _encode_json_value(
    value: Any,
    inline_max_bytes: int,
    absolute_max_bytes: int,
) -> tuple[str | None, int]:
    """Validate and measure JSON without retaining a second large string in memory."""
    encoder = json.JSONEncoder(allow_nan=False)
    chunks: list[str] | None = []
    byte_count = 0
    try:
        for chunk in encoder.iterencode(value):
            byte_count += len(chunk.encode("utf-8"))
            if byte_count > absolute_max_bytes:
                raise ValueError(
                    "Automation node output exceeds the maximum retained value size "
                    f"of {absolute_max_bytes} UTF-8 bytes."
                )
            if chunks is not None:
                if byte_count <= inline_max_bytes:
                    chunks.append(chunk)
                else:
                    chunks = None
    except (TypeError, ValueError, RecursionError) as error:
        if isinstance(error, ValueError) and str(error).startswith(
            "Automation node output exceeds"
        ):
            raise
        raise ValueError(
            "Automation node run(scope) must return a JSON-serializable value."
        ) from error
    return ("".join(chunks) if chunks is not None else None), byte_count


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


def _pixel_value(name: str, value: Any) -> str:
    """Encode one scalar Pixel argument without exposing string construction."""
    return name + "=[" + json.dumps(value) + "]"


def _data_page(
    value: Any,
    value_type: str,
    offset: int,
    limit: int,
    descriptor: dict[str, Any] | None = None,
) -> dict[str, Any]:
    table = _table_data(value, descriptor)
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
        page_entries = list(islice(value.items(), offset, offset + limit))
        return _page_result(
            "json",
            value_type,
            offset,
            limit,
            len(value),
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


def _page_descriptor(value: Any) -> dict[str, Any]:
    """Compute stable page shape once when a value enters retained storage."""
    if isinstance(value, list) and all(isinstance(row, dict) for row in value):
        headers: list[str] = []
        seen: set[str] = set()
        for row in value:
            for key in row:
                if isinstance(key, str) and key not in seen:
                    seen.add(key)
                    headers.append(key)
        return {"kind": "table", "headers": headers, "rowsAreObjects": True}

    candidate = value.get("data") if isinstance(value, dict) else None
    if not isinstance(candidate, dict) and isinstance(value, dict):
        candidate = value
    if not isinstance(candidate, dict):
        return {"kind": "value"}
    headers = candidate.get("headers")
    rows = candidate.get("values")
    if not isinstance(headers, list) or not all(isinstance(header, str) for header in headers):
        return {"kind": "value"}
    if not isinstance(rows, list) or not all(isinstance(row, list) for row in rows):
        return {"kind": "value"}
    return {"kind": "table", "headers": headers, "rowsAreObjects": False}


def _table_data(
    value: Any, descriptor: dict[str, Any] | None = None
) -> tuple[list[str], list[Any], bool] | None:
    descriptor = descriptor if descriptor is not None else _page_descriptor(value)
    if descriptor.get("kind") != "table":
        return None
    headers = descriptor.get("headers")
    rows_are_objects = descriptor.get("rowsAreObjects") is True
    if not isinstance(headers, list):
        return None
    if rows_are_objects and isinstance(value, list):
        return headers, value, True
    candidate = value.get("data") if isinstance(value, dict) else None
    if not isinstance(candidate, dict) and isinstance(value, dict):
        candidate = value
    rows = candidate.get("values") if isinstance(candidate, dict) else None
    if not isinstance(rows, list):
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
