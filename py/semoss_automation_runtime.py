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
from typing import Any


_PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")


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

        exact = _PLACEHOLDER_PATTERN.fullmatch(value)
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
        if name not in self:
            raise KeyError(
                f"Generated automation configuration references unavailable "
                f"scope value '{name}'."
            )
        return self[name]

    def resolve_config(self, value: Any) -> Any:
        """Resolve a generated-node configuration while preserving native types."""
        if isinstance(value, str):
            value = json.loads(value or "{}")
        elif value is None:
            value = {}
        return self.resolve(value)


def execute_node(encoded_scope: str, encoded_source: str, max_output_bytes: int) -> Any:
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
    return _json_result(run(scope), max_output_bytes)


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
