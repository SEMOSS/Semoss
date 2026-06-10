"""
JsonLogic - A Python implementation of the JsonLogic specification.

This module provides a way to evaluate serializable logic rules expressed as
JSON (Python dicts) against a data context. Rules are data - they can be stored
in a database, transmitted over an API, and evaluated without arbitrary code
execution.

Includes Semoss-specific extended operations (regex, fuzzy matching, date math,
type casting, collection helpers) and a DataFrame integration helper.

Based on the JsonLogic specification: https://jsonlogic.com
Original JS reference: https://github.com/jwadhams/json-logic-js

Usage::

    from utils.json_logic import JsonLogic, create_semoss_engine

    # Vanilla engine (spec-only operators)
    engine = JsonLogic()
    engine.apply({">": [{"var": "age"}, 21]}, {"age": 25})  # True

    # Semoss engine (spec + extended operators)
    engine = create_semoss_engine()
    engine.apply({"regex_match": ["^hello", "hello world"]})  # True

    # DataFrame integration
    from utils.json_logic import apply_rule_to_dataframe
    import pandas as pd
    df = pd.DataFrame({"age": [18, 25, 30]})
    apply_rule_to_dataframe(engine, {">=": [{"var": "age"}, 21]}, df)
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from functools import reduce
from typing import Any, Callable, Optional

import pandas as pd

logger: logging.Logger = logging.getLogger(__name__)


class JsonLogicError(Exception):
    """Raised when a JsonLogic rule cannot be evaluated."""

    pass


class JsonLogic:
    """
    Evaluates JsonLogic rules against a data context.

    JsonLogic rules are plain Python dicts (deserialized JSON) where each key
    is an operator and each value is the operand(s). The engine recursively
    evaluates nested rules, resolves variable references, and applies the
    requested operations.

    Attributes:
        operations: A dict mapping operator names to their callable
            implementations. Can be extended via ``add_operation``.
    """

    def __init__(self) -> None:
        self.operations: dict[str, Callable[..., Any]] = self._build_default_operations()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def apply(self, rule: Any, data: Optional[dict[str, Any]] = None) -> Any:
        """
        Evaluate a JsonLogic *rule* against *data* and return the result.

        Args:
            rule: A JsonLogic rule (dict), a primitive, or a list. If *rule*
                is not a dict it is returned as-is (base case of recursion).
            data: The data context used when resolving ``var`` references.
                Defaults to an empty dict.

        Returns:
            The evaluation result - type depends on the rule.

        Raises:
            JsonLogicError: If the rule references an unrecognised operator.
        """
        if data is None:
            data = {}

        # Base case - primitives and lists are returned directly.
        if rule is None or not isinstance(rule, dict):
            return rule

        # A rule dict must have exactly one key (the operator).
        operator = next(iter(rule))
        values = rule[operator]

        # Normalise unary shorthand: {"var": "x"} -> {"var": ["x"]}
        if not isinstance(values, (list, tuple)):
            values = [values]

        # --- Operators that need *unevaluated* or partially-evaluated args ---

        if operator == "var":
            return self._get_var(data, *[self.apply(v, data) for v in values])

        if operator == "missing":
            return self._missing(data, *[self.apply(v, data) for v in values])

        if operator == "missing_some":
            evaluated = [self.apply(v, data) for v in values]
            return self._missing_some(data, *evaluated)

        if operator == "if" or operator == "?:":
            return self._if(values, data)

        # Short-circuit boolean operators
        if operator == "and":
            return self._and(values, data)

        if operator == "or":
            return self._or(values, data)

        # Array iteration operators receive the raw sub-rule for the callback
        if operator == "filter":
            return self._filter(values, data)

        if operator == "map":
            return self._map(values, data)

        if operator == "reduce":
            return self._reduce(values, data)

        if operator == "all":
            return self._all(values, data)

        if operator == "some":
            return self._some(values, data)

        if operator == "none":
            return self._none(values, data)

        # --- Standard eager-evaluated operators ---

        if operator not in self.operations:
            raise JsonLogicError(f"Unrecognised operation: {operator}")

        evaluated_values = [self.apply(v, data) for v in values]
        return self.operations[operator](*evaluated_values)

    def add_operation(self, name: str, func: Callable[..., Any]) -> None:
        """
        Register a custom operation (or override a built-in one).

        Args:
            name: The operator name as it will appear in rules, e.g.
                ``"regex_match"``.
            func: A callable that implements the operation. It will receive
                the (already-evaluated) operands as positional arguments.

        Raises:
            ValueError: If *name* is empty or *func* is not callable.
        """
        if not name:
            raise ValueError("Operation name must be a non-empty string.")
        if not callable(func):
            raise ValueError(f"Operation handler for '{name}' must be callable.")
        self.operations[name] = func

    def remove_operation(self, name: str) -> None:
        """
        Remove a previously registered operation.

        Args:
            name: The operator name to remove.

        Raises:
            KeyError: If the operation does not exist.
        """
        if name not in self.operations:
            raise KeyError(f"Operation '{name}' is not registered.")
        del self.operations[name]

    # ------------------------------------------------------------------
    # Built-in operator implementations
    # ------------------------------------------------------------------

    @staticmethod
    def _build_default_operations() -> dict[str, Callable[..., Any]]:
        """Return the default set of JsonLogic operations."""
        return {
            # Comparison
            "==": JsonLogic._soft_equals,
            "===": JsonLogic._hard_equals,
            "!=": lambda a, b: not JsonLogic._soft_equals(a, b),
            "!==": lambda a, b: not JsonLogic._hard_equals(a, b),
            ">": lambda a, b: JsonLogic._less(b, a),
            ">=": lambda a, b: JsonLogic._less(b, a) or JsonLogic._soft_equals(a, b),
            "<": JsonLogic._less,
            "<=": JsonLogic._less_or_equal,
            # Logic
            "!": lambda a: not a,
            "!!": bool,
            # Arithmetic
            "+": JsonLogic._plus,
            "-": JsonLogic._minus,
            "*": lambda *args: reduce(lambda total, arg: total * float(arg), args, 1),
            "/": lambda a, b=None: a if b is None else float(a) / float(b),
            "%": lambda a, b: a % b,
            "min": lambda *args: min(args),
            "max": lambda *args: max(args),
            # String / Containment
            "cat": lambda *args: "".join(str(arg) for arg in args),
            "substr": JsonLogic._substr,
            "in": lambda a, b: a in b if hasattr(b, "__contains__") else False,
            # Array
            "merge": JsonLogic._merge,
            "count": lambda *args: sum(1 if a else 0 for a in args),
            # Logging
            "log": lambda a: logger.info(a) or a,
        }

    # -- Comparison helpers ------------------------------------------------

    @staticmethod
    def _soft_equals(a: Any, b: Any) -> bool:
        """
        JS-style ``==`` with type coercion.

        If either operand is a string the other is coerced to string.
        If either is a bool both are compared as bools.
        """
        if isinstance(a, str) or isinstance(b, str):
            return str(a) == str(b)
        if isinstance(a, bool) or isinstance(b, bool):
            return bool(a) is bool(b)
        return a == b

    @staticmethod
    def _hard_equals(a: Any, b: Any) -> bool:
        """JS-style ``===`` - no type coercion."""
        if type(a) is not type(b):
            return False
        return a == b

    @staticmethod
    def _less(a: Any, b: Any, *args: Any) -> bool:
        """
        JS-style ``<`` with numeric coercion.

        Supports between-style chaining: ``{"<": [1, {"var": "x"}, 10]}``.
        """
        types = {type(a), type(b)}
        if float in types or int in types:
            try:
                a, b = float(a), float(b)
            except (TypeError, ValueError):
                return False
        result = a < b
        if args:
            return result and JsonLogic._less(b, *args)
        return result

    @staticmethod
    def _less_or_equal(a: Any, b: Any, *args: Any) -> bool:
        """JS-style ``<=`` with numeric coercion."""
        result = JsonLogic._less(a, b) or JsonLogic._soft_equals(a, b)
        if args:
            return result and JsonLogic._less_or_equal(b, *args)
        return result

    # -- Numeric helpers ---------------------------------------------------

    @staticmethod
    def _to_numeric(arg: Any) -> int | float:
        """
        Convert a value to a numeric type.

        Strings containing a decimal point become floats, otherwise ints.
        Non-string values are returned as-is.
        """
        if isinstance(arg, str):
            return float(arg) if "." in arg else int(arg)
        return arg

    @staticmethod
    def _plus(*args: Any) -> int | float:
        """Sum with automatic type coercion."""
        return sum(JsonLogic._to_numeric(a) for a in args)

    @staticmethod
    def _minus(*args: Any) -> int | float:
        """Subtraction (unary negation when given a single arg)."""
        if len(args) == 1:
            return -JsonLogic._to_numeric(args[0])
        return JsonLogic._to_numeric(args[0]) - JsonLogic._to_numeric(args[1])

    # -- String helpers ----------------------------------------------------

    @staticmethod
    def _substr(source: str, start: int, end: Optional[int] = None) -> str:
        """Implements the ``substr`` operator (JS-style semantics)."""
        if end is None:
            return str(source)[int(start):]
        if end < 0:
            return str(source)[int(start):end]
        return str(source)[int(start):int(start) + int(end)]

    # -- Array helpers -----------------------------------------------------

    @staticmethod
    def _merge(*args: Any) -> list[Any]:
        """Flatten one level of nesting."""
        result: list[Any] = []
        for arg in args:
            if isinstance(arg, (list, tuple)):
                result.extend(arg)
            else:
                result.append(arg)
        return result

    # -- Variable access ---------------------------------------------------

    @staticmethod
    def _get_var(data: Any, var_name: Any = None, not_found: Any = None) -> Any:
        """
        Retrieve a value from *data* using dot-notation.

        Args:
            data: The data dict (or nested structure) to traverse.
            var_name: Dot-separated path, e.g. ``"user.address.city"``.
                An empty string returns the entire data object.
            not_found: Value to return when the path does not resolve.

        Returns:
            The resolved value, or *not_found*.
        """
        if var_name is None or var_name == "":
            return data

        try:
            for key in str(var_name).split("."):
                try:
                    data = data[key]
                except TypeError:
                    data = data[int(key)]
        except (KeyError, TypeError, ValueError, IndexError):
            return not_found
        return data

    @staticmethod
    def _missing(data: dict[str, Any], *args: Any) -> list[str]:
        """Return a list of variable names that are not present in *data*."""
        _not_found = object()
        if args and isinstance(args[0], list):
            args = tuple(args[0])
        return [
            arg for arg in args
            if JsonLogic._get_var(data, arg, _not_found) is _not_found
        ]

    @staticmethod
    def _missing_some(
        data: dict[str, Any], min_required: int, args: list[str],
    ) -> list[str]:
        """
        Return missing variable names only when fewer than *min_required*
        of the listed variables are present.
        """
        if min_required < 1:
            return []

        _not_found = object()
        found = 0
        missing: list[str] = []
        for arg in args:
            if JsonLogic._get_var(data, arg, _not_found) is _not_found:
                missing.append(arg)
            else:
                found += 1
                if found >= min_required:
                    return []
        return missing

    # -- Control-flow (lazy/short-circuit) ---------------------------------

    def _if(self, values: list[Any], data: dict[str, Any]) -> Any:
        """Lazy ``if`` / ``?:`` - only evaluates the branch taken."""
        for i in range(0, len(values) - 1, 2):
            if self.apply(values[i], data):
                return self.apply(values[i + 1], data)
        # Else branch (odd number of args)
        if len(values) % 2:
            return self.apply(values[-1], data)
        return None

    def _and(self, values: list[Any], data: dict[str, Any]) -> Any:
        """Short-circuit ``and`` - returns last truthy or first falsy value."""
        result: Any = True
        for value in values:
            result = self.apply(value, data)
            if not result:
                return result
        return result

    def _or(self, values: list[Any], data: dict[str, Any]) -> Any:
        """Short-circuit ``or`` - returns first truthy or last falsy value."""
        result: Any = False
        for value in values:
            result = self.apply(value, data)
            if result:
                return result
        return result

    # -- Array iteration (lazy - callback rule is not pre-evaluated) -------

    def _filter(self, values: list[Any], data: dict[str, Any]) -> list[Any]:
        """Return elements of the array for which the rule is truthy."""
        source = self.apply(values[0], data)
        if not isinstance(source, list):
            return []
        return [item for item in source if self.apply(values[1], item)]

    def _map(self, values: list[Any], data: dict[str, Any]) -> list[Any]:
        """Apply a rule to each element and return the results."""
        source = self.apply(values[0], data)
        if not isinstance(source, list):
            return []
        return [self.apply(values[1], item) for item in source]

    def _reduce(self, values: list[Any], data: dict[str, Any]) -> Any:
        """Reduce an array with a rule. ``current`` and ``accumulator`` vars."""
        source = self.apply(values[0], data)
        if not isinstance(source, list):
            source = []
        initial = self.apply(values[2], data) if len(values) > 2 else None
        accumulator = initial
        for current in source:
            accumulator = self.apply(
                values[1],
                {"current": current, "accumulator": accumulator},
            )
        return accumulator

    def _all(self, values: list[Any], data: dict[str, Any]) -> bool:
        """True if the rule is truthy for every element."""
        source = self.apply(values[0], data)
        if not isinstance(source, list) or len(source) == 0:
            return False
        return all(self.apply(values[1], item) for item in source)

    def _some(self, values: list[Any], data: dict[str, Any]) -> bool:
        """True if the rule is truthy for at least one element."""
        source = self.apply(values[0], data)
        if not isinstance(source, list) or len(source) == 0:
            return False
        return any(self.apply(values[1], item) for item in source)

    def _none(self, values: list[Any], data: dict[str, Any]) -> bool:
        """True if the rule is falsy for every element."""
        source = self.apply(values[0], data)
        if not isinstance(source, list) or len(source) == 0:
            return True
        return not any(self.apply(values[1], item) for item in source)


# ------------------------------------------------------------------
# Convenience singleton and function for quick usage
# ------------------------------------------------------------------

_default_engine = JsonLogic()


def json_logic(rule: Any, data: Optional[dict[str, Any]] = None) -> Any:
    """
    Evaluate a JsonLogic *rule* against *data* using the default engine.

    This is a module-level convenience function. For custom operations, create
    your own ``JsonLogic`` instance instead.

    Args:
        rule: A JsonLogic rule dict.
        data: The data context.

    Returns:
        The evaluation result.
    """
    return _default_engine.apply(rule, data)


# ------------------------------------------------------------------
# Semoss extended operations
# ------------------------------------------------------------------


def _regex_match(pattern: str, string: str) -> bool:
    """
    Test whether *string* matches the regex *pattern*.

    Args:
        pattern: A Python-style regular expression.
        string: The string to test.

    Returns:
        True if the pattern matches anywhere in the string.
    """
    try:
        return bool(re.search(str(pattern), str(string)))
    except re.error as exc:
        logger.warning("Invalid regex pattern '%s': %s", pattern, exc)
        return False


def _regex_extract(pattern: str, string: str, group: int = 0) -> Optional[str]:
    """
    Extract the first match (or a specific capture group) from *string*.

    Args:
        pattern: A Python-style regular expression.
        string: The string to search.
        group: The capture group index (default ``0`` = full match).

    Returns:
        The matched text, or ``None`` if no match.
    """
    try:
        match = re.search(str(pattern), str(string))
        return match.group(int(group)) if match else None
    except (re.error, IndexError) as exc:
        logger.warning("regex_extract error for '%s': %s", pattern, exc)
        return None


def _fuzzy_ratio(a: str, b: str) -> int:
    """
    Compute fuzzy string similarity using ``thefuzz``.

    Args:
        a: First string.
        b: Second string.

    Returns:
        Similarity score from 0 to 100.
    """
    from thefuzz import fuzz

    return fuzz.ratio(str(a), str(b))


def _fuzzy_partial_ratio(a: str, b: str) -> int:
    """
    Compute partial fuzzy string similarity using ``thefuzz``.

    Args:
        a: First string.
        b: Second string.

    Returns:
        Partial similarity score from 0 to 100.
    """
    from thefuzz import fuzz

    return fuzz.partial_ratio(str(a), str(b))


def _upper(s: str) -> str:
    """Convert a string to uppercase."""
    return str(s).upper()


def _lower(s: str) -> str:
    """Convert a string to lowercase."""
    return str(s).lower()


def _trim(s: str) -> str:
    """Strip leading/trailing whitespace."""
    return str(s).strip()


def _length(obj: Any) -> int:
    """Return the length of a string, list, or dict."""
    try:
        return len(obj)
    except TypeError:
        return 0


def _now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(tz=timezone.utc).isoformat()


def _date_diff_days(date_a: str, date_b: str) -> int:
    """
    Return the number of days between two ISO-8601 date strings.

    Args:
        date_a: Start date (ISO-8601).
        date_b: End date (ISO-8601).

    Returns:
        Signed integer difference in days (``date_b - date_a``).
    """
    try:
        a = datetime.fromisoformat(date_a)
        b = datetime.fromisoformat(date_b)
        return (b - a).days
    except (ValueError, TypeError) as exc:
        logger.warning("date_diff_days error: %s", exc)
        return 0


def _coalesce(*args: Any) -> Any:
    """Return the first non-None argument (SQL-style COALESCE)."""
    for arg in args:
        if arg is not None:
            return arg
    return None


def _type_of(value: Any) -> str:
    """Return the Python type name of *value* as a string."""
    return type(value).__name__


def _to_int(value: Any) -> int:
    """Cast a value to int."""
    return int(value)


def _to_float(value: Any) -> float:
    """Cast a value to float."""
    return float(value)


def _to_str(value: Any) -> str:
    """Cast a value to str."""
    return str(value)


def _contains_all(haystack: list, *needles: Any) -> bool:
    """True if *haystack* contains every one of *needles*."""
    if not isinstance(haystack, (list, tuple, set)):
        return False
    return all(n in haystack for n in needles)


def _contains_any(haystack: list, *needles: Any) -> bool:
    """True if *haystack* contains at least one of *needles*."""
    if not isinstance(haystack, (list, tuple, set)):
        return False
    return any(n in haystack for n in needles)


def _distinct(arr: list) -> list:
    """Return a list with duplicates removed (order preserved)."""
    seen: set = set()
    result: list = []
    for item in arr if isinstance(arr, (list, tuple)) else []:
        key = id(item) if isinstance(item, (dict, list)) else item
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


_SEMOSS_OPERATIONS: dict[str, Any] = {
    # String
    "regex_match": _regex_match,
    "regex_extract": _regex_extract,
    "fuzzy_ratio": _fuzzy_ratio,
    "fuzzy_partial_ratio": _fuzzy_partial_ratio,
    "upper": _upper,
    "lower": _lower,
    "trim": _trim,
    "length": _length,
    # Type casting
    "to_int": _to_int,
    "to_float": _to_float,
    "to_str": _to_str,
    "type_of": _type_of,
    # Date/Time
    "now": _now_iso,
    "date_diff_days": _date_diff_days,
    # Collections
    "coalesce": _coalesce,
    "contains_all": _contains_all,
    "contains_any": _contains_any,
    "distinct": _distinct,
}


def create_semoss_engine() -> JsonLogic:
    """
    Create a ``JsonLogic`` engine pre-loaded with all Semoss-specific
    custom operations.

    Returns:
        A fully configured ``JsonLogic`` instance.
    """
    engine = JsonLogic()
    for name, func in _SEMOSS_OPERATIONS.items():
        engine.add_operation(name, func)
    return engine


# ------------------------------------------------------------------
# DataFrame integration
# ------------------------------------------------------------------


def apply_rule_to_dataframe(
    engine: JsonLogic,
    rule: dict,
    df: pd.DataFrame,
) -> pd.Series:
    """
    Evaluate a JsonLogic *rule* against every row of a DataFrame.

    Each row is converted to a dict and passed as the data context. This
    allows rules like ``{">=": [{"var": "age"}, 21]}`` to be applied
    across tabular data without writing per-column code.

    Args:
        engine: The ``JsonLogic`` engine to use for evaluation.
        rule: A JsonLogic rule dict.
        df: The pandas DataFrame whose rows serve as data contexts.

    Returns:
        A ``pd.Series`` containing one result per row.
    """
    return df.apply(lambda row: engine.apply(rule, row.to_dict()), axis=1)


# ------------------------------------------------------------------
# Java Integration API
# ------------------------------------------------------------------

# Singleton engine for Java integration
_JAVA_ENGINE = None


def evaluate_json(rule_json: str, data_json: str = None) -> str:
    """
    Evaluate JSON Logic using JSON strings (for Java integration).

    This function provides a JSON-string-based interface to the Semoss JSON Logic
    engine, designed to be called from Java reactors via PyTranslator.

    Args:
        rule_json: JSON string containing the rule
        data_json: JSON string containing the data (default: None)

    Returns:
        JSON string containing the result

    Raises:
        JsonLogicError: If the rule cannot be evaluated or JSON is invalid
    """
    global _JAVA_ENGINE
    if _JAVA_ENGINE is None:
        _JAVA_ENGINE = create_semoss_engine()

    try:
        import json
        rule = json.loads(rule_json)
        data = json.loads(data_json) if data_json else None
        result = _JAVA_ENGINE.apply(rule, data)
        return json.dumps(result)
    except json.JSONDecodeError as e:
        raise JsonLogicError(f"Invalid JSON input: {e}")
    except Exception as e:
        raise JsonLogicError(f"Error evaluating JSON Logic rule: {e}")
