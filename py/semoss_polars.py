"""Native Polars frame support used by SEMOSS through PyTranslator."""

from __future__ import annotations

import datetime as dt
import math
from pathlib import Path
from typing import Any

try:
    import polars as pl
except ImportError as exc:  # pragma: no cover - exercised by Java integration
    raise ImportError(
        "Polars frame support requires the 'polars' package from "
        "py/install_config/pyproject.toml. Run uv sync in py/install_config."
    ) from exc


_INTEGER_TYPES = {
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
}
_FLOAT_TYPES = {pl.Float32, pl.Float64}
_STRING_TYPES = {pl.String, pl.Categorical, pl.Enum}
_UNSUPPORTED_TYPE_NAMES = {
    "Array",
    "Binary",
    "Decimal",
    "Duration",
    "List",
    "Object",
    "Struct",
    "Time",
    "UInt64",
}


def _dtype_base_name(dtype: pl.DataType) -> str:
    return str(dtype).split("(", 1)[0]


def semoss_type(dtype: pl.DataType) -> str:
    """Map one supported Polars dtype to its SEMOSS scalar type."""
    if dtype in _INTEGER_TYPES:
        return "INT"
    if dtype in _FLOAT_TYPES:
        return "DOUBLE"
    if dtype == pl.Boolean:
        return "BOOLEAN"
    if dtype == pl.Date:
        return "DATE"
    if isinstance(dtype, pl.Datetime):
        return "TIMESTAMP"
    if dtype in _STRING_TYPES or dtype == pl.Null:
        return "STRING"
    raise TypeError(
        f"Polars dtype '{dtype}' is not supported by the SEMOSS scalar frame contract"
    )


def _validate_schema(frame: pl.DataFrame) -> None:
    unsupported = [
        f"{name}: {dtype}"
        for name, dtype in frame.schema.items()
        if _dtype_base_name(dtype) in _UNSUPPORTED_TYPE_NAMES
    ]
    if unsupported:
        raise TypeError(
            "Unsupported Polars frame schema: " + ", ".join(unsupported)
        )
    for dtype in frame.schema.values():
        semoss_type(dtype)


def _column_name(value: str) -> str:
    return value.split("__", 1)[1] if "__" in value else value


def _literal(value: Any) -> pl.Expr:
    if isinstance(value, dict) and value.get("kind") == "value":
        value = value.get("value")
    return pl.lit(value)


def _expression(spec: dict[str, Any], *, aggregate: bool = False) -> pl.Expr:
    kind = spec["kind"]
    alias = spec.get("alias")

    if kind == "column":
        expr = pl.col(_column_name(spec["column"]))
    elif kind == "literal":
        expr = _literal(spec.get("value"))
    elif kind == "arithmetic":
        left = _expression(spec["left"])
        right = _expression(spec["right"])
        operator = spec["operator"]
        operations = {
            "+": lambda: left + right,
            "-": lambda: left - right,
            "*": lambda: left * right,
            "/": lambda: left / right,
            "%": lambda: left % right,
        }
        if operator not in operations:
            raise ValueError(f"Unsupported Polars arithmetic operator '{operator}'")
        expr = operations[operator]()
    elif kind == "function":
        function = spec["function"].upper()
        inputs = spec.get("inputs", [])
        source = _expression(inputs[0]) if inputs else None
        if function == "COUNT":
            expr = source.count() if source is not None else pl.len()
        elif function in {"UNIQUE_COUNT", "COUNT_DISTINCT"}:
            if source is None:
                raise ValueError(f"{function} requires a column")
            expr = source.drop_nulls().n_unique()
        elif function == "SUM":
            expr = source.sum()
        elif function in {"AVERAGE", "AVG", "MEAN"}:
            expr = source.mean()
        elif function == "MIN":
            expr = source.min()
        elif function == "MAX":
            expr = source.max()
        else:
            raise ValueError(f"Unsupported Polars query function '{function}'")
    else:
        raise ValueError(f"Unsupported Polars expression kind '{kind}'")

    return expr.alias(alias) if alias else expr


def _operand(spec: dict[str, Any]) -> pl.Expr:
    if spec["kind"] == "column":
        return pl.col(_column_name(spec["column"]))
    return _literal(spec.get("value"))


def _simple_filter(spec: dict[str, Any]) -> pl.Expr:
    left_spec = spec["left"]
    right_spec = spec["right"]
    left = _operand(left_spec)
    right = _operand(right_spec)
    comparator = spec["comparator"].lower()
    right_value = right_spec.get("value")

    if comparator in {"==", "="}:
        if right_value is None:
            return left.is_null()
        if isinstance(right_value, list):
            return left.is_in(right_value)
        return left == right
    if comparator in {"!=", "<>"}:
        if right_value is None:
            return left.is_not_null()
        if isinstance(right_value, list):
            return ~left.is_in(right_value)
        return left != right
    if comparator == ">":
        return left > right
    if comparator == ">=":
        return left >= right
    if comparator == "<":
        return left < right
    if comparator == "<=":
        return left <= right
    if comparator in {"?like", "?nlike", "?begins", "?nbegins", "?ends", "?nends"}:
        if isinstance(right_value, list):
            if len(right_value) != 1:
                raise ValueError(f"{comparator} accepts one search value")
            right_value = right_value[0]
        text = "" if right_value is None else str(right_value)
        string_expr = left.cast(pl.String)
        if comparator in {"?like", "?nlike"}:
            result = string_expr.str.contains(text, literal=True)
        elif comparator in {"?begins", "?nbegins"}:
            result = string_expr.str.starts_with(text)
        else:
            result = string_expr.str.ends_with(text)
        return ~result if comparator in {"?nlike", "?nbegins", "?nends"} else result
    raise ValueError(f"Unsupported Polars filter comparator '{comparator}'")


def _filter_expression(spec: dict[str, Any] | None) -> pl.Expr | None:
    if not spec:
        return None
    kind = spec["kind"]
    if kind == "simple":
        return _simple_filter(spec)
    children = [_filter_expression(child) for child in spec.get("children", [])]
    children = [child for child in children if child is not None]
    if not children:
        return None
    result = children[0]
    for child in children[1:]:
        result = result & child if kind == "and" else result | child
    return result


def _normalize_value(value: Any) -> Any:
    if isinstance(value, float) and math.isnan(value):
        return value
    if isinstance(value, (dt.date, dt.datetime)):
        return value
    return value


class SemossPolarsFrame:
    """Own one eager Polars DataFrame for a single SEMOSS Insight.

    Query plans are structural dictionaries produced by Java and execute as
    Polars expressions. Mutations validate a replacement DataFrame before
    rebinding ``data``, leaving the original unchanged when validation fails.
    """

    def __init__(self, frame: pl.DataFrame | None = None):
        """Create a wrapper around an eager, supported Polars DataFrame."""
        if frame is None:
            frame = pl.DataFrame()
        if isinstance(frame, pl.LazyFrame):
            raise TypeError(
                "Registering a Polars LazyFrame is unsupported; call collect() first"
            )
        if not isinstance(frame, pl.DataFrame):
            raise TypeError(
                f"Expected polars.DataFrame, received {type(frame).__name__}"
            )
        _validate_schema(frame)
        self.data = frame

    @classmethod
    def from_csv(cls, path: str, **options: Any) -> "SemossPolarsFrame":
        """Read a CSV file with native Polars options."""
        return cls(pl.read_csv(path, **options))

    @classmethod
    def from_parquet(cls, path: str, **options: Any) -> "SemossPolarsFrame":
        """Read a parquet file with native Polars options."""
        return cls(pl.read_parquet(path, **options))

    @classmethod
    def import_csv(
        cls,
        path: str,
        separator: str,
        columns: list[str],
        output_columns: list[str],
        limit: int,
        schema: dict[str, str],
    ) -> "SemossPolarsFrame":
        """Read a SEMOSS CSV import with projection, aliases, and type hints."""
        type_map = {
            "BOOLEAN": pl.Boolean,
            "INT": pl.Int64,
            "DOUBLE": pl.Float64,
            "STRING": pl.String,
            "DATE": pl.Date,
            "TIMESTAMP": pl.Datetime("us"),
        }
        schema_overrides = {
            column: type_map[value.upper()]
            for column, value in schema.items()
            if value and value.upper() in type_map
        }
        frame = pl.read_csv(
            path,
            separator=separator,
            columns=columns or None,
            n_rows=limit if limit >= 0 else None,
            schema_overrides=schema_overrides or None,
            try_parse_dates=True,
        )
        if output_columns:
            frame.columns = output_columns
        return cls(frame)

    @classmethod
    def import_parquet(
        cls,
        path: str,
        columns: list[str],
        output_columns: list[str],
        limit: int,
    ) -> "SemossPolarsFrame":
        """Read a projected SEMOSS parquet import and apply output aliases."""
        frame = pl.read_parquet(path, columns=columns or None)
        if limit >= 0:
            frame = frame.head(limit)
        if output_columns:
            frame.columns = output_columns
        return cls(frame)

    @classmethod
    def from_ipc(cls, path: str) -> "SemossPolarsFrame":
        """Restore frame data from an Arrow IPC file."""
        return cls(pl.read_ipc(path))

    @classmethod
    def from_rows(
        cls,
        headers: list[str],
        rows: list[list[Any]],
        schema: dict[str, str] | None = None,
    ) -> "SemossPolarsFrame":
        """Create a typed frame from the SEMOSS iterator row boundary.

        Timezone-aware timestamp strings are normalized to UTC. A column that
        mixes aware and naive timestamp strings is rejected because no single
        Polars datetime dtype can preserve both semantics.
        """
        polars_schema = None
        if schema:
            type_map = {
                "BOOLEAN": pl.Boolean,
                "INT": pl.Int64,
                "DOUBLE": pl.Float64,
                "STRING": pl.String,
                "DATE": pl.Date,
                "TIMESTAMP": pl.Datetime("us"),
            }
            polars_schema = {
                header: type_map.get(schema.get(header, "STRING").upper(), pl.String)
                for header in headers
            }
            for index, header in enumerate(headers):
                if schema.get(header, "").upper() != "TIMESTAMP":
                    continue
                has_naive_value = False
                has_timezone_value = False
                for row in rows:
                    value = row[index]
                    if not isinstance(value, str):
                        continue
                    try:
                        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
                    except ValueError:
                        continue
                    if parsed.tzinfo is not None:
                        has_timezone_value = True
                    else:
                        has_naive_value = True
                if has_naive_value and has_timezone_value:
                    raise ValueError(
                        f"Timestamp column '{header}' cannot mix timezone-aware "
                        "and timezone-naive values"
                    )
                if has_timezone_value:
                    polars_schema[header] = pl.Datetime("us", "UTC")
        if not rows:
            return cls(pl.DataFrame(schema=polars_schema or headers))

        frame = pl.DataFrame(rows, schema=headers, orient="row", strict=False)
        if polars_schema:
            expressions = []
            for header, target in polars_schema.items():
                column = pl.col(header)
                source = frame.schema[header]
                if target == pl.Date and source == pl.String:
                    column = column.str.to_date(strict=True)
                elif isinstance(target, pl.Datetime) and source == pl.String:
                    column = column.str.to_datetime(
                        time_unit=target.time_unit,
                        time_zone=target.time_zone,
                        strict=True,
                    )
                else:
                    column = column.cast(target, strict=True)
                expressions.append(column.alias(header))
            frame = frame.with_columns(expressions)
        return cls(frame)

    def clone(self) -> "SemossPolarsFrame":
        """Return an independent clone of the owned DataFrame."""
        return SemossPolarsFrame(self.data.clone())

    def clean_columns(self, columns: list[str]) -> None:
        """Replace all headers after SEMOSS validates and cleans them."""
        if len(columns) != self.data.width:
            raise ValueError("Cleaned Polars columns must match the frame width")
        replacement = self.data.clone()
        replacement.columns = columns
        self._replace(replacement)

    def schema(self) -> dict[str, Any]:
        """Return columns and Polars/SEMOSS types for metadata recreation."""
        _validate_schema(self.data)
        return {
            "columns": self.data.columns,
            "types": [semoss_type(dtype) for dtype in self.data.dtypes],
            "polarsTypes": [str(dtype) for dtype in self.data.dtypes],
        }

    def query(self, plan: dict[str, Any]) -> dict[str, Any]:
        """Execute a structural lazy query plan and materialize scalar rows."""
        lazy = self.data.lazy()
        row_filter = _filter_expression(plan.get("filter"))
        if row_filter is not None:
            lazy = lazy.filter(row_filter)

        selectors = plan.get("selectors", [])
        group_by = plan.get("groupBy", [])
        aggregate_selectors = [
            selector for selector in selectors if selector["kind"] == "function"
        ]
        sorts = plan.get("sort", [])
        if sorts and not group_by and not aggregate_selectors:
            lazy = self._sort(lazy, sorts)
        if group_by:
            group_exprs = [_expression(selector) for selector in group_by]
            lazy = lazy.group_by(group_exprs, maintain_order=True).agg(
                [_expression(selector, aggregate=True) for selector in aggregate_selectors]
            )
            selected_aliases = [selector["alias"] for selector in selectors]
            lazy = lazy.select(selected_aliases)
        elif aggregate_selectors:
            if len(aggregate_selectors) != len(selectors):
                raise ValueError(
                    "Polars scalar aggregates cannot be mixed with row selectors without GROUP BY"
                )
            lazy = lazy.select(
                [_expression(selector, aggregate=True) for selector in selectors]
            )
        elif selectors:
            lazy = lazy.select([_expression(selector) for selector in selectors])

        having_filter = _filter_expression(plan.get("having"))
        if having_filter is not None:
            lazy = lazy.filter(having_filter)
        if plan.get("distinct", False):
            lazy = lazy.unique(maintain_order=True)

        if sorts and (group_by or aggregate_selectors):
            lazy = self._sort(lazy, sorts)

        offset = max(int(plan.get("offset", 0)), 0)
        limit = int(plan.get("limit", -1))
        if limit >= 0:
            lazy = lazy.slice(offset, limit)
        elif offset:
            lazy = lazy.slice(offset)

        result = lazy.collect()
        _validate_schema(result)
        return {
            "columns": result.columns,
            "types": [semoss_type(dtype) for dtype in result.dtypes],
            "data": [
                [_normalize_value(value) for value in row]
                for row in result.iter_rows()
            ],
        }

    @staticmethod
    def _sort(lazy: pl.LazyFrame, sorts: list[dict[str, Any]]) -> pl.LazyFrame:
        return lazy.sort(
            [item["column"] for item in sorts],
            descending=[item.get("descending", False) for item in sorts],
            nulls_last=[item.get("nullsLast", False) for item in sorts],
            maintain_order=True,
        )

    def write_ipc(self, path: str) -> None:
        """Persist the owned DataFrame as Arrow IPC."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        self.data.write_ipc(target)

    def rename(self, old: str, new: str) -> None:
        """Rename one column transactionally."""
        self._replace(self.data.rename({old: new}, strict=True))

    def drop(self, columns: list[str]) -> None:
        """Drop columns transactionally."""
        self._replace(self.data.drop(columns, strict=True))

    def duplicate(self, source: str, target: str) -> None:
        """Duplicate one column under a new name."""
        self._replace(self.data.with_columns(pl.col(source).alias(target)))

    def cast(self, column: str, semoss_dtype: str) -> None:
        """Strictly cast a column to a supported SEMOSS scalar type."""
        type_map = {
            "BOOLEAN": pl.Boolean,
            "INT": pl.Int64,
            "DOUBLE": pl.Float64,
            "STRING": pl.String,
            "DATE": pl.Date,
            "TIMESTAMP": pl.Datetime("us"),
        }
        target = type_map.get(semoss_dtype.upper())
        if target is None:
            raise ValueError(f"Unsupported SEMOSS type '{semoss_dtype}'")
        self._replace(
            self.data.with_columns(pl.col(column).cast(target, strict=True))
        )

    def string_transform(self, columns: list[str], operation: str) -> None:
        """Apply a supported string transform to selected columns."""
        transforms = {
            "trim": lambda column: column.str.strip_chars(),
            "upper": lambda column: column.str.to_uppercase(),
            "lower": lambda column: column.str.to_lowercase(),
        }
        if operation not in transforms:
            raise ValueError(f"Unsupported string operation '{operation}'")
        expressions = [
            transforms[operation](pl.col(column).cast(pl.String)).alias(column)
            for column in columns
        ]
        self._replace(self.data.with_columns(expressions))

    def replace(
        self, column: str, old_value: Any, new_value: Any, regex: bool = False
    ) -> None:
        """Replace literal values in one column."""
        expression = pl.col(column)
        if regex:
            expression = expression.cast(pl.String).str.replace_all(
                str(old_value), str(new_value), literal=True
            )
        else:
            expression = expression.replace(old_value, new_value)
        self._replace(self.data.with_columns(expression.alias(column)))

    def update_rows(
        self, filter_spec: dict[str, Any], column: str, value: Any
    ) -> None:
        """Update one column where a required structural filter matches."""
        predicate = _filter_expression(filter_spec)
        if predicate is None:
            raise ValueError("A row filter is required for update")
        self._replace(
            self.data.with_columns(
                pl.when(predicate)
                .then(pl.lit(value))
                .otherwise(pl.col(column))
                .alias(column)
            )
        )

    def drop_rows(self, filter_spec: dict[str, Any]) -> None:
        """Delete rows where a required structural filter matches."""
        predicate = _filter_expression(filter_spec)
        if predicate is None:
            raise ValueError("A row filter is required for row deletion")
        self._replace(self.data.filter(~predicate))

    def append_rows(self, rows: list[list[Any]]) -> None:
        """Append rows using the existing DataFrame schema."""
        if not rows:
            return
        addition = pl.DataFrame(
            rows,
            schema=self.data.schema,
            orient="row",
            strict=True,
        )
        self._replace(self.data.vstack(addition))

    def union(self, other: "SemossPolarsFrame", distinct: bool) -> None:
        """Append a frame with an identical schema, optionally deduplicating."""
        if self.data.schema != other.data.schema:
            raise TypeError(
                "Polars union requires identical column names, order, and dtypes"
            )
        combined = pl.concat([self.data, other.data], how="vertical")
        if distinct:
            combined = combined.unique(maintain_order=True)
        self._replace(combined)

    def merge_rows(
        self,
        headers: list[str],
        rows: list[list[Any]],
        schema: dict[str, str],
        left_on: list[str],
        right_on: list[str],
        how: str,
    ) -> None:
        """Equality-join typed iterator rows into this frame."""
        right = SemossPolarsFrame.from_rows(headers, rows, schema).data
        merged = self.data.join(
            right,
            left_on=left_on,
            right_on=right_on,
            how=how,
            suffix="_right",
            nulls_equal=False,
            maintain_order="left_right",
        )
        self._replace(merged)

    def _replace(self, replacement: pl.DataFrame) -> None:
        _validate_schema(replacement)
        self.data = replacement
