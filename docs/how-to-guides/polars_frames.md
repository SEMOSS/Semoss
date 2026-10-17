# Polars frames

Polars is an explicit Python-backed SEMOSS frame type. Existing `PY`, `PYTHON`,
`PYFRAME`, and `PANDAS` frame types continue to create pandas frames.

## Runtime dependency

Polars is declared in `py/install_config/pyproject.toml`. For a source checkout,
synchronize the same uv environment referenced by `RDF_Map.prop`:

```bash
cd py/install_config
uv sync --extra cpu
```

SEMOSS does not install Polars when a request is executed. If the configured
environment is missing Polars, `CreateFrame(frameType=["POLARS"])` returns an
actionable initialization error.

## Creating and importing

```pixel
polarsFrame = CreateFrame(
    frameType=["POLARS"],
    override=[true],
    alias=["polarsFrame"]
);
```

Normal file/database import pipelines can target the frame:

```pixel
FileRead(filePath=["/data/orders.csv"])
| Import(frame=[
    CreateFrame(
        frameType=["POLARS"],
        override=[true],
        alias=["orders"]
    )
]);
```

CSV and parquet sources use native Polars readers. Excel and database/query
results use the existing SEMOSS iterator import contract.

An existing Python `polars.DataFrame` can be registered without evaluating an
arbitrary Python expression:

```pixel
GenerateFrameFromPolarsVariable(
    variable=["orders_df"],
    override=[true]
);
```

`polars.LazyFrame` registration is intentionally rejected. Materialize it with
`collect()` before registration.

## Query and transformation behavior

Normal `SelectQueryStruct` operations execute as Polars expressions and lazy
plans. Supported query behavior includes:

- ordered column selection and aliases;
- constants and arithmetic selectors;
- nested AND/OR comparison, membership, null, contains, prefix, and suffix
  filters;
- frame filters combined with query filters;
- distinct, multi-column sorting, limit, and offset;
- grouping plus count, distinct count, sum, average, min, and max;
- scalar aggregates and grouped HAVING filters;
- normal grid wrappers and SEMOSS metadata types.

The owned frame remains eager; each query builds a lazy plan and collects only
the requested result. Raw SQL/query strings and non-grid output formats are
rejected instead of falling back to pandas.

Supported frame-specific transformations are rename/drop/duplicate column,
strict type conversion, trim/upper/lower, literal replacement, filtered row
updates, and filtered row deletion. Same-backend equality joins and schema-
aligned union/union-all execute through Polars. Generic conversion uses the
normal SEMOSS iterator boundary, including pandas-to-Polars and
Polars-to-pandas conversion.

## Type policy

| Polars dtype | SEMOSS type |
|---|---|
| Signed integers, UInt8/16/32 | `INT` |
| Float32/64 | `DOUBLE` |
| String, Categorical, Enum, Null | `STRING` |
| Boolean | `BOOLEAN` |
| Date | `DATE` |
| Datetime (unit/timezone retained by Polars) | `TIMESTAMP` |

UInt64, decimal, binary, time, duration, list/array, struct, and object columns
are rejected because the current SEMOSS scalar row/metadata contract cannot
preserve them without narrowing or stringification.

Polars null and IEEE NaN remain distinct. `COUNT(column)` excludes nulls,
distinct count excludes nulls, null grouping keys are retained, and joins do
not match null keys. Row order is preserved for projection, distinct, grouping,
and union where Polars exposes a maintain-order contract; callers must specify
sorts when deterministic value ordering is required.

## Persistence and cleanup

Frame data is persisted as Arrow IPC. Metadata, filters, aliases, and source
lineage use existing `CachePropFileFrameObject` helpers. Restore creates a new
wrapper bound to the destination Insight's `PyTranslator`; live Python objects
are not reused.

The existing cache cipher protects metadata/filter files where configured.
Like existing pandas frame persistence, the Arrow data file itself is not
encrypted by that cipher. Deployments requiring encrypted cached data must use
encrypted storage or disable persisted frame caches.

Closing a Polars frame removes only its generated wrapper from that Insight's
Python runtime. It does not close the Insight translator or delete other
frames.
