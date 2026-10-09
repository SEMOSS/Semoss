"""Opt-in pandas/Polars correctness benchmark for the SEMOSS frame workload."""

from __future__ import annotations

import argparse
import platform
import time
import tracemalloc
from collections.abc import Callable
from typing import TypeVar

import numpy as np
import pandas as pd
import polars as pl
from polars.testing import assert_frame_equal

T = TypeVar("T")


def timed(name: str, operation: Callable[[], T]) -> T:
    """Run one benchmark stage and report elapsed time and Python peak memory."""
    tracemalloc.start()
    start = time.perf_counter()
    result = operation()
    elapsed = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"{name}: seconds={elapsed:.6f} python_peak_mb={peak / 1024 / 1024:.2f}")
    return result


def main(rows: int) -> None:
    """Compare equivalent pandas and Polars work on deterministic input."""
    rng = np.random.default_rng(7)
    source = {
        "category": rng.choice(["a", "b", "c", None], rows).tolist(),
        "value": rng.integers(0, 10_000, rows, dtype=np.int64),
        "amount": rng.normal(100, 20, rows),
    }
    print(
        f"python={platform.python_version()} platform={platform.platform()} "
        f"rows={rows} pandas={pd.__version__} polars={pl.__version__}"
    )

    pandas_frame = timed("pandas_import", lambda: pd.DataFrame(source))
    polars_frame = timed("polars_import", lambda: pl.DataFrame(source))

    pandas_result = timed(
        "pandas_compute",
        lambda: (
            pandas_frame.loc[pandas_frame["value"] > 5_000, ["category", "amount"]]
            .groupby("category", dropna=False, as_index=False)
            .agg(total=("amount", "sum"))
            .sort_values("category", na_position="first")
            .reset_index(drop=True)
        ),
    )
    polars_result = timed(
        "polars_compute",
        lambda: (
            polars_frame.lazy()
            .filter(pl.col("value") > 5_000)
            .group_by("category", maintain_order=True)
            .agg(pl.col("amount").sum().alias("total"))
            .sort("category", nulls_last=False)
            .collect()
        ),
    )

    expected = pl.from_pandas(pandas_result)
    assert_frame_equal(
        polars_result,
        expected,
        check_row_order=True,
        check_column_order=True,
        check_dtypes=False,
        rel_tol=1e-9,
    )
    timed("pandas_materialize", lambda: pandas_result.to_dict("split"))
    timed("polars_materialize", lambda: list(polars_result.iter_rows()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rows",
        type=int,
        default=1_000_000,
        help="number of deterministic source rows",
    )
    main(parser.parse_args().rows)
