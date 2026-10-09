import datetime as dt

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from semoss_polars import SemossPolarsFrame


@pytest.fixture
def frame():
    return SemossPolarsFrame(
        pl.DataFrame(
            {
                "name": ["alpha", "beta", "alpha", None],
                "value": [3, 1, 5, 8],
                "amount": [1.5, float("nan"), None, 2.5],
                "when": [
                    dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
                    dt.datetime(2026, 1, 2, tzinfo=dt.UTC),
                    dt.datetime(2026, 1, 3, tzinfo=dt.UTC),
                    dt.datetime(2026, 1, 4, tzinfo=dt.UTC),
                ],
            }
        )
    )


def test_query_filter_sort_and_page(frame):
    result = frame.query(
        {
            "selectors": [
                {"kind": "column", "column": "name", "alias": "label"},
                {"kind": "column", "column": "value", "alias": "value"},
            ],
            "filter": {
                "kind": "simple",
                "left": {"kind": "column", "column": "value"},
                "comparator": ">",
                "right": {"kind": "value", "value": 2},
            },
            "sort": [{"column": "value", "descending": True}],
            "distinct": False,
            "offset": 1,
            "limit": 2,
        }
    )
    assert result["columns"] == ["label", "value"]
    assert result["types"] == ["STRING", "INT"]
    assert result["data"] == [["alpha", 5], ["alpha", 3]]


def test_grouped_and_scalar_aggregates(frame):
    grouped = frame.query(
        {
            "selectors": [
                {"kind": "column", "column": "name", "alias": "name"},
                {
                    "kind": "function",
                    "function": "SUM",
                    "inputs": [{"kind": "column", "column": "value"}],
                    "alias": "total",
                },
            ],
            "groupBy": [{"kind": "column", "column": "name", "alias": "name"}],
            "sort": [{"column": "name"}],
        }
    )
    assert grouped["data"] == [[None, 8], ["alpha", 8], ["beta", 1]]

    scalar = frame.query(
        {
            "selectors": [
                {
                    "kind": "function",
                    "function": "COUNT",
                    "inputs": [{"kind": "column", "column": "name"}],
                    "alias": "count_name",
                },
                {
                    "kind": "function",
                    "function": "UNIQUE_COUNT",
                    "inputs": [{"kind": "column", "column": "name"}],
                    "alias": "distinct_name",
                },
            ]
        }
    )
    assert scalar["data"] == [[3, 2]]


def test_mutations_are_transactional(frame):
    original = frame.data.clone()
    with pytest.raises(Exception):
        frame.cast("name", "INT")
    assert_frame_equal(frame.data, original)

    frame.rename("value", "score")
    frame.duplicate("score", "score_copy")
    frame.string_transform(["name"], "upper")
    assert frame.data.columns == ["name", "score", "amount", "when", "score_copy"]
    assert frame.data["name"].to_list() == ["ALPHA", "BETA", "ALPHA", None]


def test_ipc_round_trip(tmp_path, frame):
    path = tmp_path / "frame.arrow"
    frame.write_ipc(str(path))
    restored = SemossPolarsFrame.from_ipc(str(path))
    assert_frame_equal(restored.data, frame.data)


def test_sort_uses_physical_column_before_alias_projection(frame):
    result = frame.query(
        {
            "selectors": [
                {"kind": "column", "column": "name", "alias": "label"},
                {"kind": "column", "column": "value", "alias": "score"},
            ],
            "sort": [{"column": "value", "descending": True}],
            "distinct": False,
            "limit": 2,
        }
    )
    assert result["columns"] == ["label", "score"]
    assert result["data"] == [[None, 8], ["alpha", 5]]


def test_iterator_import_union_and_clean_columns():
    left = SemossPolarsFrame.from_rows(
        ["bad name", "value"],
        [["a", 1], ["b", 2]],
        {"bad name": "STRING", "value": "INT"},
    )
    left.clean_columns(["bad_name", "value"])
    right = SemossPolarsFrame.from_rows(
        ["bad_name", "value"],
        [["b", 2], ["c", 3]],
        {"bad_name": "STRING", "value": "INT"},
    )
    left.union(right, distinct=True)
    assert left.data.rows() == [("a", 1), ("b", 2), ("c", 3)]


def test_iterator_import_parses_temporal_schema():
    imported = SemossPolarsFrame.from_rows(
        ["day", "moment"],
        [["2026-10-09", "2026-10-09T08:51:46.647Z"]],
        {"day": "DATE", "moment": "TIMESTAMP"},
    )
    assert imported.schema()["types"] == ["DATE", "TIMESTAMP"]
    assert imported.data["day"][0] == dt.date(2026, 10, 9)
    assert imported.data["moment"][0].utcoffset() == dt.timedelta(0)

    with pytest.raises(ValueError, match="cannot mix"):
        SemossPolarsFrame.from_rows(
            ["moment"],
            [["2026-10-09T08:51:46"], ["2026-10-09T08:51:46Z"]],
            {"moment": "TIMESTAMP"},
        )


def test_lazy_and_nested_types_are_rejected():
    with pytest.raises(TypeError, match="collect"):
        SemossPolarsFrame(pl.DataFrame({"x": [1]}).lazy())
    with pytest.raises(TypeError, match="Unsupported Polars frame schema"):
        SemossPolarsFrame(pl.DataFrame({"x": [[1, 2]]}))
