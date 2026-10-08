"""Focused regression tests for SEMOSS Automation Python runtime helpers."""

import base64
import json
import unittest

from semoss_automation_runtime import execute_node, extract_data_element


def _encode(value: object) -> str:
    serialized = value if isinstance(value, str) else json.dumps(value)
    return base64.urlsafe_b64encode(serialized.encode()).decode()


class ExtractDataElementTests(unittest.TestCase):
    def test_reads_dot_and_array_path(self) -> None:
        source = {"orders": [{"id": 17}]}

        self.assertEqual(17, extract_data_element(source, "orders[0].id"))
        self.assertEqual(17, extract_data_element(source, "$.orders[0].id"))

    def test_reads_quoted_key_from_root(self) -> None:
        source = {"a.b": {"value": "ok"}}

        self.assertEqual("ok", extract_data_element(source, '$["a.b"].value'))

    def test_reads_json_pointer(self) -> None:
        source = {"orders": [{"id": 17}]}

        self.assertEqual(17, extract_data_element(source, "/orders/0/id"))

    def test_distinguishes_missing_and_null(self) -> None:
        source = {"present": None}

        self.assertEqual(
            "missing",
            extract_data_element(source, "absent", missing_value="missing"),
        )
        self.assertEqual(
            "null",
            extract_data_element(source, "present", null_value="null"),
        )

    def test_reads_base64_json_and_xml(self) -> None:
        encoded = base64.b64encode(json.dumps({"value": 2}).encode()).decode()

        self.assertEqual(2, extract_data_element(encoded, "value"))
        self.assertEqual(
            "ok",
            extract_data_element(
                '<root><item id="1">ok</item></root>',
                "root.item.#text",
            ),
        )

    def test_reads_run_workspace_asset(self) -> None:
        self.assertEqual(
            "ready",
            extract_data_element(
                "/orders/example.json",
                "status",
                asset_reader=lambda path: json.dumps(
                    {"path": path, "status": "ready"}
                ),
            ),
        )

    def test_rejects_invalid_root_marker(self) -> None:
        with self.assertRaisesRegex(ValueError, "root marker"):
            extract_data_element({"value": 1}, "$value")


class DataFrameOutputTests(unittest.TestCase):
    def test_retains_dataframe_and_rebinds_it_for_the_next_node(self) -> None:
        session_globals: dict[str, object] = {}
        result = execute_node(
            _encode({}),
            _encode(
                "import pandas as pd\n\n"
                "def run(scope):\n"
                "    return pd.DataFrame([{'value': 7}, {'value': 9}])\n"
            ),
            1024,
            "rows",
            session_globals,
            {},
        )

        self.assertEqual(
            {"__automation_frame__": {"rowCount": 2, "columnCount": 1}}, result
        )
        self.assertEqual([7, 9], session_globals["rows"]["value"].tolist())

        downstream = execute_node(
            _encode({"rows": {"dataType": "table", "rowCount": 2}}),
            _encode(
                "def run(scope):\n"
                "    return {'rows': len(scope['rows']), "
                "'first': int(scope['rows'].iloc[0]['value'])}\n"
            ),
            1024,
            "metrics",
            session_globals,
            {"rows": "PY"},
        )

        self.assertEqual({"rows": 2, "first": 7}, downstream)

    def test_retains_empty_dataframe_schema(self) -> None:
        session_globals: dict[str, object] = {}
        result = execute_node(
            _encode({}),
            _encode(
                "import pandas as pd\n\n"
                "def run(scope):\n"
                "    return pd.DataFrame(columns=['id', 'name'])\n"
            ),
            1024,
            "rows",
            session_globals,
            {},
        )

        self.assertEqual(
            {"__automation_frame__": {"rowCount": 0, "columnCount": 2}}, result
        )
        self.assertEqual(["id", "name"], list(session_globals["rows"].columns))

    def test_reports_expired_frame_backing_explicitly(self) -> None:
        with self.assertRaisesRegex(ValueError, "no longer available"):
            execute_node(
                _encode({"rows": {"dataType": "table"}}),
                _encode("def run(scope):\n    return len(scope['rows'])\n"),
                1024,
                "count",
                {},
                {"rows": "PY"},
            )

    def test_json_row_list_keeps_its_existing_scope_shape(self) -> None:
        session_globals: dict[str, object] = {}
        rows = execute_node(
            _encode({}),
            _encode("def run(scope):\n    return [{'value': 7}]\n"),
            1024,
            "rows",
            session_globals,
            {},
        )

        self.assertEqual([{"value": 7}], rows)
        downstream = execute_node(
            _encode({"rows": rows}),
            _encode(
                "def run(scope):\n"
                "    return {'is_list': isinstance(scope['rows'], list)}\n"
            ),
            1024,
            "metrics",
            session_globals,
            {},
        )
        self.assertEqual({"is_list": True}, downstream)


if __name__ == "__main__":
    unittest.main()
