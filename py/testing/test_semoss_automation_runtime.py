"""Focused regression tests for SEMOSS Automation Python runtime helpers."""

import base64
import json
import unittest

from semoss_automation_runtime import extract_data_element


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


if __name__ == "__main__":
    unittest.main()
