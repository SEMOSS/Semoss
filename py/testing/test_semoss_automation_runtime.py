import base64
import json
import unittest

from semoss_automation_runtime import AutomationScope, execute_node


class AutomationScopeTest(unittest.TestCase):
    def setUp(self):
        self.scope = AutomationScope(
            {
                "selected_files": {
                    "test_pdf": "reports/test.pdf",
                    "candidates": [{"path": "reports/first.pdf"}],
                }
            }
        )

    def test_resolve_preserves_native_nested_values(self):
        self.assertEqual(
            "reports/test.pdf",
            self.scope.resolve("${selected_files.test_pdf}"),
        )
        self.assertEqual(
            "reports/first.pdf",
            self.scope.resolve("${selected_files.candidates[0].path}"),
        )

    def test_resolve_rejects_python_style_string_indexes(self):
        with self.assertRaisesRegex(KeyError, r"selected_files\['test_pdf'\]"):
            self.scope.resolve("${selected_files['test_pdf']}")

    def test_resolve_rejects_unavailable_nested_paths(self):
        for reference in (
            "${selected_files.missing}",
            "${selected_files.candidates[1]}",
            "${selected_files.candidates.first}",
        ):
            with self.subTest(reference=reference), self.assertRaises(KeyError):
                self.scope.resolve(reference)

    def test_execute_node_exposes_semoss_workspace_root(self):
        source = "def run(scope):\n    return ROOT\n"
        encoded_scope = base64.urlsafe_b64encode(json.dumps({}).encode()).decode()
        encoded_source = base64.urlsafe_b64encode(source.encode()).decode()

        self.assertEqual(
            "/tmp/insight",
            execute_node(encoded_scope, encoded_source, 1024, "/tmp/insight"),
        )


if __name__ == "__main__":
    unittest.main()
