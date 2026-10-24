"""The Python MCP generator preserves native resource URIs and drops legacy selectors."""
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "py"))
import smssutil


class McpResourceUriTests(unittest.TestCase):
    def test_native_metadata_from_import_and_ast_fallback(self):
        source = """import smssutil
@smssutil.mcp_metadata({
    "execution": "ask",
    "resourceURI": "component://mail-compose?intent=send&provider=google",
    "component": "legacy-name",
    "displayLocation": "inline",
    "autoOpen": True,
})
def send_email(body: str) -> str:
    \"\"\"Prepare an email.\"\"\"
    return body
"""
        with tempfile.TemporaryDirectory() as directory:
            driver = Path(directory) / "driver.py"
            driver.write_text(source)
            for use_ast in (False, True):
                with self.subTest(ast_fallback=use_ast):
                    if use_ast:
                        with patch.object(smssutil, "load_module_from_file", side_effect=ImportError("unavailable dependency")):
                            generated = smssutil.generate_mcp(str(driver))
                    else:
                        generated = smssutil.generate_mcp(str(driver))
                    meta = generated["tools"][0]["_meta"]
                    self.assertEqual(meta["SMSS_MCP_EXECUTION"], "ask")
                    self.assertEqual(meta["SMSS_MCP_UI"], {
                        "resourceURI": "component://mail-compose?intent=send&provider=google",
                        "displayLocation": "inline", "autoOpen": True,
                    })


if __name__ == "__main__":
    unittest.main()
