"""Check where smss_secrets finds an app's secrets: the jail's own file first, then OpenShell's."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import smss_secrets  # noqa: E402

PROJECT = "1234abcd-0000-4000-8000-000000000001"
OTHER = "9876fedc-0000-4000-8000-000000000002"


class SecretTests(unittest.TestCase):

    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        root = Path(self.folder.name)
        self.jail = root / "semosshome" / "sandbox-secrets" / "secrets.json"
        self.openshell = root / "insight" / "sandbox-secrets" / "placeholders.json"
        self.saved = (smss_secrets._JAIL_SECRETS, smss_secrets._PLACEHOLDERS)
        smss_secrets._JAIL_SECRETS = str(self.jail)
        smss_secrets._PLACEHOLDERS = str(self.openshell)

    def tearDown(self):
        smss_secrets._JAIL_SECRETS, smss_secrets._PLACEHOLDERS = self.saved
        self.folder.cleanup()

    @staticmethod
    def write(path, content):
        os.makedirs(path.parent, exist_ok=True)
        path.write_text(json.dumps(content))

    def test_the_jail_file_comes_before_the_openshell_file(self):
        self.write(self.openshell, {"1234abcd": {"DCTM_AUTH": "planted"}})
        self.write(self.jail, {"1234abcd": {"DCTM_AUTH": "Basic c3ZjOnB3"}})

        self.assertEqual("Basic c3ZjOnB3", smss_secrets.secret("DCTM_AUTH"))

    def test_openshell_placeholders_are_read_when_there_is_no_jail_file(self):
        self.write(self.openshell, {"1234abcd": {"DCTM_AUTH": "openshell:resolve:env:v1_x"}})

        self.assertEqual("openshell:resolve:env:v1_x", smss_secrets.secret("DCTM_AUTH"))

    def test_no_file_says_to_declare_the_secret(self):
        with self.assertRaisesRegex(KeyError, "no sandbox secrets are available"):
            smss_secrets.secret("DCTM_AUTH")

    def test_a_name_two_projects_declare_needs_the_project(self):
        self.write(self.jail, {"1234abcd": {"KEY": "a"}, "9876fedc": {"KEY": "b"}})

        with self.assertRaisesRegex(KeyError, "several loaded projects"):
            smss_secrets.secret("KEY")
        self.assertEqual("b", smss_secrets.secret("KEY", project=OTHER))

    def test_the_jail_file_is_under_the_semoss_home_this_module_is_in(self):
        py_folder = Path(smss_secrets.__file__).resolve().parent
        self.assertEqual(str(py_folder.parent / "sandbox-secrets" / "secrets.json"),
                         os.path.realpath(self.saved[0]))


if __name__ == "__main__":
    unittest.main()
