import contextlib
import io
import json
import subprocess
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import prepare_dev_tls as tls


class DevelopmentTlsTests(unittest.TestCase):
    def test_opt_in_and_safe_names_are_required_before_docker(self):
        for extra in ([], ["--allow-self-signed", "--hostname", "*.example.test"],
                      ["--allow-self-signed", "--volume", "../existing"],
                      ["--allow-self-signed", "--image=-privileged"]):
            with patch("prepare_dev_tls.docker") as docker, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    tls.main(["--image", "semoss:test", "--volume", "fixture",
                              "--format", "BCFKS", *extra])
            docker.assert_not_called()

    def test_names_and_formats(self):
        self.assertEqual(tls.hostname("semoss.example.test"), "semoss.example.test")
        for value in ("", "-bad", "a..b", "example.test,ip:127.0.0.1", "a\nb", "a" * 64):
            with self.subTest(value=value), self.assertRaises(ValueError):
                tls.hostname(value)
        with self.assertRaises(ValueError):
            tls.seed(Mock(), "JKS", "localhost")

    def test_seed_has_private_unique_keys_and_no_password_arguments(self):
        for kind, suffix in (("BCFKS", "bcfks"), ("PKCS12", "p12")):
            root = MagicMock(spec=Path)
            root.iterdir.return_value = iter([])
            files = {}

            def child(name):
                if name not in files:
                    files[name] = MagicMock(spec=Path)
                    files[name].__str__.return_value = "/tls/" + name
                return files[name]

            root.__truediv__.side_effect = child
            child("readiness-ca.pem").read_text.return_value = "PUBLIC CERTIFICATE"
            with patch("prepare_dev_tls.secrets.token_hex", return_value="secret-value") as random, \
                 patch("prepare_dev_tls.subprocess.run") as run, \
                 patch("prepare_dev_tls.os.chown") as chown:
                report = tls.seed(root, kind, "semoss.example.test")
            random.assert_called_once_with(48)
            calls = [c.args[0] for c in run.call_args_list]
            self.assertEqual(len(calls), 2)
            self.assertTrue(all("secret-value" not in " ".join(command) for command in calls))
            self.assertIn("-storepass:file", calls[0])
            self.assertIn("7", calls[0])
            self.assertIn("SAN=dns:localhost,dns:semoss.example.test", calls[0])
            self.assertIn("BC=ca:false", calls[0])
            self.assertEqual("-providerclass" in calls[0], kind == "BCFKS")
            child("server.password").chmod.assert_called_once_with(0o600)
            child("server." + suffix).chmod.assert_called_once_with(0o600)
            child("readiness-ca.pem").chmod.assert_called_once_with(0o644)
            self.assertEqual(chown.call_count, 2)
            self.assertEqual(report["certificate_pem"], "PUBLIC CERTIFICATE")
            self.assertNotIn("secret-value", json.dumps(report))

    def test_seed_refuses_existing_material_and_sanitizes_keytool_errors(self):
        root = MagicMock(spec=Path)
        root.iterdir.return_value = iter([Path("existing")])
        with self.assertRaisesRegex(RuntimeError, "empty"):
            tls.seed(root, "PKCS12", "localhost")
        root.iterdir.return_value = iter([])
        with patch("prepare_dev_tls.subprocess.run", side_effect=subprocess.CalledProcessError(1, "keytool")):
            with self.assertRaisesRegex(RuntimeError, "Keytool"):
                tls.seed(root, "PKCS12", "localhost")

    @contextlib.contextmanager
    def fixture(self, existing=False, failure=None):
        calls = []

        def docker(*args, **kwargs):
            calls.append((args, kwargs))
            if failure and failure(args):
                raise RuntimeError("synthetic failure")
            if args[:2] == ("volume", "ls"):
                return "fixture\n" if existing else ""
            if "inspect" in args and "Labels" in " ".join(args):
                return "run-id"
            if args[:2] == ("container", "inspect"):
                return "0"
            if args[0] == "start":
                return json.dumps({"format": "PKCS12", "hostname": "localhost",
                                   "validity_days": 7, "certificate_pem": "PUBLIC"})
            return ""

        with patch("prepare_dev_tls.docker", side_effect=docker), \
             patch("prepare_dev_tls.uuid.uuid4", return_value=Mock(hex="run-id")), \
             patch("prepare_dev_tls.Path.read_text", return_value="# worker"):
            yield calls

    def test_prepare_persists_only_the_owned_tls_volume(self):
        with self.fixture() as calls:
            report = tls.prepare("semoss:test", "fixture", "PKCS12", "localhost")
        self.assertEqual(report["volume"], "fixture")
        create = next(args for args, _ in calls if args[0] == "create")
        for required in ("--pull=never", "--read-only", "--cap-drop", "ALL", "CHOWN", "none"):
            self.assertIn(required, create)
        self.assertIn("--worker", create)
        self.assertTrue(any(args[:2] == ("container", "rm") for args, _ in calls))
        self.assertFalse(any(args[:2] == ("volume", "rm") for args, _ in calls))

    def test_existing_volume_is_never_modified(self):
        with self.fixture(existing=True) as calls:
            with self.assertRaisesRegex(RuntimeError, "already exists"):
                tls.prepare("semoss:test", "fixture", "PKCS12", "localhost")
        self.assertFalse(any(args[0] in ("create", "start") or args[:2] == ("volume", "create")
                             for args, _ in calls))

    def test_failure_cleans_only_owned_new_resources(self):
        with self.fixture(failure=lambda args: args[0] == "start") as calls:
            with self.assertRaisesRegex(RuntimeError, "synthetic failure"):
                tls.prepare("semoss:test", "fixture", "PKCS12", "localhost")
        self.assertTrue(any(args[:2] == ("container", "rm") for args, _ in calls))
        self.assertTrue(any(args[:2] == ("volume", "rm") for args, _ in calls))
        with patch("prepare_dev_tls.docker", return_value="someone-else") as docker:
            with self.assertRaisesRegex(RuntimeError, "ownership"):
                tls.remove_owned("volume", "fixture", "run-id")
        self.assertEqual(docker.call_count, 1)

    def test_docker_errors_do_not_expose_command_output(self):
        with patch("prepare_dev_tls.subprocess.run", return_value=Mock(returncode=1, stderr="private")):
            with self.assertRaisesRegex(RuntimeError, "^Docker volume failed$"):
                tls.docker("volume", "ls")
        with patch("prepare_dev_tls.subprocess.run", side_effect=OSError("private")):
            with self.assertRaisesRegex(RuntimeError, "unavailable or timed out"):
                tls.docker("version")

    def test_main_returns_public_report_or_explicit_failure(self):
        args = ["--image", "semoss:test", "--volume", "fixture", "--format", "PKCS12",
                "--allow-self-signed"]
        with patch("prepare_dev_tls.prepare", return_value={"volume": "fixture"}), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(tls.main(args), 0)
        self.assertEqual(json.loads(output.getvalue())["volume"], "fixture")
        with patch("prepare_dev_tls.prepare", side_effect=RuntimeError("fixture failure")), \
             contextlib.redirect_stderr(io.StringIO()) as error:
            self.assertEqual(tls.main(args), 1)
        self.assertIn("fixture failure", error.getvalue())
