"""Hermetic tests for the host-mounted, opt-in readiness helper."""

from contextlib import ExitStack
import io
import socket
import ssl
import unittest
from unittest.mock import patch
import http.client


import readiness


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.context = self.stack.enter_context(
            patch.object(readiness.ssl, "create_default_context")).return_value
        self.factory = self.stack.enter_context(
            patch.object(readiness.http.client, "HTTPSConnection"))
        self.connection = self.factory.return_value
        self.response = self.connection.getresponse.return_value
        self.response.status = 200
        self.response.read.return_value = b'{"startupComplete":true,"status":"READY"}'
        self.timer = self.stack.enter_context(patch.object(readiness.signal, "setitimer"))
        self.handler = self.stack.enter_context(patch.object(readiness.signal, "signal"))
        self.stack.enter_context(patch.object(readiness.signal, "getsignal", return_value=0))
        self.stderr = self.stack.enter_context(patch.object(readiness.sys, "stderr", io.StringIO()))
        self.stdout = self.stack.enter_context(patch.object(readiness.sys, "stdout", io.StringIO()))
        self.args = ["--url", "https://localhost:8443/Monolith/health/ready",
                     "--ca", "/fake/ca.pem"]

    def run_probe(self, *extra):
        return readiness.main(self.args + list(extra))

    def test_success_verifies_tls_and_sends_no_credentials(self):
        self.assertEqual(self.run_probe(), 0)
        readiness.ssl.create_default_context.assert_called_once_with(cafile="/fake/ca.pem")
        self.assertTrue(self.context.check_hostname)
        self.assertEqual(self.context.verify_mode, ssl.CERT_REQUIRED)
        self.factory.assert_called_once_with("localhost", 8443, context=self.context, timeout=5.0)
        self.connection.request.assert_called_once_with(
            "GET", "/Monolith/health/ready", headers={"Accept": "application/json"})
        self.response.read.assert_called_once_with(readiness.MAX_RESPONSE_BYTES + 1)
        self.connection.close.assert_called_once()
        self.timer.assert_any_call(readiness.signal.ITIMER_REAL, 5.0)
        self.timer.assert_any_call(readiness.signal.ITIMER_REAL, 0)

    def test_true_must_be_exact_boolean(self):
        for value in ("false", "null", "1", '"true"', "{}", "[]"):
            with self.subTest(value=value):
                self.response.read.return_value = ('{"startupComplete":' + value + "}").encode()
                self.assertEqual(self.run_probe(), 1)

    def test_status_is_not_required(self):
        self.response.read.return_value = b'{"startupComplete":true}'
        self.assertEqual(self.run_probe(), 0)

    def test_missing_key_or_wrong_document_shape(self):
        for body in (b"{}", b"[]", b"true", b"null", b'"ready"'):
            with self.subTest(body=body):
                self.response.read.return_value = body
                self.assertEqual(self.run_probe(), 1)

    def test_malformed_json_is_rejected_without_body_logging(self):
        for body in (b"", b"private-secret", b"\xff", b'{"startupComplete":true} trailing',
                     b'{"startupComplete":true,"startupComplete":false}',
                     b'{"startupComplete":true,"extra":NaN}', b"[" * 2000):
            with self.subTest(body=body[:80]):
                self.response.read.return_value = body
                self.assertEqual(self.run_probe(), 1)
                self.assertNotIn("private-secret", self.stderr.getvalue())

    def test_response_size_limit(self):
        self.response.read.return_value = b" " * (readiness.MAX_RESPONSE_BYTES + 1)
        self.assertEqual(self.run_probe(), 1)
        self.assertIn("too large", self.stderr.getvalue())

    def test_http_failures_and_redirects_never_followed(self):
        for status in (204, 301, 302, 307, 401, 403, 500, 503):
            with self.subTest(status=status):
                self.response.status = status
                self.assertEqual(self.run_probe(), 1)
        self.response.read.assert_not_called()
        self.assertEqual(self.connection.request.call_count, 8)

    def test_network_and_tls_errors_are_concise(self):
        for error in (ssl.SSLCertVerificationError("private-secret"),
                      ssl.SSLError("private-secret"), socket.gaierror("private-secret"),
                      ConnectionRefusedError("private-secret"), TimeoutError("private-secret"),
                      http.client.BadStatusLine("private-secret")):
            with self.subTest(error=type(error).__name__):
                self.connection.getresponse.side_effect = error
                self.assertEqual(self.run_probe(), 1)
                self.assertNotIn("private-secret", self.stderr.getvalue())
                self.assertNotIn("Traceback", self.stderr.getvalue())
        self.assertEqual(self.connection.close.call_count, 6)

    def test_read_error_closes_connection(self):
        self.response.read.side_effect = OSError("private-secret")
        self.assertEqual(self.run_probe(), 1)
        self.connection.close.assert_called_once()

    def test_deadline_covers_stalled_request(self):
        def expire(*args, **kwargs):
            callback = self.handler.call_args_list[0].args[1]
            callback(readiness.signal.SIGALRM, None)
        self.connection.request.side_effect = expire
        self.assertEqual(self.run_probe(), 1)
        self.assertIn("timed out", self.stderr.getvalue())
        self.connection.close.assert_called_once()
        self.timer.assert_any_call(readiness.signal.ITIMER_REAL, 0)

    def test_invalid_urls_rejected_before_network(self):
        for url in ("http://localhost/Monolith/health/ready", "https:///Monolith/health/ready",
                    "https://user:private-secret@localhost/Monolith/health/ready",
                    "https://localhost:0/Monolith/health/ready",
                    "https://localhost:65536/Monolith/health/ready",
                    "https://localhost:no/Monolith/health/ready",
                    "https://localhost:/Monolith/health/ready",
                    "https://[invalid/Monolith/health/ready",
                    "https://localhost/SemossWeb/", "https://localhost/Monolith/health/ready?q=a",
                    "https://localhost/Monolith/health/ready#fragment",
                    " https://localhost/Monolith/health/ready",
                    "https://local\nhost/Monolith/health/ready",
                    "https://local%20host/Monolith/health/ready",
                    "https://localhost\\evil/Monolith/health/ready",
                    "https://local..host/Monolith/health/ready",
                    "https://-local/Monolith/health/ready",
                    "https://" + "a" * 64 + "/Monolith/health/ready",
                    "https://" + "a." * 130 + "a/Monolith/health/ready"):
            with self.subTest(url=url):
                self.args[1] = url
                self.assertEqual(self.run_probe(), 2)
        self.factory.assert_not_called()
        self.assertNotIn("private-secret", self.stderr.getvalue())

    def test_default_https_port(self):
        self.args[1] = "https://localhost/Monolith/health/ready"
        self.assertEqual(self.run_probe(), 0)
        self.assertEqual(self.factory.call_args.args, ("localhost", 443))

    def test_ipv6_endpoint(self):
        self.args[1] = "https://[::1]:8443/Monolith/health/ready"
        self.assertEqual(self.run_probe(), 0)
        self.assertEqual(self.factory.call_args.args, ("::1", 8443))

    def test_ca_must_be_explicit_absolute_file(self):
        for ca in ("", "relative.pem", "/fake/\x00ca.pem"):
            with self.subTest(ca=ca):
                self.args[-1] = ca
                self.assertEqual(self.run_probe(), 2)
        self.factory.assert_not_called()

    def test_missing_unreadable_or_invalid_ca(self):
        for error in (FileNotFoundError("private-secret"), PermissionError("private-secret"),
                      IsADirectoryError("private-secret"), ssl.SSLError("private-secret"),
                      ValueError("private-secret")):
            with self.subTest(error=type(error).__name__):
                readiness.ssl.create_default_context.side_effect = error
                self.assertEqual(self.run_probe(), 2)
        self.factory.assert_not_called()
        self.assertNotIn("private-secret", self.stderr.getvalue())

    def test_cli_errors_do_not_echo_supplied_values(self):
        for args in ([], ["--url"], self.args + ["--password", "private-secret"],
                     self.args + ["--timeout", "private-secret"]):
            with self.subTest(args=args):
                self.assertEqual(readiness.main(args), 2)
        self.assertNotIn("private-secret", self.stderr.getvalue())
        self.factory.assert_not_called()

    def test_timeout_is_finite_positive_and_capped(self):
        for timeout in ("0", "-1", "nan", "inf", "11"):
            with self.subTest(timeout=timeout):
                self.assertEqual(self.run_probe("--timeout", timeout), 2)
        self.assertEqual(self.run_probe("--timeout", "0.5"), 0)
        self.assertEqual(self.factory.call_args.kwargs["timeout"], 0.5)

    def test_help_is_success_without_network(self):
        self.assertEqual(readiness.main(["--help"]), 0)
        self.factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
