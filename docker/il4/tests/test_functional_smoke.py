import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import urllib.error

from functional_smoke import Client, main, pixel_outputs


def document(output=2, operation="OPERATION"):
    return {"pixelReturn": [{"output": output, "operationType": [operation]}]}


class FunctionalSmokeTests(unittest.TestCase):
    def client(self):
        with patch("functional_smoke.ssl.create_default_context"), \
                patch("functional_smoke.urllib.request.build_opener"):
            return Client("https://localhost:8443/Monolith", "/fake/ca.pem")

    def test_rejects_nonlocal_and_malformed_urls(self):
        for url in ("http://localhost:8443/Monolith", "https://example.com",
                    "https://localhost:8443@example.com/Monolith", "https://localhost:8443/#fragment"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                Client(url, "/fake/ca.pem")

    def test_pixel_outputs_rejects_nested_errors_and_malformed_results(self):
        self.assertEqual(pixel_outputs(document()), [2])
        for value in ({}, document(operation="ERROR"), {"pixelReturn": [{}]},
                      document([{"operationType": ["ERROR"], "output": "python failed"}], "CODE_EXECUTION")):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                pixel_outputs(value)

    def test_requests_encode_forms_keep_cookies_and_update_csrf(self):
        client = self.client()
        response = client.opener.open.return_value.__enter__.return_value
        response.headers.get.return_value = "test-csrf"
        response.read.return_value = b'{"ok": true}'
        self.assertEqual(client.request("/test", {"a": "b c"}), {"ok": True})
        self.assertEqual(client.csrf, "test-csrf")
        request = client.opener.open.call_args.args[0]
        self.assertEqual(request.data, b"a=b+c")
        response.read.return_value = b""
        self.assertIsNone(client.request("/test"))

    def test_http_errors_are_explicit(self):
        for body in (b'{"errorMessage":"denied"}', b"<html>Error</html>", b"[]"):
            client = self.client()
            client.opener.open.side_effect = urllib.error.HTTPError(
                "https://localhost:8443", 403, "Forbidden", {}, io.BytesIO(body))
            with self.assertRaisesRegex(RuntimeError, "HTTP 403"):
                client.request("/test")

    def test_login_refreshes_csrf_and_rejects_failure(self):
        client = self.client()
        client.fetch_csrf = Mock()
        client.request = Mock(return_value={"success": "true"})
        client.login({"username": "test", "password": "test"})
        self.assertEqual(client.fetch_csrf.call_count, 2)
        client.request.return_value = {"success": False}
        with self.assertRaisesRegex(RuntimeError, "login"):
            client.login({"username": "test", "password": "test"})

    def test_missing_csrf_and_pixel_insight_reuse(self):
        client = self.client()
        client.request = Mock(return_value=document())
        with self.assertRaisesRegex(RuntimeError, "CSRF"):
            client.fetch_csrf()
        client.csrf = "token"
        client.fetch_csrf()
        self.assertEqual(client.pixel("1+1;", "insight"), document())
        self.assertEqual(client.request.call_args.args[1]["insightId"], "insight")
        client.pixel("1+1;")

    def test_main_with_existing_credentials_and_bootstrap_failures(self):
        credentials = Mock()
        credentials.exists.return_value = True
        credentials.read_text.return_value = '{"username":"test","password":"test"}'
        args = SimpleNamespace(ca="/fake/ca", credentials=credentials, bootstrap=False,
                               expression="1+1;", url="https://localhost:8443/Monolith")
        with patch("functional_smoke.argparse.ArgumentParser.parse_args", return_value=args), \
                patch("functional_smoke.Client") as factory, patch("builtins.print"):
            client = factory.return_value
            client.pixel.return_value = document()
            main()
            client.login.assert_called_once()
            args.bootstrap = True
            client.request.side_effect = [{"success": True}, {"success": "true"}]
            main()
            for results in ([{"success": False}], [{"success": True}, {"success": False}]):
                client.request.side_effect = results
                with self.assertRaises(RuntimeError):
                    main()
            credentials.exists.return_value = False
            args.bootstrap = False
            with patch("sys.stderr", new=io.StringIO()), self.assertRaises(SystemExit):
                main()
            args.bootstrap = True
            client.request.side_effect = [{"success": True}, {"success": True}]
            with patch("functional_smoke.os.open", return_value=42) as open_file, \
                    patch("functional_smoke.os.fdopen"), \
                    patch("functional_smoke.secrets.token_urlsafe", return_value="random-test-only"):
                main()
                self.assertEqual(open_file.call_args.args[2], 0o600)
