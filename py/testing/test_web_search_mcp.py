"""Check the web-search platform MCP: SearXNG results, page reading, and the private-address guard."""
import importlib.util
import ipaddress
import json
from pathlib import Path
import socket
import unittest
from unittest.mock import patch

import httpx

DRIVER = Path(__file__).resolve().parents[2] / "project/platform__web-search/app_root/version/assets/py/mcp_driver.py"
_spec = importlib.util.spec_from_file_location("web_search_mcp_driver", DRIVER)
driver = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(driver)

REAL_CLIENT = httpx.Client


def fake_dns(host, port, *args, **kwargs):
    """IP literals resolve to themselves, *.internal.test to a private address, anything else to a public one."""
    try:
        ip = str(ipaddress.ip_address(host))
    except ValueError:
        ip = "10.0.0.5" if host.endswith(".internal.test") else "93.184.215.14"
    family = socket.AF_INET6 if ":" in ip else socket.AF_INET
    return [(family, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, port))]


def searxng_reply(status=200, payload=None):
    # raise_for_status needs the request attached
    request = httpx.Request("GET", "http://searxng.test/search")
    return httpx.Response(status, json=payload if payload is not None else {}, request=request)


class WebSearchTest(unittest.TestCase):
    def test_returns_the_requested_number_of_results(self):
        payload = {
            "results": [
                {"title": "T%d" % i, "url": "https://e.com/%d" % i, "content": "snippet", "engines": ["bing"], "score": 1.0}
                for i in range(12)
            ]
        }
        with patch.object(driver.httpx, "get", return_value=searxng_reply(payload=payload)) as get:
            out = json.loads(driver.web_search("semoss", 3, "month"))

        self.assertTrue(out["success"])
        self.assertEqual(out["results"][0], {"title": "T0", "url": "https://e.com/0", "snippet": "snippet"})
        self.assertEqual(len(out["results"]), 3)
        self.assertTrue(get.call_args.args[0].endswith("/search"))
        self.assertEqual(get.call_args.kwargs["params"], {"q": "semoss", "format": "json", "time_range": "month"})

    def test_arguments_the_model_left_out_use_defaults(self):
        payload = {"results": [{"title": "T", "url": "https://e.com/%d" % i} for i in range(30)]}
        with patch.object(driver.httpx, "get", return_value=searxng_reply(payload=payload)) as get:
            out = json.loads(driver.web_search("semoss", None, None))

        self.assertEqual(len(out["results"]), 8)
        self.assertEqual(get.call_args.kwargs["params"], {"q": "semoss", "format": "json"})

    def test_empty_results_name_the_engines_that_failed(self):
        payload = {"results": [], "unresponsive_engines": [["google", "CAPTCHA"], ["bing", "timeout"]]}
        with patch.object(driver.httpx, "get", return_value=searxng_reply(payload=payload)):
            out = json.loads(driver.web_search("semoss"))

        self.assertEqual(out["results"], [])
        self.assertEqual(out["unresponsive_engines"], ["google: CAPTCHA", "bing: timeout"])

    def test_setup_problems_explain_the_fix(self):
        cases = {403: "search.formats", 429: "server.limiter"}
        for status, hint in cases.items():
            with self.subTest(status=status), patch.object(driver.httpx, "get", return_value=searxng_reply(status)):
                out = json.loads(driver.web_search("semoss"))
            self.assertFalse(out["success"])
            self.assertIn(hint, out["error"])

        with patch.object(driver.httpx, "get", side_effect=httpx.ConnectError("refused")):
            out = json.loads(driver.web_search("semoss"))
        self.assertIn("SEARXNG_URL", out["error"])

    def test_bad_arguments_are_refused(self):
        for args in (("  ",), ("semoss", 5, "week")):
            with self.subTest(args=args):
                self.assertFalse(json.loads(driver.web_search(*args))["success"])


class WebFetchTest(unittest.TestCase):
    def setUp(self):
        dns = patch.object(driver.socket, "getaddrinfo", side_effect=fake_dns)
        dns.start()
        self.addCleanup(dns.stop)

    def fetch(self, handler, url, *args):
        def client(**kwargs):
            return REAL_CLIENT(transport=httpx.MockTransport(handler), **kwargs)

        with patch.object(driver.httpx, "Client", client):
            return json.loads(driver.web_fetch(url, *args))

    def test_refuses_internal_and_non_http_addresses(self):
        def never(request):
            raise AssertionError("fetched %s" % request.url)

        # the tool turns any exception into success False, so check the guard's own reason
        cases = {
            "http://169.254.169.254/computeMetadata/v1/": "private or internal",
            "http://127.0.0.1:8080/": "private or internal",
            "http://[::ffff:127.0.0.1]/": "private or internal",
            "http://[::1]/": "private or internal",
            "http://db.internal.test/": "private or internal",
            "file:///etc/passwd": "Only full http and https",
            "localhost": "Only full http and https",
        }
        for url, reason in cases.items():
            with self.subTest(url=url):
                out = self.fetch(never, url)
                self.assertFalse(out["success"])
                self.assertIn(reason, out["error"])

    def test_refuses_a_redirect_into_the_private_network(self):
        def handler(request):
            if request.url.host == "example.com":
                return httpx.Response(302, headers={"location": "http://admin.internal.test/secrets"})
            raise AssertionError("followed the redirect to %s" % request.url)

        out = self.fetch(handler, "https://example.com/start")
        self.assertFalse(out["success"])
        self.assertIn("private or internal", out["error"])

    def test_follows_public_redirects(self):
        def handler(request):
            if request.url.path == "/old":
                return httpx.Response(301, headers={"location": "/new"})
            return httpx.Response(200, headers={"content-type": "text/plain"}, content=b"moved here")

        out = self.fetch(handler, "https://example.com/old")
        self.assertEqual(out["url"], "https://example.com/new")
        self.assertEqual(out["content"], "moved here")

    def test_html_becomes_markdown_of_the_main_content_in_parts(self):
        html = (
            "<html><head><title>Report</title><script>var x = 1;</script></head><body>"
            "<nav><a href='/home'>Home</a></nav>"
            "<main><h1>Findings</h1><p>See <a href='/full.pdf'>the full report</a>.</p>"
            "<p>" + "word " * 2000 + "</p></main>"
            "<footer>Copyright</footer></body></html>"
        )

        def handler(request):
            return httpx.Response(200, headers={"content-type": "text/html; charset=utf-8"}, content=html.encode())

        first = self.fetch(handler, "https://example.com/report", 0, 1000)
        self.assertTrue(first["success"])
        self.assertEqual(first["title"], "Report")
        self.assertTrue(first["content"].startswith("# Findings"))
        self.assertIn("[the full report](https://example.com/full.pdf)", first["content"])
        for noise in ("Home", "Copyright", "var x"):
            self.assertNotIn(noise, first["content"])
        self.assertEqual(len(first["content"]), 1000)
        self.assertEqual(first["next_offset"], 1000)

        rest = self.fetch(handler, "https://example.com/report", first["next_offset"], 50000)
        self.assertNotIn("next_offset", rest)
        self.assertEqual(rest["offset"] + len(rest["content"]), first["total_chars"])

    def test_refuses_files_it_cannot_read(self):
        def handler(request):
            return httpx.Response(200, headers={"content-type": "image/png"}, content=b"\x89PNG")

        out = self.fetch(handler, "https://example.com/logo.png")
        self.assertFalse(out["success"])
        self.assertIn("image/png", out["error"])


if __name__ == "__main__":
    unittest.main()
