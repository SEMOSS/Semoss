"""Explicit localhost bootstrap and authenticated HTTPS workflow checks."""
import argparse
import http.cookiejar
import json
import os
from pathlib import Path
import secrets
import ssl
import urllib.error
import urllib.parse
import urllib.request


def pixel_outputs(document):
    entries = document.get("pixelReturn")
    if not isinstance(entries, list) or not entries:
        raise RuntimeError("Missing Pixel results")
    outputs = []
    for entry in entries:
        if not isinstance(entry, dict) or "output" not in entry or not isinstance(entry.get("operationType"), list):
            raise RuntimeError("Malformed Pixel result")
        if "ERROR" in entry["operationType"]:
            raise RuntimeError("Pixel execution failed: " + str(entry["output"]))
        if "CODE_EXECUTION" in entry["operationType"]:
            pixel_outputs({"pixelReturn": entry["output"]})
        outputs.append(entry["output"])
    return outputs


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Client:
    def __init__(self, base_url, ca_file):
        parsed = urllib.parse.urlsplit(base_url)
        if (parsed.scheme != "https" or parsed.hostname != "localhost" or not parsed.port
                or parsed.username or parsed.password or parsed.query or parsed.fragment):
            raise ValueError("This test harness only permits HTTPS on localhost")
        self.base_url = base_url.rstrip("/")
        self.csrf = None
        self.cookies = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=ca_file)),
            urllib.request.HTTPCookieProcessor(self.cookies),
            NoRedirect(),
        )

    def request(self, path, data=None, headers=None):
        request_headers = dict(headers or {})
        if self.csrf:
            request_headers["X-CSRF-Token"] = self.csrf
        encoded = None if data is None else urllib.parse.urlencode(data).encode()
        request = urllib.request.Request(self.base_url + path, data=encoded, headers=request_headers)
        try:
            with self.opener.open(request, timeout=90) as response:
                token = response.headers.get("X-CSRF-Token")
                if token:
                    self.csrf = token
                body = response.read()
                return json.loads(body) if body else None
        except urllib.error.HTTPError as error:
            with error:
                body = error.read()
            try:
                detail = json.loads(body).get("errorMessage", error.reason)
            except (ValueError, AttributeError):
                detail = error.reason
            raise RuntimeError(f"{path}: HTTP {error.code}: {detail}") from error

    def fetch_csrf(self):
        self.request("/api/config/fetchCsrf", headers={"X-CSRF-Token": "Fetch"})
        if not self.csrf:
            raise RuntimeError("Server did not provide a CSRF token")

    def login(self, credentials):
        self.fetch_csrf()
        result = self.request("/api/auth/login", {
            "username": credentials["username"], "password": credentials["password"],
            "disableRedirect": "true",
        })
        if result.get("success") not in (True, "true"):
            raise RuntimeError("Native login did not succeed")
        self.csrf = None
        self.fetch_csrf()

    def pixel(self, expression, insight_id=None):
        data = {"expression": expression}
        if insight_id:
            data["insightId"] = insight_id
        result = self.request("/api/engine/runPixel", data)
        pixel_outputs(result)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ca", required=True)
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--expression", default="1 + 1;")
    parser.add_argument("--url", default="https://localhost:8443/Monolith")
    args = parser.parse_args()
    if not args.credentials.exists():
        if not args.bootstrap:
            parser.error("Credentials are missing; bootstrap explicitly or supply an existing test account")
        credentials = {
            "username": "local-admin", "password": "Aa1!" + secrets.token_urlsafe(32),
            "email": "local-admin@example.invalid", "name": "Local Test Administrator",
        }
        descriptor = os.open(args.credentials, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            json.dump(credentials, output)
    credentials = json.loads(args.credentials.read_text())
    client = Client(args.url, args.ca)
    if args.bootstrap:
        client.fetch_csrf()
        result = client.request("/adminconfig/setInitialAdmins", {
            "ids": json.dumps([credentials["username"]]),
        })
        if result.get("success") is not True:
            raise RuntimeError("Initial administrator assignment failed")
        result = client.request("/api/auth/createUser", credentials)
        if result.get("success") not in (True, "true"):
            raise RuntimeError("Native administrator creation failed")
        print("PASS: initial administrator created; disable native registration before further use")
    client.login(credentials)
    print("PASS: native authentication with CSRF")
    print(json.dumps(client.pixel(args.expression), indent=2))


if __name__ == "__main__":
    main()
