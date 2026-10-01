"""Credential-free HTTPS readiness probe for the opt-in development profile."""

import argparse
from contextlib import contextmanager
import http.client
import ipaddress
import json
import math
import os
import re
import signal
import ssl
import sys
from urllib.parse import urlsplit


READY_PATH = "/Monolith/health/ready"
MAX_RESPONSE_BYTES = 65536


class ConfigurationError(Exception):
    pass


class ProbeError(Exception):
    pass


class Parser(argparse.ArgumentParser):
    def error(self, message):
        # argparse's normal diagnostics can echo accidentally supplied credentials.
        raise ConfigurationError("invalid arguments; use --help")


def endpoint(url):
    try:
        if not url.isascii() or any(ord(char) <= 32 or ord(char) == 127 for char in url):
            raise ValueError
        parsed = urlsplit(url)
        host = parsed.hostname
        if (parsed.scheme != "https" or not host or parsed.username is not None
                or parsed.password is not None or parsed.path != READY_PATH
                or "?" in url or "#" in url or "\\" in url or "%" in url):
            raise ValueError
        if ":" in host:
            ipaddress.IPv6Address(host)
        elif len(host) > 253 or any(
                not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label)
                for label in host.removesuffix(".").split(".")):
            raise ValueError
        port = parsed.port if parsed.port is not None else 443
        if not 1 <= port <= 65535 or parsed.netloc.endswith(":"):
            raise ValueError
        return host, port
    except ValueError:
        raise ConfigurationError("invalid HTTPS readiness URL") from None


@contextmanager
def request_deadline(seconds):
    # Socket timeouts alone allow trickled headers/body or slow DNS to run forever.
    # This single-shot CLI runs on the main thread of the Linux container.
    def expired(signum, frame):
        raise TimeoutError

    previous = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, expired)
    try:
        signal.setitimer(signal.ITIMER_REAL, seconds)
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def invalid_constant(value):
    raise ValueError


def check(url, ca, timeout):
    host, port = endpoint(url)
    if not math.isfinite(timeout) or not 0 < timeout <= 10:
        raise ConfigurationError("timeout must be greater than 0 and at most 10 seconds")
    if not os.path.isabs(ca) or any(ord(char) < 32 or ord(char) == 127 for char in ca):
        raise ConfigurationError("CA must be an absolute PEM file path")
    try:
        context = ssl.create_default_context(cafile=ca)
    except (OSError, ValueError):
        raise ConfigurationError("CA file is missing, unreadable, or invalid") from None
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED

    # http.client neither uses environment proxies nor follows redirects, and has
    # no cookie jar, netrc lookup, or authentication handler.
    connection = http.client.HTTPSConnection(host, port, context=context, timeout=timeout)
    try:
        with request_deadline(timeout):
            connection.request("GET", READY_PATH, headers={"Accept": "application/json"})
            response = connection.getresponse()
            if response.status != 200:
                raise ProbeError("endpoint did not return HTTP 200")
            body = response.read(MAX_RESPONSE_BYTES + 1)
            if len(body) > MAX_RESPONSE_BYTES:
                raise ProbeError("readiness response too large")
    finally:
        connection.close()
    try:
        payload = json.loads(body.decode("utf-8"), object_pairs_hook=unique_object,
                             parse_constant=invalid_constant)
    except (ValueError, RecursionError):
        raise ProbeError("invalid readiness JSON") from None
    if not isinstance(payload, dict) or payload.get("startupComplete") is not True:
        raise ProbeError("startup is not complete")


def main(argv=None):
    parser = Parser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--url", required=True, help="HTTPS URL ending in " + READY_PATH)
    parser.add_argument("--ca", required=True, help="absolute path to a trusted PEM CA file")
    parser.add_argument("--timeout", type=float, default=5.0, help="total request seconds (0 < n <= 10)")
    try:
        args = parser.parse_args(argv)
        check(args.url, args.ca, args.timeout)
    except SystemExit as error:
        return error.code
    except ConfigurationError as error:
        print("readiness: " + str(error), file=sys.stderr)
        return 2
    except ProbeError as error:
        print("readiness: " + str(error), file=sys.stderr)
        return 1
    except TimeoutError:
        print("readiness: request timed out", file=sys.stderr)
        return 1
    except (OSError, http.client.HTTPException):
        print("readiness: TLS or network failure", file=sys.stderr)
        return 1
    print("readiness: ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
