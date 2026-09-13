"""
Route the openai SDK back into Tomcat instead of out to the network.

A script running in an insight is already authenticated: it has a user, and
that user already has access to some set of MODEL engines. Making the script
paste an access/secret key into python just to reach those models is both
redundant and exposes a secret. So the SDK is pointed at the
platform instead, and copy/pasted code runs unchanged:

```python
from openai import OpenAI

client = OpenAI()
client.chat.completions.create(
    model="<engine id>", messages=[{"role": "user", "content": "hi"}]
)
```

The real openai package stays in place and keeps doing its own request
building, response parsing, typed models and streaming. Only the bottom layer
is swapped out: an httpx transport that, instead of opening a socket, runs the
`OpenAIPassthrough(...)` pixel back over the insight socket. Java serves that
pixel through the same `prerna.engine.impl.model.openai` helpers that back
`/Monolith/api/model/openai`, so the bytes handed to the SDK are the bytes that
endpoint would have written to the wire.

Engine access is still authorized on the Java side against the user who owns
the executing insight, so a script can only reach the models its user can view.

Passing an explicit `base_url` (or exporting `OPENAI_BASE_URL`) opts back out
and talks to that host over the network with whatever key was supplied, so
reaching the real api.openai.com with your own key still works.

`stream=True` yields tokens as the model produces them. A pixel cannot hold a
response open, so a streaming request answers with the id of the job running
the model, and the transport drains that job with repeated
`OpenAIStreamPoll(...)` calls, handing each slice of SSE to the SDK as it
arrives.
"""

import asyncio
import base64
import json
import os
import re
import sys
import threading

import httpx

# Any absolute url works here - the transport never resolves or dials it - but
# it has to look like an openai base so the SDK builds the paths it always does.
INTERNAL_BASE_URL = "http://smss.internal/v1"

# The SDK refuses to construct without a key. Nothing reads this value: the
# caller is authorized by the insight it is running inside.
INTERNAL_API_KEY = "smss-internal"

JSON_CONTENT_TYPE = "application/json"
SSE_CONTENT_TYPE = "text/event-stream"

# The path goes into a pixel string, so keep it to the characters an openai
# route and an engine id can actually contain.
_SAFE_PATH = re.compile(r"^[A-Za-z0-9/_.\-]+$")

# Sentinel for "the caller did not pass this argument at all", which is not the
# same as passing None and not the same as the SDK's own NOT_GIVEN.
_UNSET = object()

_install_lock = threading.Lock()

# Re-entrancy marker for the thread currently installing. Per thread rather than
# global: only the installing thread can re-enter through the import hook, and
# any other thread must still block on the lock and see a finished patch.
_install_state = threading.local()

_installed = False


class _PassthroughError(Exception):
    """The pixel call itself failed, as opposed to the model returning an error."""


def _normalize_path(url_path):
    """
    Reduce an SDK request url to the operation the reactor dispatches on.

    Args:
        url_path (`str`): the path portion of the request url, e.g.
            "/v1/chat/completions".

    Returns:
        `str`: the bare operation, e.g. "chat/completions".
    """
    path = (url_path or "").strip().strip("/")
    if path.startswith("v1/"):
        path = path[len("v1/") :]
    return path


def _run_pixel(pixel):
    """
    Run a pixel over the insight socket and return the map it answered with.

    Args:
        pixel (`str`): the pixel expression to execute.

    Returns:
        `dict`: the reactor's output map.

    Raises:
        _PassthroughError: if the pixel could not be run, or answered with
            something other than a map.
    """
    from gaas_server_proxy import ServerProxy

    proxy = ServerProxy()
    pixel_return = proxy.callReactor(epoc=proxy.get_next_epoc(), pixel=pixel)
    if not pixel_return:
        raise _PassthroughError("The pixel returned no output")

    result = pixel_return[0]["pixelReturn"][0]
    if "ERROR" in result["operationType"]:
        raise _PassthroughError(str(result["output"]))

    output = result["output"]
    if not isinstance(output, dict):
        raise _PassthroughError(f"Unexpected pixel output: {output}")
    return output


def _call_passthrough(path, body):
    """
    Run the OpenAIPassthrough pixel and return the response it describes.

    A streaming request answers with a job id in place of a body; the caller
    then drains it through `_poll_stream`.

    Args:
        path (`str`): the normalized openai operation, e.g. "chat/completions".
        body (`dict`): the request body the SDK built.

    Returns:
        `Tuple[int, str, str, Optional[str]]`: the status code, content type,
        body text, and the job id to poll when the response is streamed.

    Raises:
        _PassthroughError: if the pixel could not be run at all.
    """
    request_json = json.dumps(body, ensure_ascii=False)
    payload = base64.b64encode(request_json.encode("utf-8")).decode("ascii")
    output = _run_pixel(f'OpenAIPassthrough(path="{path}", payload="{payload}");')

    job_id = output.get("jobId")
    if job_id is None and "body" not in output:
        raise _PassthroughError(f"Unexpected OpenAIPassthrough output: {output}")

    status = int(output.get("status", 200))
    content_type = output.get("contentType") or JSON_CONTENT_TYPE
    return status, content_type, output.get("body") or "", job_id


def _poll_stream(job_id):
    """
    Yield the SSE of a streaming response as java produces it.

    Each poll hands back whatever the model has emitted since the last one. The
    reactor waits briefly for output before answering, so an idle generation
    costs a few round trips a second rather than a busy loop.

    Args:
        job_id (`str`): the job returned by the streaming request.

    Yields:
        `bytes`: the next slice of the SSE conversation.
    """
    while True:
        output = _run_pixel(f'OpenAIStreamPoll(jobId="{job_id}");')
        text = output.get("body") or ""
        if text:
            yield text.encode("utf-8")
        if output.get("done"):
            return


def _error_body(message):
    """Build the openai error payload so the SDK raises its own typed error."""
    return json.dumps({"error": {"message": message, "type": "api_error"}})


def _resolve_request(method, url_path, content):
    """
    Turn an intercepted http request into the status / content type / body the
    platform answers it with. Shared by the sync and async transports.

    Args:
        method (`str`): the http method the SDK used.
        url_path (`str`): the path portion of the request url.
        content (`bytes`): the raw request body, empty for GET requests.

    Returns:
        `Tuple[int, str, str, Optional[str]]`: the status code, content type,
        body text, and the job id to poll when the response is streamed.
    """
    path = _normalize_path(url_path)
    if not _SAFE_PATH.match(path):
        return (
            404,
            JSON_CONTENT_TYPE,
            _error_body(f"Unsupported openai path '{path}'"),
            None,
        )

    body = {}
    if content:
        try:
            body = json.loads(content)
        except ValueError:
            return (
                400,
                JSON_CONTENT_TYPE,
                _error_body(
                    f"The in-platform openai client can only send json bodies, "
                    f"so '{method} {path}' is not supported here. Pass an explicit "
                    f"base_url to reach a real openai endpoint instead."
                ),
                None,
            )
    if not isinstance(body, dict):
        return (
            400,
            JSON_CONTENT_TYPE,
            _error_body("The request body must be a json object"),
            None,
        )

    try:
        return _call_passthrough(path, body)
    except Exception as e:
        # a pixel level failure (bad reactor output, socket trouble, an exception
        # raised inside the reactor) becomes a 500 so the SDK raises its own
        # APIStatusError carrying the message instead of a bare Exception
        return 500, JSON_CONTENT_TYPE, _error_body(str(e) or type(e).__name__), None


class _PolledStream(httpx.SyncByteStream):
    """Feeds httpx the SSE of a streaming response as java produces it."""

    def __init__(self, job_id):
        self._job_id = job_id

    def __iter__(self):
        return _poll_stream(self._job_id)


class _AsyncPolledStream(httpx.AsyncByteStream):
    """
    Async twin of `_PolledStream`.

    Each poll blocks the calling thread until java answers, so it runs in a
    worker thread rather than on the event loop.
    """

    def __init__(self, job_id):
        self._job_id = job_id

    async def __aiter__(self):
        while True:
            output = await asyncio.to_thread(
                _run_pixel, f'OpenAIStreamPoll(jobId="{self._job_id}");'
            )
            text = output.get("body") or ""
            if text:
                yield text.encode("utf-8")
            if output.get("done"):
                return


def _build_response(request, status, content_type, text, job_id=None):
    """Build the httpx response the SDK reads, polling when it is streamed."""
    headers = {"content-type": content_type}
    if job_id is not None:
        return httpx.Response(
            status, headers=headers, stream=_PolledStream(job_id), request=request
        )
    return httpx.Response(
        status, headers=headers, content=text.encode("utf-8"), request=request
    )


def _build_async_response(request, status, content_type, text, job_id=None):
    """Async twin of `_build_response`."""
    headers = {"content-type": content_type}
    if job_id is not None:
        return httpx.Response(
            status, headers=headers, stream=_AsyncPolledStream(job_id), request=request
        )
    return httpx.Response(
        status, headers=headers, content=text.encode("utf-8"), request=request
    )


class SemossTransport(httpx.BaseTransport):
    """
    httpx transport that answers openai requests from the platform.

    Mount it under any openai client to make that client talk to the engines the
    executing insight's user can view, with no key and no network call.
    """

    def handle_request(self, request):
        # read() rather than .content: a multipart body (a file upload) is still
        # a stream at this point, and .content raises on those
        content = request.read()
        status, content_type, text, job_id = _resolve_request(
            request.method, request.url.path, content
        )
        return _build_response(request, status, content_type, text, job_id)


class AsyncSemossTransport(httpx.AsyncBaseTransport):
    """
    Async twin of `SemossTransport`.

    The pixel call blocks the calling thread until Java answers, so it runs in a
    worker thread rather than on the event loop.
    """

    async def handle_async_request(self, request):
        content = await request.aread()
        status, content_type, text, job_id = await asyncio.to_thread(
            _resolve_request, request.method, request.url.path, content
        )
        return _build_async_response(request, status, content_type, text, job_id)


def _route_in_platform(base_url, kwargs):
    """
    Decide whether this client should be served in-platform.

    A caller who named a host, or who asked for a specific provider or workload
    identity, means it: leave that client alone so it goes out to the network
    the way it was configured.

    Args:
        base_url (`Any`): the base_url the caller passed, or `_UNSET`.
        kwargs (`dict`): the rest of the caller's keyword arguments.

    Returns:
        `bool`: whether to mount the in-platform transport.
    """
    if base_url is not _UNSET and base_url is not None:
        return False
    if os.environ.get("OPENAI_BASE_URL"):
        return False
    opt_outs = ("provider", "workload_identity", "websocket_base_url", "data_residency")
    for opt_out in opt_outs:
        if kwargs.get(opt_out) is not None:
            return False
    return True


def _client_defaults(kwargs, transport):
    """
    Fill in the arguments that make a client route in-platform: the placeholder
    key the SDK insists on, the sentinel base url, and our transport.

    Retries and timeouts are turned off because there is no network to be flaky
    and no response to time out - the call returns when Java answers, and a
    retry would just ask the model the same question twice.
    """
    kwargs["base_url"] = INTERNAL_BASE_URL
    if not kwargs.get("api_key"):
        kwargs["api_key"] = os.environ.get("OPENAI_API_KEY") or INTERNAL_API_KEY
    if kwargs.get("http_client") is None:
        kwargs["http_client"] = transport
    kwargs.setdefault("max_retries", 0)
    kwargs.setdefault("timeout", None)
    return kwargs


_client_classes = None


def _make_client_classes(openai_module):
    """
    Build the patched client classes, once per process.

    Takes the openai module rather than importing it: this runs while the
    install lock is held, and an import here would re-enter install() through
    the import hook.

    Args:
        openai_module (`ModuleType`): the already-imported openai module.

    Returns:
        `Tuple[type, type]`: the sync and async client subclasses.
    """
    global _client_classes
    if _client_classes is not None:
        return _client_classes

    _openai = openai_module

    class SemossOpenAI(_openai.OpenAI):
        """openai.OpenAI that serves requests from the platform by default."""

        def __init__(self, *args, base_url=_UNSET, **kwargs):
            if _route_in_platform(base_url, kwargs):
                kwargs = _client_defaults(
                    kwargs, httpx.Client(transport=SemossTransport(), timeout=None)
                )
            elif base_url is not _UNSET:
                kwargs["base_url"] = base_url
            super().__init__(*args, **kwargs)

    class SemossAsyncOpenAI(_openai.AsyncOpenAI):
        """openai.AsyncOpenAI that serves requests from the platform by default."""

        def __init__(self, *args, base_url=_UNSET, **kwargs):
            if _route_in_platform(base_url, kwargs):
                kwargs = _client_defaults(
                    kwargs,
                    httpx.AsyncClient(transport=AsyncSemossTransport(), timeout=None),
                )
            elif base_url is not _UNSET:
                kwargs["base_url"] = base_url
            super().__init__(*args, **kwargs)

    SemossOpenAI.__name__ = "OpenAI"
    SemossOpenAI.__qualname__ = "OpenAI"
    SemossAsyncOpenAI.__name__ = "AsyncOpenAI"
    SemossAsyncOpenAI.__qualname__ = "AsyncOpenAI"
    _client_classes = (SemossOpenAI, SemossAsyncOpenAI)
    return _client_classes


def __getattr__(name):
    """
    Expose `OpenAI` / `AsyncOpenAI` for code that wants the platform client
    explicitly, without building them until they are asked for.
    """
    if name in ("OpenAI", "AsyncOpenAI"):
        import openai

        client_class, async_client_class = _make_client_classes(openai)
        globals()["OpenAI"] = client_class
        globals()["AsyncOpenAI"] = async_client_class
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def is_installed():
    """Whether the openai package has already been pointed at the platform."""
    return _installed


def install(openai_module=None):
    """
    Point the openai package's client classes at the platform.

    Rebinding the classes on the module is what makes an unmodified
    `from openai import OpenAI` pick this up: the name is looked up on the
    module after the import completes, so a script, or a library like
    langchain-openai that builds `openai.OpenAI(...)` at call time, gets the
    patched class without knowing anything about SEMOSS.

    Safe to call repeatedly; the second call is a no-op.

    Args:
        openai_module (`Optional[ModuleType]`): the already-imported openai
            module. Taken from sys.modules when not passed.

    Returns:
        `bool`: whether the patch is in place.
    """
    global _installed

    if openai_module is None:
        openai_module = sys.modules.get("openai")
        if openai_module is None:
            # resolve the module before taking the lock - this import runs
            # through the hook that calls straight back in here
            import openai as openai_module

    # install() is called from inside builtins.__import__, and patching the SDK
    # touches openai attributes that can themselves import submodules, which
    # calls the hook again on this same thread. The lock is not reentrant, so a
    # nested call has to return rather than wait for a lock this thread already
    # holds. The outer call is doing the work.
    if getattr(_install_state, "installing", False):
        return _installed

    with _install_lock:
        if getattr(openai_module, "_smss_patched", False):
            _installed = True
            return True

        _install_state.installing = True
        try:
            client_class, async_client_class = _make_client_classes(openai_module)
            openai_module.OpenAI = client_class
            openai_module.Client = client_class
            openai_module.AsyncOpenAI = async_client_class
            openai_module.AsyncClient = async_client_class
            # the SDK raises on construction when it cannot find a key, before
            # any request reaches the transport, so anything building its own
            # client needs to find something in the environment
            os.environ.setdefault("OPENAI_API_KEY", INTERNAL_API_KEY)
            openai_module._smss_patched = True
            _installed = True
        finally:
            _install_state.installing = False
        return True
