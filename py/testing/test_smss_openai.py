"""
Tests for smss_openai, the in-platform routing of the openai SDK.

The real openai package does the request building and response parsing here, so
these exercise the actual wire contract between the SDK and the
OpenAIPassthrough reactor. What stands in for Java is a fake ServerProxy that
records the pixel it was handed and answers with the
{status, contentType, body} map the reactor returns.

Run from the py directory:
    python -m unittest testing.test_smss_openai
"""

import asyncio
import base64
import io
import json
import os
import sys
import threading
import types
import unittest

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SEMOSS_PY = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
sys.path.append(SEMOSS_PY)

CHAT_COMPLETION = {
    "id": "chatcmpl-1",
    "object": "chat.completion",
    "created": 1,
    "model": "engine-1",
    "choices": [
        {
            "index": 0,
            "finish_reason": "stop",
            "message": {"role": "assistant", "content": "hello there"},
        }
    ],
    "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
}

# The slices java hands back, one per poll. Split mid conversation on purpose:
# the SDK has to parse events that arrive across separate polls.
CHAT_COMPLETION_SSE_SLICES = [
    'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,'
    '"model":"engine-1","choices":[{"index":0,"delta":{"role":"assistant",'
    '"content":"hel"},"finish_reason":null}]}\n\n',
    'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,'
    '"model":"engine-1","choices":[{"index":0,"delta":{"content":"lo"},'
    '"finish_reason":null}]}\n\n',
    'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,'
    '"model":"engine-1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n'
    'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,'
    '"model":"engine-1","choices":[],"usage":{"prompt_tokens":3,'
    '"completion_tokens":2,"total_tokens":5}}\n\n'
    "data: [DONE]\n\n",
]


def streaming_poll_sequence(slices=None, job_id="job-1"):
    """A poll answer per slice, the last one marked done."""
    slices = CHAT_COMPLETION_SSE_SLICES if slices is None else slices
    return [
        poll_reactor_response(text, done=(index == len(slices) - 1), job_id=job_id)
        for index, text in enumerate(slices)
    ]


def content_slice(text):
    """One chat.completion.chunk carrying `text`."""
    chunk = {
        "id": "chatcmpl-1",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "engine-1",
        "choices": [{"index": 0, "delta": {"content": text}, "finish_reason": None}],
    }
    return "data: " + json.dumps(chunk) + "\n\n"


def finish_slice():
    """The chunk that closes a conversation, plus the terminal marker."""
    chunk = {
        "id": "chatcmpl-1",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "engine-1",
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
    }
    return "data: " + json.dumps(chunk) + "\n\ndata: [DONE]\n\n"


class FakeServerProxy:
    """Stands in for the socket bridge, answering with canned reactor output."""

    # every (path, body) the transport has sent, newest last
    calls = []
    # the {status, contentType, body} map to answer an OpenAIPassthrough with
    response = None
    # start answers to hand out in order, when several requests are in flight
    response_queue = []
    # the OpenAIStreamPoll answers to hand back in order, for a streamed request
    poll_responses = []
    # per jobId poll queues, for when several streams are in flight
    poll_responses_by_job = {}
    # every jobId OpenAIStreamPoll was called with, newest last
    polls = []
    # when set, raise this instead of answering, the way a pixel failure does
    failure = None

    lock = threading.Lock()

    def get_next_epoc(self):
        return "py_test"

    def callReactor(self, epoc=None, pixel=None, insight_id=None):
        with FakeServerProxy.lock:
            if pixel.startswith("OpenAIStreamPoll"):
                job_id = pixel.split('jobId="')[1].split('"')[0]
                FakeServerProxy.polls.append(job_id)
                if FakeServerProxy.poll_responses_by_job:
                    output = FakeServerProxy.poll_responses_by_job[job_id].pop(0)
                else:
                    output = FakeServerProxy.poll_responses.pop(0)
            else:
                path = pixel.split('path="')[1].split('"')[0]
                payload = pixel.split('payload="')[1].split('"')[0]
                body = (
                    json.loads(base64.b64decode(payload).decode("utf-8"))
                    if payload
                    else {}
                )
                FakeServerProxy.calls.append((path, body))
                if FakeServerProxy.response_queue:
                    output = FakeServerProxy.response_queue.pop(0)
                else:
                    output = FakeServerProxy.response

            if FakeServerProxy.failure is not None:
                raise FakeServerProxy.failure

        return [{"pixelReturn": [{"operationType": ["OPERATION"], "output": output}]}]


def smss_openai_module():
    """The module under test, imported lazily so the fake bridge is in place."""
    import smss_openai

    return smss_openai


def json_reactor_response(payload, status=200):
    return {
        "status": status,
        "contentType": "application/json",
        "body": json.dumps(payload),
    }


def streaming_reactor_response(job_id="job-1"):
    """What OpenAIPassthrough answers a streaming request with: a job to poll."""
    return {"status": 200, "contentType": "text/event-stream", "jobId": job_id}


def poll_reactor_response(body, done=False, job_id="job-1"):
    """One OpenAIStreamPoll answer: a slice of the conversation."""
    return {"jobId": job_id, "body": body, "done": done}


class SmssOpenAITest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # the transport imports gaas_server_proxy at call time, so a module in
        # sys.modules is enough to stand in for the whole socket bridge
        module = types.ModuleType("gaas_server_proxy")
        module.ServerProxy = FakeServerProxy
        sys.modules["gaas_server_proxy"] = module

        import openai
        import smss_openai

        cls.openai = openai
        cls.sdk_client_class = openai.OpenAI
        smss_openai.install()

    def setUp(self):
        FakeServerProxy.calls = []
        FakeServerProxy.polls = []
        FakeServerProxy.poll_responses = []
        FakeServerProxy.poll_responses_by_job = {}
        FakeServerProxy.response_queue = []
        FakeServerProxy.failure = None
        FakeServerProxy.response = None

    def last_call(self):
        return FakeServerProxy.calls[-1]

    def test_patched_classes_subclass_the_sdk(self):
        self.assertIsNot(self.openai.OpenAI, self.sdk_client_class)
        self.assertTrue(issubclass(self.openai.OpenAI, self.sdk_client_class))
        self.assertIs(self.openai.Client, self.openai.OpenAI)
        self.assertIs(self.openai.AsyncClient, self.openai.AsyncOpenAI)

    def test_install_survives_the_import_hook_calling_back_in(self):
        """
        In the platform, install() runs from inside builtins.__import__: the
        hook in gaas_tcp_server_handler fires on `import openai`. Anything that
        imports openai while install() is working therefore re-enters install()
        on the same thread, which deadlocked on the non-reentrant install lock.
        """
        import builtins
        import smss_openai

        real_import = builtins.__import__

        def hook(name, *args, **kwargs):
            module = real_import(name, *args, **kwargs)
            if name.partition(".")[0] == "openai" and not smss_openai.is_installed():
                smss_openai.install(sys.modules.get("openai"))
            return module

        # force a fresh install so the patch actually runs under the hook
        smss_openai._installed = False
        smss_openai._client_classes = None
        self.openai._smss_patched = False
        builtins.__import__ = hook
        try:
            done = threading.Event()

            def run():
                smss_openai.install()
                done.set()

            worker = threading.Thread(target=run, daemon=True)
            worker.start()
            self.assertTrue(
                done.wait(timeout=10),
                "install() deadlocked when the import hook called back into it",
            )
        finally:
            builtins.__import__ = real_import

        self.assertTrue(smss_openai.is_installed())
        self.assertTrue(issubclass(self.openai.OpenAI, self.sdk_client_class))

    def test_client_needs_no_key_and_does_not_retry(self):
        client = self.openai.OpenAI()
        self.assertTrue(str(client.base_url).startswith("http://smss.internal"))
        self.assertEqual(0, client.max_retries)

    def test_a_supplied_key_is_left_alone(self):
        """
        The model engines' python processes run this same handler, so importing
        openai in genai_client installs the patch there too. Those clients are
        built as OpenAI(api_key=<engine key>) with no base_url - routing one of
        those in-platform would have the engine call itself through the model it
        is serving. A supplied credential means the caller wants that service.
        """
        client = self.openai.OpenAI(api_key="sk-the-engines-key")

        self.assertTrue(str(client.base_url).startswith("https://api.openai.com"))
        self.assertNotIsInstance(
            client._client._transport, smss_openai_module().SemossTransport
        )

    def test_a_supplied_provider_is_left_alone(self):
        """The engines' bedrock path passes provider= and no api_key."""
        try:
            from openai.providers import bedrock
        except ImportError:
            self.skipTest("this openai build has no providers module")

        client = self.openai.OpenAI(provider=bedrock(region="us-east-1"))

        self.assertFalse(str(client.base_url).startswith("http://smss.internal"))

    def test_explicit_base_url_is_left_alone(self):
        client = self.openai.OpenAI(
            api_key="sk-real", base_url="https://api.openai.com/v1"
        )
        self.assertTrue(str(client.base_url).startswith("https://api.openai.com"))

    def test_chat_completion(self):
        FakeServerProxy.response = json_reactor_response(CHAT_COMPLETION)
        client = self.openai.OpenAI()

        response = client.chat.completions.create(
            model="engine-1", messages=[{"role": "user", "content": "hi"}]
        )

        self.assertEqual("hello there", response.choices[0].message.content)
        self.assertEqual(5, response.usage.total_tokens)
        path, body = self.last_call()
        self.assertEqual("chat/completions", path)
        self.assertEqual("engine-1", body["model"])
        self.assertEqual("hi", body["messages"][0]["content"])

    def test_extra_body_reaches_the_reactor(self):
        FakeServerProxy.response = json_reactor_response(CHAT_COMPLETION)
        client = self.openai.OpenAI()

        client.chat.completions.create(
            model="engine-1",
            messages=[{"role": "user", "content": "hi"}],
            extra_body={"room_id": "room-7"},
        )

        self.assertEqual("room-7", self.last_call()[1]["room_id"])

    def test_streaming_chat_completion(self):
        FakeServerProxy.response = streaming_reactor_response()
        FakeServerProxy.poll_responses = streaming_poll_sequence()
        client = self.openai.OpenAI()

        content = []
        usage = None
        for chunk in client.chat.completions.create(
            model="engine-1",
            messages=[{"role": "user", "content": "hi"}],
            stream=True,
            stream_options={"include_usage": True},
        ):
            if chunk.choices and chunk.choices[0].delta.content:
                content.append(chunk.choices[0].delta.content)
            if chunk.usage is not None:
                usage = chunk.usage

        self.assertEqual("hello", "".join(content))
        self.assertEqual(5, usage.total_tokens)
        # the stream flag travels on to the model engine, which is what makes it
        # emit the partial chunks java polls for
        self.assertTrue(self.last_call()[1]["stream"])
        # one poll per slice, all against the job the start call handed back
        self.assertEqual(["job-1", "job-1", "job-1"], FakeServerProxy.polls)

    def test_streaming_yields_before_the_model_finishes(self):
        """
        The point of the two call split: the first chunks reach the caller while
        java is still polling the model, instead of after the last token.
        """
        FakeServerProxy.response = streaming_reactor_response()
        FakeServerProxy.poll_responses = streaming_poll_sequence()
        client = self.openai.OpenAI()

        stream = client.chat.completions.create(
            model="engine-1",
            messages=[{"role": "user", "content": "hi"}],
            stream=True,
        )

        first = next(iter(stream))
        self.assertEqual("hel", first.choices[0].delta.content)
        # only the slice carrying that chunk has been fetched so far
        self.assertEqual(1, len(FakeServerProxy.polls))
        self.assertEqual(2, len(FakeServerProxy.poll_responses))

    def test_concurrent_streams_keep_to_their_own_job(self):
        """
        Every streaming request starts its own model job, so two cells (or two
        threads in one cell) streaming at once must not read each other's
        chunks. Each poll goes to the job the start call handed back.
        """
        FakeServerProxy.response_queue = [
            streaming_reactor_response("job-a"),
            streaming_reactor_response("job-b"),
        ]
        FakeServerProxy.poll_responses_by_job = {
            "job-a": streaming_poll_sequence(
                [content_slice("aaa"), finish_slice()], job_id="job-a"
            ),
            "job-b": streaming_poll_sequence(
                [content_slice("bbb"), finish_slice()], job_id="job-b"
            ),
        }
        client = self.openai.OpenAI()
        collected = {}

        def consume(name):
            stream = client.chat.completions.create(
                model="engine-1",
                messages=[{"role": "user", "content": "hi"}],
                stream=True,
            )
            collected[name] = "".join(
                chunk.choices[0].delta.content
                for chunk in stream
                if chunk.choices and chunk.choices[0].delta.content
            )

        threads = [threading.Thread(target=consume, args=(n,)) for n in ("one", "two")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
            self.assertFalse(thread.is_alive(), "a streaming thread hung")

        # whichever thread got which job, neither saw a mix of the two
        self.assertEqual({"aaa", "bbb"}, set(collected.values()))
        self.assertEqual(4, len(FakeServerProxy.polls))
        self.assertEqual(2, FakeServerProxy.polls.count("job-a"))
        self.assertEqual(2, FakeServerProxy.polls.count("job-b"))

    def test_streaming_stops_when_the_job_is_already_gone(self):
        """A poll for a swept or finished job reports done with nothing left."""
        FakeServerProxy.response = streaming_reactor_response()
        FakeServerProxy.poll_responses = [poll_reactor_response("", done=True)]
        client = self.openai.OpenAI()

        chunks = list(
            client.chat.completions.create(
                model="engine-1",
                messages=[{"role": "user", "content": "hi"}],
                stream=True,
            )
        )

        self.assertEqual([], chunks)
        self.assertEqual(1, len(FakeServerProxy.polls))

    def test_models_list_sends_no_body(self):
        FakeServerProxy.response = json_reactor_response(
            {
                "object": "list",
                "data": [{"object": "model", "id": "engine-1", "created": 1}],
            }
        )
        client = self.openai.OpenAI()

        models = client.models.list()

        self.assertEqual(["engine-1"], [model.id for model in models.data])
        self.assertEqual(("models", {}), self.last_call())

    def test_embeddings(self):
        FakeServerProxy.response = json_reactor_response(
            {
                "object": "list",
                "model": "engine-1",
                "data": [{"object": "embedding", "index": 0, "embedding": [0.1, 0.2]}],
                "usage": {"prompt_tokens": 1, "total_tokens": 1},
            }
        )
        client = self.openai.OpenAI()

        response = client.embeddings.create(model="engine-1", input="hello")

        self.assertEqual([0.1, 0.2], response.data[0].embedding)
        self.assertEqual("embeddings", self.last_call()[0])

    def test_reactor_status_becomes_the_sdk_error(self):
        FakeServerProxy.response = json_reactor_response(
            {
                "error": {
                    "message": "Model x inaccessible.",
                    "type": "invalid_request_error",
                }
            },
            status=403,
        )
        client = self.openai.OpenAI()

        with self.assertRaises(self.openai.PermissionDeniedError) as caught:
            client.responses.create(model="engine-1", input="hi")
        self.assertIn("Model x inaccessible.", str(caught.exception))

    def test_pixel_failure_becomes_a_server_error(self):
        FakeServerProxy.failure = Exception("An error occurred running the pixel")
        client = self.openai.OpenAI()

        with self.assertRaises(self.openai.InternalServerError) as caught:
            client.chat.completions.create(
                model="engine-1", messages=[{"role": "user", "content": "hi"}]
            )
        self.assertIn("An error occurred running the pixel", str(caught.exception))

    def test_non_json_request_is_rejected_with_an_explanation(self):
        FakeServerProxy.response = json_reactor_response({})
        client = self.openai.OpenAI()

        with self.assertRaises(self.openai.BadRequestError) as caught:
            client.files.create(
                file=("a.txt", io.BytesIO(b"hello"), "text/plain"), purpose="assistants"
            )
        self.assertIn("json bodies", str(caught.exception))

    def test_async_client(self):
        FakeServerProxy.response = json_reactor_response(CHAT_COMPLETION)

        async def ask():
            client = self.openai.AsyncOpenAI()
            response = await client.chat.completions.create(
                model="engine-1", messages=[{"role": "user", "content": "hi"}]
            )
            return response.choices[0].message.content

        self.assertEqual("hello there", asyncio.run(ask()))

    def test_async_streaming_client(self):
        FakeServerProxy.response = streaming_reactor_response()
        FakeServerProxy.poll_responses = streaming_poll_sequence()

        async def ask():
            client = self.openai.AsyncOpenAI()
            stream = await client.chat.completions.create(
                model="engine-1",
                messages=[{"role": "user", "content": "hi"}],
                stream=True,
            )
            content = []
            async for chunk in stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    content.append(chunk.choices[0].delta.content)
            return "".join(content)

        self.assertEqual("hello", asyncio.run(ask()))


if __name__ == "__main__":
    unittest.main()
