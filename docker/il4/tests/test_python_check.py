from contextlib import contextmanager, ExitStack
import io
import json
import subprocess
import unittest
from unittest.mock import Mock, patch

from python_check import check, encode_request, read_exact, receive_result


def response_socket(*messages):
    stream = io.BytesIO(b"".join(
        len(data).to_bytes(4, "big") + data
        for data in (json.dumps(message).encode() for message in messages)))
    client = Mock()
    client.recv.side_effect = stream.read
    return client


@contextmanager
def worker_fakes(result=None):
    with ExitStack() as stack:
        directory = stack.enter_context(patch("python_check.tempfile.TemporaryDirectory"))
        directory.return_value.__enter__.return_value = "/fake"
        log = stack.enter_context(patch("python_check.Path.open"))
        log.return_value.__enter__.return_value.read.return_value = "worker diagnostics"
        process = stack.enter_context(patch("python_check.subprocess.Popen")).return_value
        process.poll.return_value = None
        client = response_socket({
            "epoc": "probe".ljust(20, "0"), "operation": "PYTHON", "response": True,
            "payload": [result],
        })
        factory = stack.enter_context(patch("python_check.socket.socket"))
        factory.return_value.__enter__.return_value = client
        clock = stack.enter_context(patch("python_check.time.monotonic", return_value=0))
        stack.enter_context(patch("python_check.time.sleep"))
        stack.enter_context(patch("python_check.sys.version_info", (3, 14)))
        stack.enter_context(patch("builtins.print"))
        yield process, client, clock


class PythonCheckTests(unittest.TestCase):
    def test_request_uses_semoss_wire_format(self):
        epoch = "probe".ljust(20, "0")
        message = encode_request("1 + 1", epoch)
        size = int.from_bytes(message[:4], "big")
        self.assertEqual(message[4:24], epoch.encode("ascii"))
        self.assertEqual(size, len(message[24:]))
        body = json.loads(message[24:])
        self.assertEqual(body["operation"], "PYTHON")
        self.assertEqual(body["payload"], ["1 + 1"])
        self.assertTrue(body["disableCancelTrace"])
        with self.assertRaisesRegex(ValueError, "20 ASCII bytes"):
            encode_request("1 + 1", "short")

    def test_read_exact_handles_partial_reads_and_closed_socket(self):
        client = Mock()
        client.recv.side_effect = [b"ab", b"c"]
        self.assertEqual(read_exact(client, 3), b"abc")
        client.recv.side_effect = [b""]
        with self.assertRaisesRegex(RuntimeError, "closed"):
            read_exact(client, 4)

    def test_receive_skips_logs_and_returns_matching_response(self):
        messages = [
            {"operation": "STDOUT", "payload": ["log"]},
            {"operation": "PYTHON", "response": True, "epoc": "another", "payload": [0]},
            {"operation": "PYTHON", "response": True, "epoc": "probe", "payload": [{"sum": 6}]},
        ]
        client = response_socket(*messages)
        self.assertEqual(receive_result(client, "probe"), {"sum": 6})

    def test_receive_rejects_oversized_or_error_response(self):
        client = Mock()
        client.recv.return_value = (2 ** 31).to_bytes(4, "big")
        with self.assertRaisesRegex(ValueError, "size"):
            receive_result(client, "probe")
        client = response_socket({"epoc": "probe", "ex": "Module import failed"})
        with self.assertRaisesRegex(RuntimeError, "Module import failed"):
            receive_result(client, "probe")

    def test_receive_rejects_bad_shape_and_excessive_logs(self):
        client = response_socket({"epoc": "probe", "operation": "PYTHON",
                                  "response": True, "payload": []})
        with self.assertRaisesRegex(ValueError, "shape"):
            receive_result(client, "probe")
        client = response_socket(*[{"epoc": "probe", "operation": "STDOUT"}] * 1000)
        with self.assertRaisesRegex(RuntimeError, "final result"):
            receive_result(client, "probe")

    def test_check_executes_worker_and_cleans_up(self):
        expected = {"sum": 6, "arrow_rows": 3, "dataset_rows": 3,
                    "prediction": 8.0, "torch_sum": 6}
        with worker_fakes(expected) as (process, client, _):
            client.connect.side_effect = [FileNotFoundError, None]
            check()
            client.sendall.assert_called_once()
            process.terminate.assert_called_once()
            process.wait.assert_called_once_with(timeout=10)

    def test_check_reports_startup_failure_and_timeout(self):
        for exited in (True, False):
            with self.subTest(exited=exited), worker_fakes() as (process, _, clock):
                if exited:
                    process.poll.return_value = 1
                else:
                    clock.side_effect = [0, 31]
                with self.assertRaisesRegex(RuntimeError, "worker diagnostics"):
                    check()
                process.wait.assert_called_once_with(timeout=10)

    def test_check_rejects_wrong_result_and_kills_only_its_child(self):
        with worker_fakes({"wrong": "result"}) as (process, _, _):
            process.wait.side_effect = [subprocess.TimeoutExpired("worker", 10), 0]
            with self.assertRaisesRegex(RuntimeError, "Unexpected SEMOSS Python result"):
                check()
            process.kill.assert_called_once()

    def test_check_requires_python314(self):
        with patch("python_check.sys.version_info", (3, 13)):
            with self.assertRaisesRegex(RuntimeError, "Python 3.14"):
                check()


if __name__ == "__main__":
    unittest.main()
