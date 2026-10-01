"""Exercise the bundled SEMOSS worker with a local, disposable Unix socket."""
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time


def encode_request(code, epoch):
    encoded_epoch = epoch.encode("ascii")
    if len(encoded_epoch) != 20:
        raise ValueError("SEMOSS request epoch must be exactly 20 ASCII bytes")
    body = json.dumps({
        "epoc": epoch, "operation": "PYTHON", "payload": [code],
        "response": False, "insightId": "container-check", "disableCancelTrace": True,
    }).encode("utf-8")
    return len(body).to_bytes(4, "big") + encoded_epoch + body


def read_exact(client, size):
    chunks = bytearray()
    while len(chunks) < size:
        chunk = client.recv(size - len(chunks))
        if not chunk:
            raise RuntimeError("SEMOSS Python worker closed the connection")
        chunks.extend(chunk)
    return bytes(chunks)


def receive_result(client, epoch):
    for _ in range(1000):
        size = int.from_bytes(read_exact(client, 4), "big")
        if not 0 < size <= 1024 * 1024:
            raise ValueError("Invalid SEMOSS response size")
        message = json.loads(read_exact(client, size))
        if message.get("epoc") != epoch:
            continue
        if message.get("ex"):
            raise RuntimeError("SEMOSS Python execution failed: " + str(message["ex"]))
        if (message.get("operation") == "PYTHON" and message.get("response")
                and not message.get("interim")):
            payload = message.get("payload")
            if not isinstance(payload, list) or len(payload) != 1:
                raise ValueError("Unexpected SEMOSS Python result shape")
            return payload[0]
    raise RuntimeError("SEMOSS Python worker did not return a final result")


def check(py_folder=Path("/opt/semosshome/py")):
    if sys.version_info[:2] != (3, 14):
        raise RuntimeError("Python 3.14 is required")
    code = """
import pandas as pd
import pyarrow as pa
from datasets import Dataset
from sklearn.linear_model import LinearRegression
import torch
frame = pd.DataFrame({"value": [1, 2, 3]})
table = pa.Table.from_pandas(frame)
dataset = Dataset.from_dict({"value": [1, 2, 3]})
model = LinearRegression().fit([[1], [2]], [2, 4])
{"sum": int(frame["value"].sum()), "arrow_rows": table.num_rows,
 "dataset_rows": len(dataset), "prediction": round(float(model.predict([[4]])[0]), 4),
 "torch_sum": int(torch.tensor([1, 2, 3]).sum().item())}
"""
    expected = {"sum": 6, "arrow_rows": 3, "dataset_rows": 3,
                "prediction": 8.0, "torch_sum": 6}
    with tempfile.TemporaryDirectory(prefix="semoss-python-") as directory:
        work = Path(directory)
        address = str(work / "worker.sock")
        with (work / "worker.log").open("w+") as log:
            worker = subprocess.Popen([
                sys.executable, str(py_folder / "gaas_tcp_socket_server.py"),
                "--uds-path", address, "--py_folder", str(py_folder),
                "--insight_folder", directory, "--max_count", "1", "--timeout", "1",
                "--logger_level", "WARNING",
            ], stdout=log, stderr=subprocess.STDOUT)
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(120)
                    deadline = time.monotonic() + 30
                    while True:
                        if worker.poll() is not None or time.monotonic() >= deadline:
                            log.seek(0)
                            raise RuntimeError("SEMOSS Python worker did not start:\n" + log.read()[-4096:])
                        try:
                            client.connect(address)
                            break
                        except (FileNotFoundError, ConnectionRefusedError):
                            time.sleep(0.1)
                    epoch = "probe".ljust(20, "0")
                    client.sendall(encode_request(code, epoch))
                    actual = receive_result(client, epoch)
                    if actual != expected:
                        raise RuntimeError("Unexpected SEMOSS Python result: " + repr(actual))
            finally:
                if worker.poll() is None:
                    worker.terminate()
                try:
                    worker.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    worker.kill()
                    worker.wait(timeout=10)
    print("PASS: SEMOSS Python 3.14 worker, pandas/Arrow/datasets, sklearn, CPU torch")


if __name__ == "__main__":
    check()
