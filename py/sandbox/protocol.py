"""
Protocol helpers for the Semoss PayloadStruct wire format.

Wire format (NativePy JSON):
    [4-byte big-endian message length][20-byte epoc id][UTF-8 JSON payload]

The JSON payload mirrors Java's PayloadStruct fields:
    epoc, operation, methodName, payload, payloadClassNames,
    insightId, executionInsightId, jobId, sessionId, mdc,
    asset_paths, ex, response, interim, processed, objId,
    engineType
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class Operation(str, Enum):
    """Operations matching Java PayloadStruct.OPERATION."""
    R = "R"
    PYTHON = "PYTHON"
    CHROME = "CHROME"
    ECHO = "ECHO"
    ENGINE = "ENGINE"
    REACTOR = "REACTOR"
    INSIGHT = "INSIGHT"
    PROJECT = "PROJECT"
    CMD = "CMD"
    STDOUT = "STDOUT"
    STDERR = "STDERR"
    STRUCTURED_STREAM = "STRUCTURED_STREAM"


# --- Standalone sandbox protocol messages (JSON-lines) ---

@dataclass
class ExecuteRequest:
    """Client -> Supervisor: execute Python code."""
    user_id: str
    code: str
    insight_id: Optional[str] = None
    type: str = "execute"


@dataclass
class UpdateFoldersRequest:
    """Client -> Supervisor: change visible folders, triggers sandbox restart."""
    user_id: str
    folders: List[Dict[str, Any]] = field(default_factory=list)
    type: str = "update_folders"


@dataclass
class DisconnectRequest:
    """Client -> Supervisor: clean shutdown for user."""
    user_id: str
    type: str = "disconnect"


@dataclass
class ExecutionResult:
    """Supervisor -> Client (or entrypoint -> supervisor)."""
    stdout: str = ""
    stderr: str = ""
    error: Optional[str] = None


@dataclass
class FolderMount:
    """Describes a folder to expose inside the sandbox."""
    name: str       # folder name under /data/shared/
    writable: bool  # mount as rw or ro

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "writable": self.writable}

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "FolderMount":
        return cls(name=d["name"], writable=d.get("writable", False))


# --- Semoss wire-format helpers ---

EPOC_SIZE = 20
HEADER_SIZE = 4  # uint32 big-endian length prefix


def encode_semoss_message(epoc: str, payload: dict) -> bytes:
    """Encode a message in the Semoss NativePy wire format.

    Returns bytes: [4-byte len][20-byte epoc][json-utf8]
    """
    epoc_bytes = epoc.ljust(EPOC_SIZE)[:EPOC_SIZE].encode("utf-8")
    body = json.dumps(payload, default=str).encode("utf-8")
    length = len(body)
    return struct.pack(">I", length) + epoc_bytes + body


def decode_semoss_header(data: bytes) -> tuple[int, str]:
    """Decode the 4-byte length + 20-byte epoc from raw bytes.

    Returns (message_length, epoc_string).
    """
    if len(data) < HEADER_SIZE + EPOC_SIZE:
        raise ValueError(f"Header too short: {len(data)} bytes")
    length = struct.unpack(">I", data[:HEADER_SIZE])[0]
    epoc = data[HEADER_SIZE : HEADER_SIZE + EPOC_SIZE].decode("utf-8").strip()
    return length, epoc


async def read_semoss_message(reader) -> tuple[str, dict]:
    """Read one Semoss-framed message from an asyncio StreamReader.

    Returns (epoc, payload_dict).
    """
    header = await reader.readexactly(HEADER_SIZE + EPOC_SIZE)
    length, epoc = decode_semoss_header(header)
    body = await reader.readexactly(length)
    payload = json.loads(body.decode("utf-8"))
    return epoc, payload


async def write_semoss_message(writer, epoc: str, payload: dict) -> None:
    """Write one Semoss-framed message to an asyncio StreamWriter."""
    raw = encode_semoss_message(epoc, payload)
    writer.write(raw)
    await writer.drain()


def read_semoss_message_sync(sock) -> tuple[str, dict]:
    """Read one Semoss-framed message from a blocking socket.

    Returns (epoc, payload_dict).
    """
    header = _recvall(sock, HEADER_SIZE + EPOC_SIZE)
    length, epoc = decode_semoss_header(header)
    body = _recvall(sock, length)
    payload = json.loads(body.decode("utf-8"))
    return epoc, payload


def write_semoss_message_sync(sock, epoc: str, payload: dict) -> None:
    """Write one Semoss-framed message to a blocking socket."""
    raw = encode_semoss_message(epoc, payload)
    sock.sendall(raw)


def _recvall(sock, n: int) -> bytes:
    """Read exactly n bytes from a blocking socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed while reading")
        buf.extend(chunk)
    return bytes(buf)


# --- JSON-lines helpers (supervisor <-> entrypoint communication) ---

def encode_jsonl(obj: dict) -> bytes:
    """Encode a dict as a single JSON line (UTF-8 + newline)."""
    return (json.dumps(obj, default=str) + "\n").encode("utf-8")


def decode_jsonl(line: str) -> dict:
    """Decode a single JSON line."""
    return json.loads(line.strip())
