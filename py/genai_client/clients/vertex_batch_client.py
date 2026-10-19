"""
Provider-agnostic helpers for Vertex AI's Cloud Storage-mediated batch
prediction jobs (google.genai's Client().batches.* API). This is Google's
general Vertex batch mechanism -- the same job lifecycle applies whether the
target model is first-party Gemini or a partner model such as Anthropic
Claude; only the per-line request/response JSON shape differs.

This module never touches Cloud Storage directly -- Java drives all GCS I/O
through the SEMOSS storage engine that already holds the credentials for it
(see ModelBatchManager's submitVertexBatch/fetchVertexBatchResults), having
this side build plain JSONL text and parse local copies of the output. That
keeps a storage engine's credentials from ever crossing into this
out-of-process Python runtime.
"""

import json
from typing import Any, Callable, Dict, List, Optional


# Vertex AI JobState values -> the SEMOSS batch-status vocabulary
# (QUEUED/IN_PROGRESS/COMPLETED/FAILED/CANCELED/EXPIRED) used by the
# OpenAI/native-Anthropic paths. JOB_STATE_PAUSED has no clean SEMOSS
# equivalent -- mapped to IN_PROGRESS since the job is neither done nor dead.
_JOB_STATE_MAP = {
    "JOB_STATE_QUEUED": "QUEUED",
    "JOB_STATE_PENDING": "QUEUED",
    "JOB_STATE_RUNNING": "IN_PROGRESS",
    "JOB_STATE_PAUSED": "IN_PROGRESS",
    "JOB_STATE_UPDATING": "IN_PROGRESS",
    "JOB_STATE_SUCCEEDED": "COMPLETED",
    "JOB_STATE_PARTIALLY_SUCCEEDED": "COMPLETED",
    "JOB_STATE_FAILED": "FAILED",
    "JOB_STATE_CANCELLING": "IN_PROGRESS",
    "JOB_STATE_CANCELLED": "CANCELED",
    "JOB_STATE_EXPIRED": "EXPIRED",
}


def normalize_job_state(state: Any) -> str:
    key = getattr(state, "name", None) or str(state)
    if key in _JOB_STATE_MAP:
        return _JOB_STATE_MAP[key]
    return key.upper() if isinstance(key, str) else "UNKNOWN"


def job_output_uri(job: Any) -> Optional[str]:
    """Best-effort read of the output GCS location a succeeded job actually wrote
    results to, straight off the job object (the provider is the source of truth
    for where its own output landed).

    NOTE: the exact attribute the google-genai SDK echoes this back on has not
    been confirmed against a live batch job; adjust the attribute names below
    if this doesn't match what a real BatchJob object returns. Not load-bearing
    today -- Java recomputes the same output prefix it originally chose rather
    than depending on this -- but kept for diagnostics/future use.
    """
    dest = getattr(job, "dest", None)
    if dest is None:
        config = getattr(job, "output_config", None) or getattr(job, "config", None)
        dest = getattr(config, "dest", None) if config is not None else None
    if dest is None:
        return None
    if isinstance(dest, str):
        return dest
    gcs_dest = getattr(dest, "gcs_destination", None) or dest
    return getattr(gcs_dest, "output_uri_prefix", None) or getattr(gcs_dest, "gcs_uri", None)


def normalize_result_line(line: Dict[str, Any]) -> Dict[str, Any]:
    """Normalizes one output JSONL line for an Anthropic-on-Vertex batch item to
    {"custom_id", "ok", "message", "error", "input_tokens", "output_tokens"}.

    The line (checked against a live batch job) is {"custom_id", "status",
    "processed_time", "response"}, the same envelope as Gemini's: "status" is
    an empty string on success and the provider's error otherwise, and
    "response" is the Anthropic Message, empty when the request failed.
    """
    custom_id = line.get("custom_id")
    status = line.get("status")
    message = line.get("response")

    if status or not isinstance(message, dict) or not message:
        return {
            "custom_id": custom_id,
            "ok": False,
            "message": None,
            "error": status or "No response returned for this request",
            "input_tokens": None,
            "output_tokens": None,
        }

    usage = message.get("usage") or {}
    return {
        "custom_id": custom_id,
        "ok": True,
        "message": {
            "role": message.get("role"),
            "content": message.get("content"),
        },
        "error": None,
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
    }


def normalize_gemini_result_line(line: Dict[str, Any]) -> Dict[str, Any]:
    """Normalizes one output JSONL line for a Gemini-on-Vertex batch item to
    {"custom_id", "ok", "message", "error", "input_tokens", "output_tokens"}.

    Gemini's batch output shape (confirmed against Google Cloud's public batch
    prediction sample files/docs) is {"id", "status", "processed_time",
    "request", "response"} -- unlike Anthropic's format there is no "custom_id"
    field; Vertex instead echoes back whatever arbitrary "id" key was set on
    the input line (Vertex itself ignores "id"/"custom_id" for routing, but
    still passes it through), and "response" is a plain GenerateContentResponse
    when present. "status" is an empty string on success and an error message
    otherwise.
    """
    custom_id = line.get("id")
    status = line.get("status")
    response = line.get("response")

    if status or not isinstance(response, dict):
        return {
            "custom_id": custom_id,
            "ok": False,
            "message": None,
            "error": status or "No response returned for this request",
            "input_tokens": None,
            "output_tokens": None,
        }

    candidates = response.get("candidates") or []
    role = None
    text_parts = []
    if candidates:
        content = candidates[0].get("content") or {}
        role = content.get("role")
        for part in content.get("parts") or []:
            # thought summaries are not the answer, same as a regular ask
            if isinstance(part, dict) and "text" in part and not part.get("thought"):
                text_parts.append(part["text"])

    text = "".join(text_parts)
    usage = response.get("usageMetadata") or {}
    # Gemini counts thoughts apart from the answer; fold them in so output_tokens
    # is all billed output, the same as a regular ask
    output_tokens = usage.get("candidatesTokenCount")
    if usage.get("thoughtsTokenCount"):
        output_tokens = (output_tokens or 0) + usage["thoughtsTokenCount"]
    return {
        "custom_id": custom_id,
        "ok": True,
        "message": {
            "role": role,
            # content blocks, the shape BatchResultItem reads from every provider
            "content": [{"type": "text", "text": text}] if text else [],
        },
        "error": None,
        "input_tokens": usage.get("promptTokenCount"),
        "output_tokens": output_tokens,
    }


def read_output_files(
    paths: List[str], normalize: Callable[[Dict[str, Any]], Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Normalizes every line of the output files Java downloaded for a batch,
    one line at a time, so a large output is never held as one string."""
    items = []
    for path in paths:
        with open(path, encoding="utf-8") as output:
            for raw_line in output:
                raw_line = raw_line.strip()
                if raw_line:
                    items.append(normalize(json.loads(raw_line)))
    return items
