"""
Provider-agnostic helpers for Vertex AI's Cloud Storage-mediated batch
prediction jobs (google.genai's Client().batches.* API). This is Google's
general Vertex batch mechanism -- the same job lifecycle applies whether the
target model is first-party Gemini or a partner model such as Anthropic
Claude; only the per-line request/response JSON shape differs.

This module never touches Cloud Storage directly -- Java drives all GCS I/O
through the SEMOSS storage engine that already holds the credentials for it
(see ModelBatchManager's submitVertexBatch/fetchVertexBatchResults), handing
this side plain JSONL text to build/parse. That keeps a storage engine's
credentials from ever crossing into this out-of-process Python runtime.
"""

from typing import Any, Dict, Optional


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

    NOTE: the exact key the prediction body is nested under (tried below in
    order) has not been confirmed against a live batch job output sample --
    adjust to match once one is available.
    """
    custom_id = line.get("custom_id")
    body = line.get("response") or line.get("result") or line.get("prediction") or line

    error = line.get("error") or (body.get("error") if isinstance(body, dict) else None)
    if error:
        return {
            "custom_id": custom_id,
            "ok": False,
            "message": None,
            "error": error,
            "input_tokens": None,
            "output_tokens": None,
        }

    message = body.get("message") if isinstance(body, dict) and "message" in body else body
    usage = (message or {}).get("usage") if isinstance(message, dict) else None
    return {
        "custom_id": custom_id,
        "ok": True,
        "message": {
            "role": (message or {}).get("role"),
            "content": (message or {}).get("content"),
        },
        "error": None,
        "input_tokens": (usage or {}).get("input_tokens"),
        "output_tokens": (usage or {}).get("output_tokens"),
    }
