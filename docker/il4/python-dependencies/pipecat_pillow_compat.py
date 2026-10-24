"""Correct pipecat-ai 1.13.0's installed METADATA to reflect the independently
verified compatibility with Pillow 13.0.0, instead of the package's own
stale Pillow<13 upper bound.

pipecat-ai's declared Requires-Dist (Pillow<13,>=11.1.0) is not evidence of
a real incompatibility: a source trace of pipecat-ai 1.13.0's real PyPI sdist
found its Pillow usage limited to open/frombytes/convert/resize/save/tobytes
on raster frames (see compatibility.json's pillow approved_compatibility_changes
entry), touching none of Pillow 13.0.0's documented behavioral changes, and a
functional smoke test against pipecat-ai's real encode_image() call chain
passed under Pillow 13.0.0. `uv pip check` otherwise fails the build on this
known, evidenced mismatch alone (pillow>=13.0.0 is forced via overrides.lock
to close CVE-2026-112393/112403/112900 and the rest of pillow's findings).
This is a metadata correction, not a behavior change: it narrows the bound to
<14, not an open-ended removal, so a future Pillow 14 would still need its own
review.
"""
import hashlib
import sys
from pathlib import Path

EXPECTED_SHA256 = "91f27670eb636401fec8443e431b8e488846524cde4013256b51934867e0b203"
OLD_LINE = "Requires-Dist: Pillow<13,>=11.1.0\n"
NEW_LINE = "Requires-Dist: Pillow>=11.1.0,<14\n"


def patch_metadata(path):
    original = path.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    if digest != EXPECTED_SHA256:
        raise ValueError("Unexpected pipecat-ai METADATA hash: " + digest)
    text = original.decode("utf-8")
    if text.count(OLD_LINE) != 1:
        raise ValueError("pipecat-ai compatibility patch target must occur exactly once")
    updated = text.replace(OLD_LINE, NEW_LINE, 1).encode("utf-8")
    path.write_bytes(updated)
    return {
        "path": "pipecat_ai-1.13.0.dist-info/METADATA",
        "change": "Requires-Dist: Pillow<13,>=11.1.0 -> Pillow>=11.1.0,<14",
        "original_sha256": digest,
        "patched_sha256": hashlib.sha256(updated).hexdigest(),
    }


if __name__ == "__main__":
    venv = sys.argv[1] if len(sys.argv) > 1 else "/opt/semoss-python"
    candidates = list(Path(venv).glob("lib/python3.*/site-packages/pipecat_ai-*.dist-info/METADATA"))
    if len(candidates) != 1:
        raise ValueError("Expected exactly one pipecat-ai METADATA under " + venv + ", found " + str(len(candidates)))
    import json
    print(json.dumps(patch_metadata(candidates[0])))
