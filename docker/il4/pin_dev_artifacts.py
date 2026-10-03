"""Patch artifacts.lock.json's dev-HEAD Monolith/semosshome entries to match
a freshly built artifact before assemble.py consumes it.

Maven output (javadoc timestamps, archive member ordering) is not byte
reproducible between builds of the same source commit, so these three
coordinates cannot carry a fixed sha256 the way an externally fetched release
artifact can. The real integrity control for them is the pinned commits that
produced them (this branch's own HEAD, and the MONOLITH_COMMIT pinned in
il4-container.yml), not their build output's hash. This script closes the
gap between "what was just built" and "what assemble.py's resolve() will
accept" without weakening that check for every other, externally sourced
coordinate.
"""
import hashlib
import json
import sys
from pathlib import Path


DEV_BUILD_COORDINATES = {
    "semoss-0.0.1-SNAPSHOT-semosshome.tar.gz": "org.semoss:semoss:0.0.1-SNAPSHOT:tar.gz:semosshome",
    "monolith-0.0.1-SNAPSHOT.war": "org.semoss:monolith:0.0.1-SNAPSHOT:war",
    "monolith-0.0.1-SNAPSHOT-libraries.tar.gz": "org.semoss:monolith:0.0.1-SNAPSHOT:tar.gz:libraries",
}


def pin(manifest, artifacts_dir):
    by_coordinate = {entry["coordinate"]: entry for entry in manifest}
    missing = [coordinate for coordinate in DEV_BUILD_COORDINATES.values()
               if coordinate not in by_coordinate]
    if missing:
        raise ValueError("Lock file is missing expected coordinates: " + ", ".join(missing))
    updated = []
    for filename, coordinate in DEV_BUILD_COORDINATES.items():
        digest = hashlib.sha256((artifacts_dir / filename).read_bytes()).hexdigest()
        entry = by_coordinate[coordinate]
        if entry["sha256"] != digest:
            updated.append({"coordinate": coordinate, "from": entry["sha256"], "to": digest})
            entry["sha256"] = digest
    return updated


def main(lock_path=Path("artifacts.lock.json"), artifacts_dir=Path("dev-build-artifacts")):
    manifest = json.loads(lock_path.read_text())
    updated = pin(manifest, artifacts_dir)
    lock_path.write_text(
        "[\n" + ",\n".join("  " + json.dumps(entry) for entry in manifest) + "\n]\n")
    for change in updated:
        print(f"Repinned {change['coordinate']}: {change['from']} -> {change['to']}")


if __name__ == "__main__":
    main()
