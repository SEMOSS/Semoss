import hashlib
from pathlib import Path
import tempfile
import unittest

import pin_dev_artifacts


class PinDevArtifactsTests(unittest.TestCase):
    def manifest(self, semosshome_sha, war_sha, libraries_sha):
        return [
            {"coordinate": "org.semoss:semoss:0.0.1-SNAPSHOT:tar.gz:semosshome",
             "sha256": semosshome_sha},
            {"coordinate": "org.semoss:monolith:0.0.1-SNAPSHOT:war", "sha256": war_sha},
            {"coordinate": "org.semoss:monolith:0.0.1-SNAPSHOT:tar.gz:libraries",
             "sha256": libraries_sha},
            {"coordinate": "org.apache.tomcat:tomcat:11.0.26:tar.gz", "sha256": "unrelated"},
        ]

    def test_repins_only_the_three_dev_build_coordinates(self):
        with tempfile.TemporaryDirectory() as workdir:
            artifacts_dir = Path(workdir)
            contents = {
                "semoss-0.0.1-SNAPSHOT-semosshome.tar.gz": b"semosshome bytes",
                "monolith-0.0.1-SNAPSHOT.war": b"war bytes",
                "monolith-0.0.1-SNAPSHOT-libraries.tar.gz": b"libraries bytes",
            }
            for name, data in contents.items():
                (artifacts_dir / name).write_bytes(data)
            manifest = self.manifest("stale-1", "stale-2", "stale-3")
            updated = pin_dev_artifacts.pin(manifest, artifacts_dir)
            self.assertEqual({change["coordinate"] for change in updated},
                             {entry["coordinate"] for entry in manifest[:3]})
            for entry, (name, data) in zip(manifest, contents.items()):
                self.assertEqual(entry["sha256"], hashlib.sha256(data).hexdigest())
            self.assertEqual(manifest[3]["sha256"], "unrelated")

    def test_is_idempotent_once_hashes_already_match(self):
        with tempfile.TemporaryDirectory() as workdir:
            artifacts_dir = Path(workdir)
            data = b"identical bytes"
            for name in ("semoss-0.0.1-SNAPSHOT-semosshome.tar.gz", "monolith-0.0.1-SNAPSHOT.war",
                         "monolith-0.0.1-SNAPSHOT-libraries.tar.gz"):
                (artifacts_dir / name).write_bytes(data)
            digest = hashlib.sha256(data).hexdigest()
            manifest = self.manifest(digest, digest, digest)
            updated = pin_dev_artifacts.pin(manifest, artifacts_dir)
            self.assertEqual(updated, [])

    def test_rejects_a_lock_file_missing_an_expected_coordinate(self):
        manifest = [entry for entry in self.manifest("a", "b", "c")
                    if entry["coordinate"] != "org.semoss:monolith:0.0.1-SNAPSHOT:war"]
        with self.assertRaisesRegex(ValueError, "missing expected coordinates"):
            pin_dev_artifacts.pin(manifest, Path("unused"))


if __name__ == "__main__":
    unittest.main()
