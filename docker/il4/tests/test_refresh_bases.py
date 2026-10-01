import contextlib
import io
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock, patch

from refresh_bases import main, parse_tags, resolve_bases


NAMES = ("MAVEN_IMAGE", "UBI_IMAGE", "PYTHON_IMAGE")
OLD_DIGEST = "a" * 64
NEW_DIGEST = "b" * 64


def dockerfile():
    return "\n".join(
        f"ARG {name}=registry1.dso.mil/ironbank/test/{name.lower()}:stable@sha256:{OLD_DIGEST}"
        for name in NAMES
    )


class RefreshBasesTests(unittest.TestCase):
    def test_parses_only_existing_tags_without_changing_versions(self):
        tags = parse_tags(dockerfile())
        self.assertEqual(tuple(tags), NAMES)
        for name, tag in tags.items():
            self.assertEqual(tag, f"registry1.dso.mil/ironbank/test/{name.lower()}:stable")
        self.assertEqual(parse_tags(dockerfile() + "\nARG UNRELATED=example"), tags)

    def test_rejects_missing_duplicate_or_untrusted_definitions(self):
        valid = dockerfile()
        for invalid in (
            "",
            valid.splitlines()[0],
            valid + "\n" + valid.splitlines()[0],
            valid.replace("registry1.dso.mil", "example.com"),
            valid.replace("stable@", "@"),
            valid.replace(OLD_DIGEST, "bad"),
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                parse_tags(invalid)

    @patch("refresh_bases.subprocess.run")
    def test_resolves_every_tag_to_a_validated_immutable_digest(self, run):
        run.return_value = Mock(stdout=json.dumps({"digest": "sha256:" + NEW_DIGEST}))
        tags = parse_tags(dockerfile())
        result = resolve_bases(tags)
        self.assertEqual(run.call_count, 3)
        for name, tag in tags.items():
            self.assertEqual(result[name], tag + "@sha256:" + NEW_DIGEST)
        self.assertEqual(run.call_args.args[0][:4], ["docker", "buildx", "imagetools", "inspect"])
        self.assertTrue(run.call_args.kwargs["check"])
        self.assertEqual(run.call_args.kwargs["timeout"], 120)

    @patch("refresh_bases.subprocess.run")
    def test_rejects_malformed_registry_output(self, run):
        for output in ("not json", "[]", "{}", '{"digest": null}',
                       '{"digest": "sha256:bad"}'):
            run.return_value = Mock(stdout=output)
            with self.subTest(output=output), self.assertRaises(ValueError):
                resolve_bases(parse_tags(dockerfile()))

    @patch("refresh_bases.subprocess.run")
    def test_registry_failure_does_not_fall_back_to_old_pins(self, run):
        run.side_effect = subprocess.CalledProcessError(1, ["docker"], stderr="unauthorized")
        with self.assertRaises(subprocess.CalledProcessError):
            resolve_bases(parse_tags(dockerfile()))

    def test_main_prints_complete_env_only_after_success(self):
        expected = {"MAVEN_IMAGE": "example@sha256:" + NEW_DIGEST}
        output = io.StringIO()
        with patch.object(Path, "read_text", return_value=dockerfile()), \
                patch("refresh_bases.resolve_bases", return_value=expected), \
                contextlib.redirect_stdout(output):
            main([])
        self.assertEqual(output.getvalue(), f"MAVEN_IMAGE={expected['MAVEN_IMAGE']}\n")

    def test_main_reports_failures_without_partial_output(self):
        errors = (
            ValueError("invalid digest"),
            OSError("missing Docker"),
            subprocess.CalledProcessError(1, ["docker"], stderr="unauthorized"),
            subprocess.TimeoutExpired(["docker"], 120),
        )
        for error in errors:
            output = io.StringIO()
            with self.subTest(error=error), \
                    patch.object(Path, "read_text", return_value=dockerfile()), \
                    patch("refresh_bases.resolve_bases", side_effect=error), \
                    contextlib.redirect_stdout(output), \
                    self.assertRaisesRegex(SystemExit, "Base image refresh failed"):
                main([])
            self.assertEqual(output.getvalue(), "")
