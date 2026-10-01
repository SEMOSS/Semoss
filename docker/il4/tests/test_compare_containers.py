import argparse
import contextlib
import io
import json
import subprocess
import unittest
from unittest.mock import Mock, patch

import compare_containers as comparison


class ComparisonTests(unittest.TestCase):
    def test_image_references_reject_options_and_shell_content(self):
        for image in ("semoss:test", "ghcr.io/semoss/test@sha256:" + "a" * 64):
            self.assertEqual(comparison.image_reference(image), image)
        for image in ("", "-v", "a b", "$(id)", "repo;true", "repo\nbad"):
            with self.subTest(image=image), self.assertRaises(argparse.ArgumentTypeError):
                comparison.image_reference(image)

    def test_docker_uses_argv_and_reports_failures(self):
        result = Mock(returncode=0, stdout="ok", stderr="")
        with patch("compare_containers.subprocess.run", return_value=result) as run:
            self.assertEqual(comparison.docker("version"), "ok")
            self.assertEqual(run.call_args.args[0], ["docker", "version"])
            self.assertFalse(run.call_args.kwargs.get("shell", False))
        with patch("compare_containers.subprocess.run", return_value=Mock(returncode=1)):
            with self.assertRaisesRegex(RuntimeError, "Docker command failed"):
                comparison.docker("inspect", "fixture")
        for error in (OSError("private diagnostic"), subprocess.TimeoutExpired("docker", 1)):
            with patch("compare_containers.subprocess.run", side_effect=error):
                with self.assertRaisesRegex(RuntimeError, "Docker command unavailable or timed out"):
                    comparison.docker("version")

    def test_waits_for_both_healthy_instances(self):
        states = [
            {"Running": True, "Health": {"Status": "starting"}},
            {"Running": True, "Health": {"Status": "healthy"}},
            {"Running": True, "Health": {"Status": "healthy"}},
        ]
        with patch("compare_containers.docker", side_effect=[json.dumps(s) for s in states]) as docker:
            with patch("compare_containers.time.monotonic", return_value=0), patch("compare_containers.time.sleep"):
                comparison.wait_healthy(["baseline", "candidate"])
            self.assertEqual(docker.call_count, 3)

    def test_wait_fails_on_exit_timeout_or_malformed_state(self):
        for state in ({"Running": False}, {}, [], {"Running": True, "Health": None},
                      {"Running": True, "Health": {}}):
            with self.subTest(state=state), patch("compare_containers.docker", return_value=json.dumps(state)):
                with self.assertRaises(RuntimeError):
                    comparison.wait_healthy(["fixture"])
        with patch("compare_containers.docker", return_value='{"Running":true,"Health":{"Status":"starting"}}'):
            with patch("compare_containers.time.monotonic", side_effect=[0, 301]):
                with self.assertRaisesRegex(RuntimeError, "readiness deadline"):
                    comparison.wait_healthy(["fixture"])

    @contextlib.contextmanager
    def fixture(self, failure=None):
        calls = []

        def fake(*args, **kwargs):
            calls.append(args)
            if failure and failure(args):
                raise RuntimeError("synthetic failure")
            if args[:2] == ("image", "inspect"):
                return "baseline-id" if args[-1] == "baseline:test" else "candidate-id"
            if args[:1] == ("inspect",):
                return "run-id"
            if args[:2] == ("volume", "inspect"):
                return "run-id"
            if args[:2] == ("network", "inspect"):
                return "run-id"
            if args[:1] == ("exec",):
                if args[-1] == "--check":
                    return "BCFIPS" if "baseline" in args[1] else "ACCP"
                if args[-1] == comparison.RUNTIME_PROBE:
                    return '{"runtime":"pass"}'
                if args[-1] == comparison.APPLICATION_PROBE:
                    return json.dumps({"admin_login": "pass", "admin_only_operation": "pass",
                                       "registration_disabled": "pass", "wrong_password_rejected": "pass",
                                       "databases": {key: {"registration": "pass", "aggregate": [[3, 12]],
                                                          "tls": "pass", "readonly_account": "pass"}
                                                     for key in ("postgres", "mariadb")}})
            return ""

        with patch("compare_containers.docker", side_effect=fake), \
             patch("compare_containers.uuid.uuid4", return_value=Mock(hex="run-id")), \
             patch("compare_containers.Path.read_text", return_value="# readiness helper"), \
             patch("compare_containers.start_databases", return_value={"fixtures": "pass"}), \
             patch("compare_containers.verify_read_only_accounts"), \
             patch("compare_containers.wait_healthy"):
            yield calls

    def test_comparison_starts_both_before_probing_and_cleans_owned_resources(self):
        with self.fixture() as calls:
            report = comparison.compare("baseline:test", "candidate:test")
        self.assertEqual(report["status"], "passed")
        self.assertEqual(len(report["results"]), 2)
        starts = [i for i, c in enumerate(calls) if c[:1] == ("start",) and "--attach" not in c]
        probes = [i for i, c in enumerate(calls) if c[:1] == ("exec",)]
        self.assertEqual(len(starts), 2)
        self.assertLess(max(starts), min(probes))
        creates = [c for c in calls if c[:1] == ("create",)]
        self.assertTrue(all("--read-only" in c for c in creates))
        apps = [c for c in creates if "--health-cmd" in c]
        self.assertEqual(len(apps), 2)
        self.assertTrue(all("--network" in c and "semoss-compare-run-id-network" in c for c in apps))
        self.assertTrue(any(c[:2] == ("network", "create") and "--internal" in c for c in calls))
        self.assertEqual(len([c for c in calls if c[:2] == ("network", "rm")]), 1)
        self.assertTrue(all("-p" not in c and "--publish" not in c for c in apps))
        candidate = next(c for c in apps if "candidate:test" in c)
        self.assertIn("SEMOSS_ALLOW_NONVALIDATED_ACCP=true", candidate)
        self.assertEqual(len([c for c in calls if c[:2] == ("volume", "rm")]), 3)
        self.assertEqual(len([c for c in calls if c[:2] == ("rm", "--force")]), 3)

    def test_failures_clean_up_without_hiding_the_failure(self):
        with self.fixture(lambda args: args[:1] == ("exec",)) as calls:
            with self.assertRaisesRegex(RuntimeError, "synthetic failure"):
                comparison.compare("baseline:test", "candidate:test")
        self.assertEqual(len([c for c in calls if c[:2] == ("volume", "rm")]), 3)

    def test_wrong_provider_or_same_image_is_not_a_comparison_pass(self):
        with self.fixture():
            with patch("compare_containers.docker", return_value="same-image"):
                with self.assertRaisesRegex(RuntimeError, "distinct images"):
                    comparison.compare("baseline:test", "candidate:test")
        with self.fixture() as calls:
            original = comparison.docker.side_effect
            def wrong(*args, **kwargs):
                if args[:1] == ("exec",) and args[-1] == "--check":
                    return "wrong provider"
                return original(*args, **kwargs)
            comparison.docker.side_effect = wrong
            with self.assertRaisesRegex(RuntimeError, "provider check"):
                comparison.compare("baseline:test", "candidate:test")

    def test_cleanup_refuses_resources_with_different_ownership(self):
        with patch("compare_containers.docker", return_value="another-run") as docker:
            with self.assertRaisesRegex(RuntimeError, "ownership"):
                comparison.cleanup(["fixture"], ["home"], "run-id")
        self.assertFalse(any(c.args[:1] == ("rm",) for c in docker.call_args_list))
        self.assertFalse(any(c.args[:2] == ("volume", "rm") for c in docker.call_args_list))
        with patch("compare_containers.docker", return_value="another-run") as docker:
            with self.assertRaisesRegex(RuntimeError, "Network ownership mismatch"):
                comparison.cleanup([], [], "run-id", ["network"])
        self.assertFalse(any(c.args[:2] == ("network", "rm") for c in docker.call_args_list))

    def test_application_results_must_cover_all_acceptance_gates(self):
        with self.fixture():
            valid = json.loads(comparison.docker("exec", "fixture", comparison.APPLICATION_PROBE))
        invalid = [None, {}, {**valid, "admin_login": "fail"},
                   {**valid, "databases": {}},
                   {**valid, "databases": {"postgres": None, "mariadb": None}},
                   {**valid, "databases": {
                       "postgres": {**valid["databases"]["postgres"], "aggregate": [[0, 0]]},
                       "mariadb": valid["databases"]["mariadb"]}}]
        for document in invalid:
            with self.subTest(document=document), self.fixture():
                original = comparison.docker.side_effect

                def fake(*args, **kwargs):
                    if args[-1] == comparison.APPLICATION_PROBE:
                        return json.dumps(document)
                    return original(*args, **kwargs)

                comparison.docker.side_effect = fake
                with self.assertRaises(RuntimeError):
                    comparison.compare("baseline:test", "candidate:test")

    def test_main_outputs_report_or_explicit_failure(self):
        argv = ["--baseline", "baseline:test", "--candidate", "candidate:test"]
        with patch("compare_containers.compare", return_value={"status": "passed"}):
            with contextlib.redirect_stdout(io.StringIO()) as out:
                self.assertEqual(comparison.main(argv), 0)
            self.assertEqual(json.loads(out.getvalue())["status"], "passed")
        with patch("compare_containers.compare", side_effect=RuntimeError("test failure")):
            with contextlib.redirect_stderr(io.StringIO()) as err:
                self.assertEqual(comparison.main(argv), 1)
            self.assertIn("test failure", err.getvalue())


if __name__ == "__main__":
    unittest.main()
