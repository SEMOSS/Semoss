"""Hermetic tests: no daemon, deployment, or real baseline files are used."""

from contextlib import ExitStack
import copy
import io
import json
import subprocess
import unittest
from unittest.mock import mock_open, patch

import audit_runtime


IMAGE_ID = "sha256:" + "a" * 64
EXPECTED = "example/semoss@sha256:" + "b" * 64
SECRET = "never-print-private-secret"
TMPFS = ["/tmp", "/opt/tomcat/temp", "/opt/tomcat/work", "/opt/tomcat/logs"]
TLS = ["/run/secrets/server.bcfks", "/run/secrets/server.password",
       "/run/secrets/readiness-ca.pem"]


def container():
    return {
        "Image": IMAGE_ID,
        "Config": {"Image": SECRET, "User": "10001:10001", "Env": [SECRET],
                   "Hostname": SECRET, "Healthcheck": {"Test": ["CMD", SECRET]}},
        "State": {"Running": True, "Health": {"Status": "healthy",
                                             "Log": [{"Output": SECRET}]}},
        "HostConfig": {
            "Privileged": False, "CapDrop": ["ALL"], "CapAdd": None,
            "SecurityOpt": ["no-new-privileges:true"], "ReadonlyRootfs": True,
            "Memory": 8589934592, "NanoCpus": 4000000000, "CpuQuota": 0,
            "CpuPeriod": 0, "PidsLimit": 512, "PidMode": "", "IpcMode": "private",
            "NetworkMode": "project_default", "Devices": [], "DeviceRequests": None,
            "DeviceCgroupRules": None,
            "Tmpfs": {path: "rw,noexec,nosuid,nodev,uid=10001" for path in TMPFS},
            "LogConfig": {"Type": "local", "Config": {"max-size": "10m", "max-file": "3"}},
            "PortBindings": {"8443/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8443"}]},
        },
        "Mounts": [
            {"Type": "volume", "Source": "/" + SECRET, "Destination": "/opt/semosshome",
             "RW": True},
            *[{"Type": "bind", "Source": "/" + SECRET, "Destination": path, "RW": False}
              for path in TLS],
        ],
    }


class AuditTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.run = self.stack.enter_context(patch.object(audit_runtime.subprocess, "run"))
        self.stdout = self.stack.enter_context(patch.object(audit_runtime.sys, "stdout", io.StringIO()))
        self.stderr = self.stack.enter_context(patch.object(audit_runtime.sys, "stderr", io.StringIO()))
        self.open = self.stack.enter_context(patch("builtins.open", mock_open()))
        self.args = ["--container", "semoss-dev", "--expected-image", EXPECTED]
        self.data = container()
        self.image = {"Id": IMAGE_ID, "RepoDigests": [EXPECTED]}

    def invoke(self, *extra):
        self.stdout.seek(0)
        self.stdout.truncate()
        self.stderr.seek(0)
        self.stderr.truncate()
        self.run.side_effect = [
            subprocess.CompletedProcess([], 0, json.dumps([self.data]), SECRET),
            subprocess.CompletedProcess([], 0, json.dumps([self.image]), SECRET),
        ]
        code = audit_runtime.main(self.args + list(extra))
        output = self.stdout.getvalue()
        self.assertNotIn(SECRET, output + self.stderr.getvalue())
        return code, json.loads(output) if output else None

    def test_success_safe_snapshot_and_read_only_commands(self):
        code, report = self.invoke("--strict")
        self.assertEqual(code, 0)
        self.assertEqual(report["schema_version"], 1)
        self.assertEqual(report["image_id"], IMAGE_ID)
        self.assertTrue(all(report["checks"].values()))
        self.assertEqual(report["drift"], {"compared": False, "detected": False, "fields": []})
        self.assertIn("metadata", report["scope"])
        self.assertNotIn("127.0.0.1", self.stdout.getvalue())
        self.assertNotIn("HostIp", self.stdout.getvalue())
        self.assertNotIn("Env", self.stdout.getvalue())
        self.assertNotIn("Healthcheck", self.stdout.getvalue())
        self.assertEqual([call.args[0] for call in self.run.call_args_list],
                         [["docker", "container", "inspect", "semoss-dev"],
                          ["docker", "image", "inspect", EXPECTED]])
        for call in self.run.call_args_list:
            self.assertEqual(call.kwargs["timeout"], 10)
            self.assertFalse(call.kwargs.get("shell", False))
        self.open.assert_not_called()

    def test_each_control_reports_and_strict_fails(self):
        cases = [
            ("image_identity", ("Image",), "sha256:" + "c" * 64),
            ("running", ("State", "Running"), False),
            ("non_root", ("Config", "User"), "0:0"),
            ("non_privileged", ("HostConfig", "Privileged"), True),
            ("capabilities", ("HostConfig", "CapAdd"), ["NET_ADMIN"]),
            ("capabilities", ("HostConfig", "CapDrop"), []),
            ("no_new_privileges", ("HostConfig", "SecurityOpt"), []),
            ("no_new_privileges", ("HostConfig", "SecurityOpt"),
             ["no-new-privileges:true", "no-new-privileges:false"]),
            ("read_only_root", ("HostConfig", "ReadonlyRootfs"), False),
            ("resource_limits", ("HostConfig", "Memory"), 0),
            ("resource_limits", ("HostConfig", "NanoCpus"), 0),
            ("resource_limits", ("HostConfig", "PidsLimit"), -1),
            ("isolated_namespaces", ("HostConfig", "PidMode"), "host"),
            ("isolated_namespaces", ("HostConfig", "IpcMode"), "shareable"),
            ("isolated_namespaces", ("HostConfig", "NetworkMode"), "container:private"),
            ("isolated_namespaces", ("HostConfig", "NetworkMode"), "host"),
            ("no_devices_or_runtime_sockets", ("HostConfig", "Devices"), [{"PathOnHost": SECRET}]),
            ("no_devices_or_runtime_sockets", ("HostConfig", "DeviceRequests"), [{"Driver": SECRET}]),
            ("no_devices_or_runtime_sockets", ("HostConfig", "DeviceCgroupRules"), ["c *:* rwm"]),
            ("hardened_tmpfs", ("HostConfig", "Tmpfs"), {}),
            ("bounded_local_logging", ("HostConfig", "LogConfig", "Type"), "json-file"),
            ("bounded_local_logging", ("HostConfig", "LogConfig", "Config", "max-size"), "0"),
            ("bounded_local_logging", ("HostConfig", "LogConfig", "Config", "max-file"), "-1"),
            ("healthcheck", ("Config", "Healthcheck"), {"Test": ["NONE"]}),
            ("healthy", ("State", "Health"), {"Status": "unhealthy", "Log": [SECRET]}),
        ]
        for name, keys, value in cases:
            with self.subTest(check=name, keys=keys, value=value):
                self.data = container()
                target = self.data
                for key in keys[:-1]:
                    target = target[key]
                target[keys[-1]] = value
                for flags, expected_code in [((), 0), (("--strict",), 1)]:
                    code, report = self.invoke(*flags)
                    self.assertEqual(code, expected_code)
                    self.assertFalse(report["checks"][name])

    def test_user_is_numeric_and_not_root(self):
        for user in ("", "root", "semoss", "0", "000:10001", "10001:0", "10001:root", "-1"):
            self.data["Config"]["User"] = user
            self.assertFalse(self.invoke()[1]["checks"]["non_root"])
        for user in ("10001", "10001:10001"):
            self.data["Config"]["User"] = user
            self.assertTrue(self.invoke()[1]["checks"]["non_root"])

    def test_digest_identity_requires_repo_digest(self):
        self.image["RepoDigests"] = ["other/repo@sha256:" + "b" * 64]
        self.assertFalse(self.invoke()[1]["checks"]["image_identity"])
        self.image["RepoDigests"] = None
        self.assertFalse(self.invoke()[1]["checks"]["image_identity"])

    def test_cpu_quota_and_optional_publication(self):
        self.data["HostConfig"].update(NanoCpus=0, CpuQuota=200000, CpuPeriod=100000,
                                       PortBindings=None, SecurityOpt=["no-new-privileges"])
        self.assertEqual(self.invoke("--strict")[0], 0)
        self.data["HostConfig"]["PortBindings"] = {
            "8443/tcp": [{"HostIp": "0.0.0.0", "HostPort": "8443"}]}
        self.assertEqual(self.invoke("--strict")[0], 0)
        self.assertEqual(self.invoke()[1]["snapshot"]["publication"]["non_loopback"], 1)

    def test_mount_controls(self):
        cases = [
            ("writable_home", lambda d: d["Mounts"][0].update(RW=False)),
            ("writable_home", lambda d: d["Mounts"].pop(0)),
            ("readonly_tls_mounts", lambda d: d["Mounts"][1].update(RW=True)),
            ("readonly_tls_mounts", lambda d: d["Mounts"].pop()),
            ("approved_writable_mounts", lambda d: d["Mounts"].append(
                {"Type": "bind", "Source": "/" + SECRET, "Destination": "/extra", "RW": True})),
            ("no_devices_or_runtime_sockets", lambda d: d["Mounts"].append(
                {"Type": "bind", "Source": "/var/run/docker.sock", "Destination": "/socket", "RW": False})),
            ("no_devices_or_runtime_sockets", lambda d: d["Mounts"].append(
                {"Type": "bind", "Source": "/run", "Destination": "/runtime", "RW": False})),
            ("no_devices_or_runtime_sockets", lambda d: d["Mounts"].append(
                {"Type": "bind", "Source": "/run/user/10001", "Destination": "/runtime", "RW": False})),
            ("no_devices_or_runtime_sockets", lambda d: d["Mounts"].append(
                {"Type": "bind", "Source": "/data/renamed", "Destination": "/docker.sock", "RW": False})),
            ("no_devices_or_runtime_sockets", lambda d: d["Mounts"].append(
                {"Type": "bind", "Source": "/dev/fuse", "Destination": "/device", "RW": False})),
            ("hardened_tmpfs", lambda d: d["HostConfig"]["Tmpfs"].update({"/tmp": "rw,nosuid,nodev"})),
            ("hardened_tmpfs", lambda d: d["HostConfig"]["Tmpfs"].update({"/tmp": "rw,noexec,nosuid,nodev,exec"})),
            ("hardened_tmpfs", lambda d: d["HostConfig"]["Tmpfs"].update({"/tmp": "ro,noexec,nosuid,nodev"})),
            ("hardened_tmpfs", lambda d: d["Mounts"].append(
                {"Type": "bind", "Source": "/" + SECRET, "Destination": "/tmp", "RW": True})),
            ("approved_writable_mounts", lambda d: d["HostConfig"]["Tmpfs"].update({"/extra": "rw"})),
            ("writable_home", lambda d: d["HostConfig"]["Tmpfs"].update({"/opt/semosshome": "rw"})),
        ]
        for name, mutate in cases:
            with self.subTest(check=name):
                self.data = container()
                mutate(self.data)
                self.assertFalse(self.invoke()[1]["checks"][name])

    def test_valid_alternate_metadata(self):
        self.data["Mounts"][0]["Type"] = "bind"
        self.data["Mounts"].append({"Type": "bind", "Source": "/config/ca.pem",
                                    "Destination": "/run/secrets/extra.pem", "RW": False})
        self.data["Mounts"].extend({"Type": "tmpfs", "Destination": path, "RW": True}
                                   for path in TMPFS)
        self.data["HostConfig"]["Tmpfs"]["/tmp"] = "rw,exec,noexec,suid,nosuid,dev,nodev"
        self.data["HostConfig"]["PortBindings"] = {
            "8443/tcp": [dict(HostIp="", HostPort="8443"), dict(HostIp="::1", HostPort="8443")],
            "8080/tcp": None,
        }
        code, report = self.invoke("--strict")
        self.assertEqual(code, 0)
        self.assertEqual(report["snapshot"]["publication"], {"loopback": 1, "non_loopback": 1})

    def test_missing_optional_controls_are_violations(self):
        del self.data["Config"]["Healthcheck"]
        del self.data["State"]["Health"]
        self.data["HostConfig"]["PidsLimit"] = None
        code, report = self.invoke("--strict")
        self.assertEqual(code, 1)
        for name in ("healthcheck", "healthy", "resource_limits"):
            self.assertFalse(report["checks"][name])

    def test_invalid_shapes_are_safe_errors(self):
        cases = [
            (("Config",), []), (("State",), None), (("Image",), SECRET),
            (("State", "Running"), "true"), (("HostConfig", "Memory"), True),
            (("HostConfig", "CapDrop"), ["ALL", 1]),
            (("HostConfig", "Tmpfs"), {"/tmp": []}),
            (("HostConfig", "PortBindings"), {"8443/tcp": [None]}),
            (("HostConfig", "LogConfig", "Config"), []),
            (("Config", "Healthcheck", "Test"), "CMD"),
            (("State", "Health", "Status"), []), (("Mounts",), [None]),
            (("Mounts",), [{"Destination": "/tmp"}]),
            (("HostConfig", "Tmpfs"), {"relative": "rw"}),
        ]
        for keys, value in cases:
            with self.subTest(keys=keys):
                self.data = container()
                target = self.data
                for key in keys[:-1]:
                    target = target[key]
                target[keys[-1]] = value
                self.assertEqual(self.invoke()[0], 2)
                self.assertEqual(self.stdout.getvalue(), "")
        for value in ([], [1], SECRET):
            self.data = container()
            self.image["RepoDigests"] = value
            expected_code = 0 if value == [] else 2
            self.assertEqual(self.invoke()[0], expected_code)
        self.image = {"Id": IMAGE_ID, "RepoDigests": [EXPECTED]}
        for destination in ("/opt/semosshome", "relative", "/opt/../opt/semosshome"):
            self.data = container()
            self.data["Mounts"].append({"Type": "volume", "Destination": destination, "RW": True})
            self.assertEqual(self.invoke()[0], 2)

    def test_nullable_controls_do_not_crash(self):
        for key in ("CapDrop", "CapAdd", "SecurityOpt", "Tmpfs"):
            self.data["HostConfig"][key] = None
        self.data["Config"]["Healthcheck"] = None
        self.data["State"]["Health"] = None
        self.assertEqual(self.invoke("--strict")[0], 1)

    def test_compose_null_devices_and_normal_volume_storage(self):
        self.data["HostConfig"].update(Devices=None, DeviceRequests=None, DeviceCgroupRules=None)
        self.data["Mounts"][0]["Source"] = "/var/lib/docker/volumes/project_home/_data"
        code, report = self.invoke("--strict")
        self.assertEqual(code, 0)
        self.assertTrue(report["checks"]["no_devices_or_runtime_sockets"])
        self.assertNotIn("/var/lib/docker", self.stdout.getvalue())
        self.data["Mounts"][0]["Type"] = "bind"
        self.assertFalse(self.invoke()[1]["checks"]["no_devices_or_runtime_sockets"])
        self.data["Mounts"][0]["Type"] = "volume"
        for source in ("/run", "/var/lib/docker", "/var/run/docker.sock", "/dev/fuse"):
            self.data["Mounts"][0]["Source"] = source
            self.assertFalse(self.invoke()[1]["checks"]["no_devices_or_runtime_sockets"])

    def test_missing_or_malformed_device_collections_never_pass(self):
        for key in ("Devices", "DeviceRequests", "DeviceCgroupRules"):
            for value in ("absent", {}, False, SECRET, [None]):
                with self.subTest(key=key, value=value):
                    self.data = container()
                    if value == "absent":
                        del self.data["HostConfig"][key]
                    else:
                        self.data["HostConfig"][key] = value
                    self.assertEqual(self.invoke()[0], 2)
            for value in (None, []):
                self.data = container()
                self.data["HostConfig"][key] = value
                self.assertEqual(self.invoke("--strict")[0], 0)
            self.data = container()
            self.data["HostConfig"][key] = ["c *:* rwm"] if key == "DeviceCgroupRules" else [{}]
            self.assertFalse(self.invoke()[1]["checks"]["no_devices_or_runtime_sockets"])

    def test_absent_metadata_never_assumes_safe_configuration(self):
        cases = [("Config", "User"), ("State", "Running"),
                 *[("HostConfig", key) for key in
                   ("Privileged", "ReadonlyRootfs", "CapDrop", "CapAdd", "SecurityOpt",
                    "Memory", "NanoCpus", "CpuQuota", "CpuPeriod", "PidsLimit",
                    "PidMode", "IpcMode", "NetworkMode", "Tmpfs", "PortBindings", "LogConfig")]]
        for parent, key in cases:
            with self.subTest(parent=parent, key=key):
                self.data = container()
                del self.data[parent][key]
                self.assertEqual(self.invoke()[0], 2)
        self.data = container()
        del self.image["RepoDigests"]
        self.assertEqual(self.invoke()[0], 2)
        self.image["RepoDigests"] = [EXPECTED]
        del self.data["Mounts"][0]["Source"]
        self.assertEqual(self.invoke()[0], 2)
        for source in ("", "relative"):
            self.data["Mounts"][0]["Source"] = source
            self.assertEqual(self.invoke()[0], 2)

    def test_inspect_errors_and_invalid_json_never_leak(self):
        outcomes = [
            subprocess.TimeoutExpired(SECRET, 10, output=SECRET, stderr=SECRET),
            OSError(SECRET),
            subprocess.CompletedProcess([], 1, SECRET, SECRET),
            *[subprocess.CompletedProcess([], 0, body, SECRET)
              for body in (SECRET, "[]", "{}", "[null]", "[{},{}]",
                           '[{"Image":1,"Image":2}]', "[NaN]", "[" * 2000)],
        ]
        for result in outcomes:
            with self.subTest(result=type(result).__name__):
                self.run.side_effect = result if isinstance(result, Exception) else [result]
                self.assertEqual(audit_runtime.main(self.args), 2)
                self.assertNotIn(SECRET, self.stderr.getvalue() + self.stdout.getvalue())

    def test_invalid_arguments_never_echo_input(self):
        for args in ([], ["--container", SECRET], self.args + ["--unknown", SECRET],
                     ["--container=", "--expected-image", EXPECTED],
                     ["--container=--help", "--expected-image", EXPECTED],
                     ["--container", "bad name", "--expected-image", EXPECTED],
                     ["--container", "ok", "--expected-image", "repo:latest"],
                     ["--container", "ok", "--expected-image", "repo:tag@sha256:" + "a" * 64],
                     ["--container", "ok", "--expected-image", EXPECTED.upper()]):
            self.assertEqual(audit_runtime.main(args), 2)
            self.assertNotIn(SECRET, self.stderr.getvalue())
        self.run.assert_not_called()
        self.assertEqual(audit_runtime.main(["--help"]), 0)

    def test_registry_port_and_nested_repository(self):
        for ref in ("localhost:5000/team/semoss", "registry.example.com/team/sub/semoss",
                    "semoss", "team/semoss_runtime"):
            self.args[-1] = ref + "@sha256:" + "b" * 64
            self.image["RepoDigests"] = [self.args[-1]]
            self.assertEqual(self.invoke("--strict")[0], 0)

    def test_baseline_drift_and_strict(self):
        original = self.invoke()[1]
        self.open.return_value.read.return_value = json.dumps(original)
        self.assertEqual(self.invoke("--baseline", "baseline.json", "--strict")[0], 0)
        self.data["HostConfig"]["Memory"] *= 2
        code, report = self.invoke("--baseline", "baseline.json")
        self.assertEqual(code, 0)
        self.assertEqual(report["drift"], {
            "compared": True, "detected": True, "fields": ["resources.memory_bytes"]})
        self.assertEqual(self.invoke("--baseline", "baseline.json", "--strict")[0], 1)
        self.open.return_value.read.return_value = json.dumps(report)
        self.assertEqual(self.invoke("--baseline", "baseline.json", "--strict")[0], 0)

    def test_weak_baseline_does_not_relax_checks(self):
        self.data["HostConfig"]["Privileged"] = True
        previous = self.invoke()[1]
        self.open.return_value.read.return_value = json.dumps(previous)
        code, report = self.invoke("--baseline", "baseline.json", "--strict")
        self.assertEqual(code, 1)
        self.assertFalse(report["drift"]["detected"])
        self.assertFalse(report["checks"]["non_privileged"])

    def test_invalid_baseline_is_error(self):
        original = self.invoke()[1]
        missing = copy.deepcopy(original)
        del missing["snapshot"]["resources"]
        wrong_type = copy.deepcopy(original)
        wrong_type["snapshot"]["resources"]["memory_bytes"] = SECRET
        invalid_drift = copy.deepcopy(original)
        invalid_drift["drift"] = {"compared": True, "detected": True, "fields": [SECRET]}
        invalid_snapshot = copy.deepcopy(original)
        invalid_snapshot["snapshot"]["checks"]["running"] = False
        for value in ("{}", "[]", SECRET, '{"schema_version":NaN}',
                      json.dumps(dict(original, schema_version=2)),
                      json.dumps(dict(original, schema_version=True)),
                      json.dumps(missing), json.dumps(wrong_type),
                      json.dumps(invalid_drift), json.dumps(invalid_snapshot)):
            self.open.return_value.read.return_value = value
            self.assertEqual(self.invoke("--baseline", SECRET)[0], 2)
        self.open.side_effect = OSError(SECRET)
        self.assertEqual(self.invoke("--baseline", SECRET)[0], 2)


if __name__ == "__main__":
    unittest.main()
