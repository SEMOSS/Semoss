import json
import re
import subprocess
import unittest
from unittest.mock import patch

import database_fixtures as fixtures


class Docker:
    def __init__(self):
        self.calls = []
        self.failure = None
        self.not_ready = 0
        self.bad_permissions = False

    def __call__(self, *args, input=None, timeout=120):
        self.calls.append((args, input, timeout))
        if self.failure and self.failure(args, input):
            raise RuntimeError("sensitive diagnostic must not escape")
        if args[0] == "exec":
            if "current_setting('server_version')" in (input or ""):
                if self.not_ready:
                    self.not_ready -= 1
                    return "15.19|on|f\n"
                return "15.19 (Debian 15.19-1.pgdg13+1)|on|t\n"
            if "@@require_secure_transport" in (input or ""):
                return "11.8.6-MariaDB\t1\t0\t1\n"
            if "has_table_privilege" in (input or ""):
                return "f\n" if self.bad_permissions else "t\n"
            if "information_schema.TABLE_PRIVILEGES" in (input or ""):
                return "0\n" if self.bad_permissions else "1\n"
        return ""


class DatabaseFixtureTests(unittest.TestCase):
    def setUp(self):
        self.docker = Docker()
        self.containers = []
        self.volumes = []

    def start(self):
        return fixtures.start_databases(self.docker, "comparison-test", "run-123",
                                        "internal-network", "tls-volume",
                                        self.containers, self.volumes)

    def test_both_tls_databases_use_pins_and_owned_resources(self):
        result = self.start()
        self.assertEqual(self.containers, ["comparison-test-postgres", "comparison-test-mariadb"])
        self.assertEqual(self.volumes, ["comparison-test-postgres-data", "comparison-test-mariadb-data"])
        self.assertEqual(result["postgres"]["image"], fixtures.POSTGRES_IMAGE)
        self.assertEqual(result["mariadb"]["image"], fixtures.MARIA_IMAGE)
        self.assertEqual(result["postgres"]["version"], "15.19 (Debian 15.19-1.pgdg13+1)")
        self.assertEqual(result["mariadb"]["version"], "11.8.6-MariaDB")
        for engine, metadata in result.items():
            self.assertEqual(metadata["container"], "comparison-test-" + engine)
            self.assertEqual(metadata["database"], "semoss_validation")
            self.assertEqual(metadata["role"], "semoss_reader")
            self.assertTrue(metadata["read_only_verified"])
        self.assertNotIn("password", json.dumps(result).lower())
        creation = [args for args, _, _ in self.docker.calls if args[0] == "create"]
        self.assertEqual(len(creation), 2)
        for args in creation:
            joined = " ".join(args)
            for value in ("--pull=never", "--memory=512m", "--cpus=1",
                          "--security-opt=no-new-privileges:true",
                          "org.semoss.comparison=run-123",
                          "tls-volume:/run/secrets:ro", "--log-driver=local",
                          "--log-opt=max-size=10m", "--log-opt=max-file=3"):
                self.assertIn(value, joined)
            self.assertNotIn("--platform", joined)
            self.assertNotIn("--publish", joined)
            self.assertNotIn("--cap-drop", joined)
            self.assertIn("--network=internal-network", joined)
        self.assertIn("--network-alias=postgres", creation[0])
        self.assertIn("--network-alias=mariadb", creation[1])
        volumes = [args for args, _, _ in self.docker.calls if args[:2] == ("volume", "create")]
        self.assertEqual(len(volumes), 2)
        self.assertTrue(all("org.semoss.comparison=run-123" in args for args in volumes))

    def test_sql_and_commands_never_contain_external_secret_values(self):
        self.start()
        commands = "\n".join(" ".join(args) for args, _, _ in self.docker.calls)
        inputs = "\n".join(value for _, value, _ in self.docker.calls if value)
        self.assertIn("POSTGRES_PASSWORD_FILE=/run/secrets/db-root.password", commands)
        self.assertIn("MARIADB_ROOT_PASSWORD_FILE=/run/secrets/db-root.password", commands)
        self.assertIn("MARIADB_ROOT_HOST=localhost", commands)
        self.assertNotIn("MYSQL_PWD=", commands)
        self.assertNotIn("POSTGRES_PASSWORD=", commands)
        self.assertNotIn("--password=", commands)
        self.assertIn("hostssl semoss_validation semoss_reader all scram-sha-256", commands)
        self.assertIn("host all all all reject", commands)
        self.assertIn("local all postgres peer", commands)
        self.assertIn("--require-secure-transport=ON", commands)
        self.assertIn("pg_read_file('/run/secrets/db-reader.password')", inputs)
        self.assertIn("NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS", inputs)
        self.assertIn("REVOKE ALL ON DATABASE semoss_validation FROM PUBLIC", inputs)
        self.assertIn("REQUIRE SSL", inputs)
        self.assertIn("GRANT SELECT", inputs)
        self.assertIn("INSERT INTO", inputs)
        self.assertIn("(1,2),(2,4),(3,6)", inputs)
        self.assertIn("^([0-9a-f]{2}){16,64}$", inputs)
        self.assertIn("*[!0-9a-f]*", commands + inputs)
        self.assertIn("ON_ERROR_STOP=1", commands)
        self.assertIn("protocol=socket", commands)
        self.assertIn("umask 077", commands)

    def test_resource_tracking_survives_start_failure(self):
        self.docker.failure = lambda args, _: args[0] == "start"
        with self.assertRaisesRegex(RuntimeError, "fixture") as error:
            self.start()
        self.assertNotIn("sensitive", str(error.exception))
        self.assertEqual(self.containers, ["comparison-test-postgres"])
        self.assertEqual(self.volumes, ["comparison-test-postgres-data"])

    def test_resource_tracking_does_not_claim_failed_creations(self):
        for operation, expected_volumes in (("volume", []), ("create", ["comparison-test-postgres-data"])):
            with self.subTest(operation=operation):
                self.setUp()
                self.docker.failure = lambda args, _, operation=operation: args[0] == operation
                with self.assertRaises(RuntimeError):
                    self.start()
                self.assertEqual(self.containers, [])
                self.assertEqual(self.volumes, expected_volumes)

    @patch("database_fixtures.time.sleep")
    def test_readiness_retries_initializing_server(self, sleep):
        self.docker.not_ready = 1
        self.start()
        sleep.assert_called_once()

    @patch("database_fixtures.time.sleep")
    @patch("database_fixtures.time.monotonic")
    def test_readiness_deadline_is_bounded_and_sanitized(self, clock, sleep):
        clock.side_effect = [0, 0, 179, 181]
        self.docker.failure = lambda args, _: args[0] == "exec"
        with self.assertRaisesRegex(RuntimeError, "180 seconds") as error:
            self.start()
        self.assertNotIn("sensitive", str(error.exception))
        for args, _, timeout in self.docker.calls:
            if args[0] == "exec":
                self.assertLessEqual(timeout, 10)

    def test_setup_failure_is_sanitized_and_resources_still_tracked(self):
        self.docker.failure = lambda args, data: data is not None and "CREATE TABLE" in data
        with self.assertRaisesRegex(RuntimeError, "fixture") as error:
            self.start()
        self.assertNotIn("sensitive", str(error.exception))
        self.assertEqual(len(self.containers), 2)
        self.assertEqual(len(self.volumes), 2)

    def test_read_only_verification_is_exposed_and_fails_closed(self):
        result = self.start()
        self.assertEqual(fixtures.verify_read_only_accounts(self.docker, result),
                         {"postgres": True, "mariadb": True})
        self.docker.bad_permissions = True
        with self.assertRaisesRegex(RuntimeError, "read-only"):
            fixtures.verify_read_only_accounts(self.docker, result)

    def test_maria_privilege_failure_is_not_skipped(self):
        result = self.start()
        original = self.docker

        def wrong(*args, input=None, timeout=120):
            if "information_schema.TABLE_PRIVILEGES" in (input or ""):
                return "0\n"
            return original(*args, input=input, timeout=timeout)

        with self.assertRaisesRegex(RuntimeError, "mariadb.*read-only"):
            fixtures.verify_read_only_accounts(wrong, result)

    def test_privilege_diagnostic_is_sanitized(self):
        result = self.start()
        self.docker.failure = lambda args, data: data is not None and "has_table_privilege" in data
        with self.assertRaisesRegex(RuntimeError, "read-only verification failed") as error:
            fixtures.verify_read_only_accounts(self.docker, result)
        self.assertNotIn("sensitive", str(error.exception))

    def test_shell_syntax_and_password_validation(self):
        for script in (fixtures.POSTGRES_START, fixtures.MARIA_CONFIG, fixtures.MARIA_INIT):
            with self.subTest(script=script[:30]):
                result = subprocess.run(["sh", "-n"], input=script, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
        pg_pattern = re.search(r"IF secret !~ '([^']+)'", fixtures.POSTGRES_INIT).group(1)
        cases = [("", False), ("aa", False), ("a" * 31, False), ("a" * 32, True),
                 ("abcd0123" * 8, True), ("b" * 127, False), ("f" * 128, True),
                 ("c" * 130, False), ("AB" * 32, False), ("a" * 32 + "'", False),
                 ("a" * 32 + "\nembedded", False)]
        for password, accepted in cases:
            with self.subTest(length=len(password), accepted=accepted):
                result = subprocess.run(
                    ["sh", "-ec", "set -eu\npassword=$(cat)\n" + fixtures.HEX_CHECK],
                    input=password, capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, accepted)
                self.assertEqual(bool(re.fullmatch(pg_pattern, password)), accepted)
                self.assertEqual(result.stdout, "")
                self.assertEqual(result.stderr, "")

    def test_postgres_accepts_newline_terminated_password_files(self):
        self.assertIn(
            "rtrim(pg_read_file('/run/secrets/db-reader.password'), E'\\r\\n')",
            fixtures.POSTGRES_INIT)
        pattern = re.search(r"IF secret !~ '([^']+)'", fixtures.POSTGRES_INIT).group(1)
        for ending in ("", "\n", "\r\n"):
            self.assertIsNotNone(re.fullmatch(pattern, ("a" * 48 + ending).rstrip("\r\n")))
        self.assertIsNone(re.fullmatch(pattern, ("a" * 24 + "\n" + "b" * 24).rstrip("\r\n")))

    def test_invalid_resource_names_rejected_before_docker(self):
        for name in (None, "", "-invalid", "name;injection", "name/invalid", "name\ninvalid"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                fixtures.start_databases(self.docker, name, "run", "network", "tls",
                                          self.containers, self.volumes)
        self.assertEqual(self.docker.calls, [])


if __name__ == "__main__":
    unittest.main()
