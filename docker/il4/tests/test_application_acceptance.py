import json
import contextlib
import io
import unittest
from unittest.mock import Mock, mock_open, patch

import application_acceptance as acceptance


def pixel(value):
    return {"pixelReturn": [{"output": value, "operationType": ["TEST"]}]}


def table(values):
    return pixel({"data": {"values": values}})


class ApplicationAcceptanceTests(unittest.TestCase):
    def test_registration_uses_verified_tls_and_escaped_passwords(self):
        for database in ("postgres", "mariadb"):
            expression = acceptance.registration_expression(database, 'quoted"password')
            self.assertIn(json.dumps('quoted"password'), expression)
            self.assertIn("verify-full", expression)
            self.assertIn("/run/secrets/localhost.pem", expression)
            self.assertNotIn("trustServerCertificate=true", expression)
            self.assertIn('"measurements.id": ["id", "amount"]', expression)
        with self.assertRaises(ValueError):
            acceptance.registration_expression("unknown", "password")

    def test_admin_bootstrap_login_and_privilege_check(self):
        client = Mock()
        client.request.return_value = {"success": True}
        client.pixel.side_effect = [pixel(2), pixel(True)]
        disable = Mock()
        credentials = {"username": "test", "password": "secret"}
        acceptance.bootstrap_admin(client, credentials, disable)
        client.login.assert_called_once_with(credentials)
        disable.assert_called_once_with()
        self.assertEqual(client.pixel.call_args.args[0], "AdminReloadSocialProperties();")

    def test_admin_rejects_failed_bootstrap_and_missing_admin_privilege(self):
        for response in ({}, None, {"success": False}):
            client = Mock()
            client.request.return_value = response
            with self.subTest(response=response), self.assertRaises(RuntimeError):
                acceptance.bootstrap_admin(client, {"username": "test"}, Mock())
        client = Mock()
        client.request.side_effect = [{"success": True}, {"success": False}]
        with self.assertRaises(RuntimeError):
            acceptance.bootstrap_admin(client, {"username": "test"}, Mock())
        for outputs in ([pixel(7)], [pixel(2), pixel(False)]):
            client = Mock()
            client.request.return_value = {"success": True}
            client.pixel.side_effect = outputs
            with self.assertRaises(RuntimeError):
                acceptance.bootstrap_admin(client, {"username": "test"}, Mock())

    def test_authentication_rejection_is_not_a_generic_server_failure(self):
        for response in ({"success": False}, {"success": "false"}):
            client = Mock()
            client.request.return_value = response
            acceptance.check_wrong_password(client, {"username": "test"})
        for error in ("/api/auth/login: HTTP 401: denied", "/api/auth/login: HTTP 403: denied"):
            client = Mock()
            client.request.side_effect = RuntimeError(error)
            acceptance.check_wrong_password(client, {"username": "test"})
        for response in ({}, {"success": True}, None):
            client = Mock()
            client.request.return_value = response
            with self.assertRaises(RuntimeError):
                acceptance.check_wrong_password(client, {"username": "test"})
        client = Mock()
        client.request.side_effect = RuntimeError("HTTP 500: unavailable")
        with self.assertRaises(RuntimeError):
            acceptance.check_wrong_password(client, {"username": "test"})

    def test_live_registration_query_encryption_and_readonly_privileges(self):
        for database, tls, grants in (
            ("postgres", [[True]], [[False, False, False, False]]),
            ("mariadb", [["Ssl_cipher", "TLS_AES_256_GCM_SHA384"]],
             [["GRANT USAGE ON *.* TO `semoss_reader`@`%` REQUIRE SSL"],
              ["GRANT SELECT ON `semoss_validation`.`measurements` TO `semoss_reader`@`%`"]]),
        ):
            client = Mock()
            client.pixel.side_effect = [
                pixel({"database_id": "fixture-engine", "database_global": False}),
                table([[3, 12]]), table(tls), table(grants),
            ]
            result = acceptance.check_database(client, database, "synthetic")
            self.assertEqual(result["aggregate"], [[3, 12]])
            self.assertEqual(result["tls"], "pass")
            self.assertEqual(result["readonly_account"], "pass")
            self.assertEqual(client.pixel.call_count, 4)

    def test_database_failures_cannot_be_reported_as_passes(self):
        invalid = [
            [pixel({})],
            [pixel({"database_id": "fixture-engine", "database_global": True})],
            [pixel({"database_id": "fixture-engine", "database_global": False}), table([[0, 0]])],
            [pixel({"database_id": "fixture-engine", "database_global": False}),
             table([[3, 12]]), table([[False]])],
            [pixel({"database_id": "fixture-engine", "database_global": False}),
             table([[3, 12]]), table([[True]]), table([[True, False, False, False]])],
        ]
        for outputs in invalid:
            client = Mock()
            client.pixel.side_effect = outputs
            with self.subTest(outputs=outputs), self.assertRaises(RuntimeError):
                acceptance.check_database(client, "postgres", "never-print")
        for grants in ([], [["GRANT ALL PRIVILEGES ON *.* TO reader"]]):
            client = Mock()
            client.pixel.side_effect = [
                pixel({"database_id": "fixture-engine", "database_global": False}),
                table([[3, 12]]), table([["Ssl_cipher", "cipher"]]), table(grants),
            ]
            with self.assertRaises(RuntimeError):
                acceptance.check_database(client, "mariadb", "never-print")

    def test_pixel_errors_are_redacted_and_malformed_tables_rejected(self):
        client = Mock()
        client.pixel.side_effect = RuntimeError("PASSWORD=never-print")
        with self.assertRaisesRegex(RuntimeError, "^Registration failed$"):
            acceptance.checked_pixel(client, "sensitive expression", "Registration")
        for result in (pixel({}), pixel({"data": {}}), pixel({"data": {"values": "bad"}})):
            with self.assertRaises(RuntimeError):
                acceptance.rows(result)

    def test_main_reports_only_synthetic_results_and_checks_credential_shape(self):
        for password in ("1" * 48, "invalid"):
            with patch("application_acceptance.Client"), \
                 patch("application_acceptance.bootstrap_admin"), \
                 patch("application_acceptance.check_wrong_password"), \
                 patch("application_acceptance.check_database", return_value={"tls": "pass"}), \
                 patch("application_acceptance.secrets.token_urlsafe", return_value="private-secret"), \
                 patch("application_acceptance.Path.read_text", return_value=password), \
                 contextlib.redirect_stdout(io.StringIO()) as output:
                if password == "invalid":
                    with self.assertRaisesRegex(RuntimeError, "Invalid synthetic"):
                        acceptance.main()
                else:
                    acceptance.main()
                    result = json.loads(output.getvalue())
                    self.assertEqual(result["admin_login"], "pass")
                    self.assertEqual(set(result["databases"]), {"postgres", "mariadb"})
                    self.assertNotIn("private-secret", output.getvalue())
                    self.assertNotIn(password, output.getvalue())

    def test_registration_is_disabled_only_in_the_fixture_properties(self):
        with patch("application_acceptance.Path.open", mock_open()) as opened:
            acceptance.disable_registration()
        opened.assert_called_once_with("a")
        opened().write.assert_called_once_with("\nnative_registration=false\n")


if __name__ == "__main__":
    unittest.main()
