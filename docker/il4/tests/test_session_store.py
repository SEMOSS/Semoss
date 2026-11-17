import importlib.util
import os
import stat
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

IL4 = Path(__file__).resolve().parents[1]
SETENV = IL4 / "conf" / "setenv.sh"
SERVER_XML = IL4 / "conf" / "server.xml"
ENTRYPOINT = IL4 / "entrypoint.sh"
HELPER = IL4 / "session_store.py"
INIT = IL4 / "SessionStoreInit.java"

spec = importlib.util.spec_from_file_location("session_store", HELPER)
session_store = importlib.util.module_from_spec(spec)
spec.loader.exec_module(session_store)

URL = ("jdbc:postgresql://db.cluster-x.us-gov-west-1.rds.amazonaws.com:5432/app"
       "?sslmode=verify-full&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory")


def good(**extra):
    env = {"SESSION_STORE": "jdbc", "SESSION_STORE_URL": URL, "SESSION_STORE_USER": "sessuser7",
           "SESSION_STORE_PASSWORD": "p@ss w0rd!&<>\"'$`;"}
    env.update(extra)
    return env


class SettingsTests(unittest.TestCase):
    def test_memory_is_the_default_and_needs_nothing(self):
        self.assertIsNone(session_store.settings({}))
        self.assertIsNone(session_store.settings({"SESSION_STORE": "memory"}))
        self.assertIsNone(session_store.settings({"SESSION_STORE": ""}))

    def test_an_unknown_mode_is_refused_not_ignored(self):
        with self.assertRaises(session_store.ConfigurationError):
            session_store.settings({"SESSION_STORE": "redis"})

    def test_jdbc_accepts_a_verified_tls_url(self):
        config = session_store.settings(good())
        self.assertEqual(config["pool"], 10)
        self.assertEqual(config["sync"], 2)

    def test_tls_verification_is_required_by_default(self):
        for url in ("jdbc:postgresql://db.example.mil:5432/app",
                    "jdbc:postgresql://db.example.mil:5432/app?sslmode=require",
                    "jdbc:postgresql://db.example.mil:5432/app?sslmode=disable",
                    "jdbc:postgresql://db.example.mil:5432/app?sslmode=verify-ca"):
            with self.subTest(url=url), self.assertRaisesRegex(session_store.ConfigurationError, "verify-full"):
                session_store.settings(good(SESSION_STORE_URL=url))

    def test_a_local_test_database_needs_an_explicit_opt_out(self):
        env = good(SESSION_STORE_URL="jdbc:postgresql://localhost:5432/app", SESSION_STORE_TLS_REQUIRED="false")
        self.assertIsNotNone(session_store.settings(env))
        with self.assertRaises(session_store.ConfigurationError):
            session_store.settings(good(SESSION_STORE_TLS_REQUIRED="no"))

    def test_the_url_cannot_smuggle_anything_into_the_generated_file(self):
        for url in ("jdbc:mysql://h:3306/app?sslmode=verify-full",
                    "jdbc:postgresql://h:5432/app?sslmode=verify-full\"/><x",
                    "jdbc:postgresql://h:5432/app?sslmode=verify-full&a=b c",
                    "jdbc:postgresql://h:5432/app?sslmode=verify-full'",
                    "jdbc:postgresql://user:pw@h:5432/app?sslmode=verify-full",
                    "jdbc:postgresql://h:5432/app\n",
                    ""):
            with self.subTest(url=url), self.assertRaises(session_store.ConfigurationError):
                session_store.settings(good(SESSION_STORE_URL=url))

    def test_user_and_password_are_checked(self):
        for user in ("", "a b", "a\"b", "a;b", "x" * 129):
            with self.subTest(user=user), self.assertRaises(session_store.ConfigurationError):
                session_store.settings(good(SESSION_STORE_USER=user))
        for password in ("", "a\nb", "a\x00b", "x" * 257):
            with self.subTest(password=repr(password)), self.assertRaises(session_store.ConfigurationError):
                session_store.settings(good(SESSION_STORE_PASSWORD=password))

    def test_numbers_are_bounded(self):
        for name in ("SESSION_STORE_POOL_SIZE", "SESSION_STORE_SYNC_SECONDS"):
            for value in ("0", "-1", "abc", "1000", "1.5"):
                with self.subTest(name=name, value=value), self.assertRaises(session_store.ConfigurationError):
                    session_store.settings(good(**{name: value}))


class DescriptorTests(unittest.TestCase):
    def test_every_character_of_the_password_survives_xml_escaping(self):
        password = "p@ss w0rd!&<>\"'$`;\\#{}%"
        config = session_store.settings(good(SESSION_STORE_PASSWORD=password))
        root = ET.fromstring(session_store.render(config))
        resource = root.find("Resource")
        self.assertEqual(resource.get("password"), password)
        self.assertEqual(resource.get("username"), "sessuser7")
        self.assertEqual(resource.get("url"), URL)

    def test_the_descriptor_configures_a_shared_persistent_manager(self):
        root = ET.fromstring(session_store.render(session_store.settings(good())))
        self.assertEqual(root.tag, "Context")
        self.assertEqual(root.get("backgroundProcessorDelay"), "2")
        manager = root.find("Manager")
        self.assertEqual(manager.get("className"), "org.apache.catalina.session.PersistentManager")
        # a stopping container must leave its sessions in the shared store, not expire them
        self.assertEqual(manager.get("saveOnRestart"), "true")
        self.assertEqual(manager.get("maxIdleBackup"), "0")
        self.assertEqual(manager.get("maxIdleSwap"), "-1")
        # same session id generator provider as the global context.xml
        self.assertEqual(manager.get("secureRandomProvider"), "BCFIPS")
        store = manager.find("Store")
        self.assertEqual(store.get("className"), "org.apache.catalina.session.DataSourceStore")
        self.assertEqual(store.get("sessionTable"), "tomcat_sessions")
        self.assertTrue(store.get("dataSourceName").endswith(root.find("Resource").get("name").split("/")[-1]))

    def test_the_sync_interval_and_pool_follow_the_settings(self):
        root = ET.fromstring(session_store.render(session_store.settings(
            good(SESSION_STORE_SYNC_SECONDS="5", SESSION_STORE_POOL_SIZE="4"))))
        self.assertEqual(root.get("backgroundProcessorDelay"), "5")
        self.assertEqual(root.find("Resource").get("maxActive"), "4")

    def test_the_file_is_private_and_logging_never_shows_a_secret(self):
        config = session_store.settings(good())
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "session-store"
            path = Path(session_store.write(str(target), config))
            self.assertEqual(path.name, "Monolith.xml")
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)
        line = session_store.describe(config)
        self.assertIn("db.cluster-x.us-gov-west-1.rds.amazonaws.com:5432/app", line)
        self.assertNotIn("sessuser7", line)
        self.assertNotIn("w0rd", line)

    def test_the_command_line_helper_prints_no_secret_and_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            env = {**os.environ, **good()}
            ok = subprocess.run(["python3", str(HELPER), "prepare", f"{directory}/s"], env=env,
                                capture_output=True, text=True)
            self.assertEqual(ok.returncode, 0, ok.stderr)
            self.assertNotIn("w0rd", ok.stdout + ok.stderr)
            bad = subprocess.run(["python3", str(HELPER), "prepare", f"{directory}/t"],
                                 env={**env, "SESSION_STORE_URL": "jdbc:postgresql://h:5432/app"},
                                 capture_output=True, text=True)
            self.assertEqual(bad.returncode, 1)
            self.assertIn("verify-full", bad.stderr)
            self.assertNotIn("w0rd", bad.stdout + bad.stderr)
            self.assertFalse((Path(directory) / "t").exists())


class WiringTests(unittest.TestCase):
    def options(self, **env):
        clean = {k: v for k, v in os.environ.items() if not k.startswith("SESSION_STORE")
                 and k not in ("CATALINA_OPTS",)}
        clean.update(env)
        return subprocess.run(["sh", "-c", f". {SETENV} && printf '%s' \"$CATALINA_OPTS\""],
                              env=clean, capture_output=True, text=True)

    def test_nothing_changes_by_default(self):
        result = self.options()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("session.xml.base", result.stdout)

    def test_jdbc_adds_only_the_directory_name_to_the_jvm_options(self):
        result = self.options(**good())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("-Dsession.xml.base=/tmp/session-store", result.stdout)
        for secret in ("w0rd", "sessuser7", "rds.amazonaws.com", "sslmode"):
            self.assertNotIn(secret, result.stdout)

    def test_a_misspelled_mode_stops_the_start(self):
        self.assertNotEqual(self.options(SESSION_STORE="jdbc2").returncode, 0)

    def test_server_xml_only_changes_the_descriptor_directory(self):
        host = ET.parse(SERVER_XML).getroot().find(".//Host")
        self.assertEqual(host.get("xmlBase"), "${session.xml.base:-conf/Catalina/localhost}")
        # descriptors inside a web application stay ignored
        self.assertEqual(host.get("deployXML"), "false")

    def test_the_global_context_still_uses_the_in_memory_manager(self):
        context = ET.parse(IL4 / "conf" / "context.xml").getroot()
        manager = context.find("Manager")
        self.assertIsNone(manager.get("className"))
        self.assertEqual(manager.get("pathname"), "")
        self.assertIsNone(context.find("Resource"))

    def test_the_entrypoint_prepares_the_store_before_tomcat_and_fails_closed(self):
        text = ENTRYPOINT.read_text()
        self.assertEqual(subprocess.run(["sh", "-n", str(ENTRYPOINT)], capture_output=True).returncode, 0)
        prepare = text.index("session_store.py prepare /tmp/session-store")
        init = text.index("SessionStoreInit")
        launch = text.index("exec /opt/tomcat/bin/catalina.sh run")
        self.assertLess(prepare, init)
        self.assertLess(init, launch)
        self.assertIn("set -eu", text)
        self.assertIn("SESSION_STORE must be memory or jdbc", text)

    def test_the_java_helper_never_takes_secrets_from_arguments_or_prints_them(self):
        source = INIT.read_text()
        self.assertIn('System.getenv(name)', source)
        self.assertNotIn("args[", source)
        self.assertNotIn('"password"', source.replace('properties.setProperty("password"', ""))
        self.assertIn("pg_advisory_lock", source)
        self.assertIn("to_regclass", source)


if __name__ == "__main__":
    unittest.main()
