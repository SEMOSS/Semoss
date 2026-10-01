import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
WARNING = ("WARNING: Experimental/nonvalidated ACCP runtime; SunJSSE and SunJCE PBKDF2 "
           "are not a validated FIPS path. No certificate coverage is claimed.")


class AccpRuntimeTests(unittest.TestCase):
    def entrypoint(self, acknowledgement, argument, *, java_status=0, secrets=True, global_options=False):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            stub = base / "stub"
            stub.write_text("#!/bin/sh\nexit 0\n")
            stub.chmod(0o755)
            envfile = base / "setenv.sh"
            envfile.write_text("CATALINA_OPTS=''\n")
            secret = base / "secret"
            if secrets:
                secret.write_text("fixture")
            script = (ROOT / "entrypoint.sh").read_text()
            script = script.replace("/opt/tomcat/bin/setenv.sh", str(envfile))
            script = script.replace("/opt/semoss-python/bin/python", str(stub))
            script = script.replace("/opt/tomcat/bin/catalina.sh", str(stub))
            script = script.replace("/run/secrets/server.p12", str(secret))
            script = script.replace("/run/secrets/server.password", str(secret))
            (base / "java").write_text(f"#!/bin/sh\nexit {java_status}\n")
            (base / "java").chmod(0o755)
            env = {key: value for key, value in os.environ.items()
                   if key not in ("SEMOSS_ALLOW_NONVALIDATED_ACCP", "JAVA_TOOL_OPTIONS",
                                  "JDK_JAVA_OPTIONS", "JAVA_OPTS")}
            env["PATH"] = str(base) + ":" + env["PATH"]
            if acknowledgement is not None:
                env["SEMOSS_ALLOW_NONVALIDATED_ACCP"] = acknowledgement
            if global_options:
                env["JAVA_TOOL_OPTIONS"] = "-Dunreviewed=true"
            return subprocess.run(["sh", "-c", script, "entrypoint", argument],
                                  env=env, capture_output=True, text=True)

    def test_run_requires_exact_acknowledgement(self):
        for acknowledgement in (None, "", "false", "TRUE", "1", "true "):
            with self.subTest(acknowledgement=acknowledgement):
                result = self.entrypoint(acknowledgement, "run")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(WARNING, result.stderr)
                self.assertIn("SEMOSS_ALLOW_NONVALIDATED_ACCP=true", result.stderr)

    def test_explicit_development_ack_runs_with_warning(self):
        result = self.entrypoint("true", "run")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(WARNING, result.stderr)

    def test_check_does_not_require_development_ack(self):
        result = self.entrypoint(None, "--check")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(WARNING, result.stderr)

    def test_invalid_command_rejected(self):
        self.assertNotEqual(self.entrypoint("true", "other").returncode, 0)

    def test_provider_check_failure_is_fatal_even_when_acknowledged(self):
        for argument in ("run", "--check"):
            with self.subTest(argument=argument):
                result = self.entrypoint("true", argument, java_status=7)
                self.assertEqual(result.returncode, 7, result.stderr)

    def test_tls_secret_and_global_option_guards_remain(self):
        result = self.entrypoint("true", "run", secrets=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Required TLS secret", result.stderr)
        result = self.entrypoint("true", "run", global_options=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Do not inject global JVM options", result.stderr)

    def test_tls_configuration(self):
        server = ET.parse(ROOT / "conf/server.xml")
        cert = server.find(".//Certificate")
        self.assertEqual(cert.get("certificateKeystoreFile"), "/run/secrets/server.p12")
        self.assertEqual(cert.get("certificateKeystoreType"), "PKCS12")
        self.assertEqual(cert.get("certificateKeystoreProvider"), "SUN")
        manager = ET.parse(ROOT / "conf/context.xml").find("Manager")
        self.assertEqual(manager.get("secureRandomProvider"), "AmazonCorrettoCryptoProvider")
        self.assertEqual(manager.get("secureRandomAlgorithm"), "LibCryptoRng")
        options = (ROOT / "conf/setenv.sh").read_text()
        self.assertIn("-Dcom.amazon.corretto.crypto.provider.useExternalLib=true", options)
        self.assertIn("-Djava.library.path=/opt/accp/lib", options)
        self.assertIn("-Djavax.net.ssl.trustStoreType=PKCS12", options)
        self.assertNotIn("BCFIPS", options)


if __name__ == "__main__":
    unittest.main()
