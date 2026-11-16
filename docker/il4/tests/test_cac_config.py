import os
import subprocess
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

IL4 = Path(__file__).resolve().parents[1]
SETENV = IL4 / "conf" / "setenv.sh"
SERVER_XML = IL4 / "conf" / "server.xml"
ENTRYPOINT = IL4 / "entrypoint.sh"


def cac_options(**env):
    """CATALINA_OPTS words that conf/setenv.sh adds, for the given environment."""
    clean = {k: v for k, v in os.environ.items()
             if k not in ("CAC_PIV_REQUIRED", "CAC_PIV_CLIENT_AUTH", "CATALINA_OPTS")}
    clean.update(env)
    out = subprocess.run(["sh", "-c", f". {SETENV} && printf '%s' \"$CATALINA_OPTS\""],
                         env=clean, capture_output=True, text=True, check=False)
    return out


def properties(result):
    return dict(word[2:].split("=", 1) for word in result.stdout.split() if word.startswith("-Dcac.piv."))


class CacRequiredByDefaultTests(unittest.TestCase):
    def test_default_requires_cac_and_requests_a_certificate(self):
        result = cac_options()
        self.assertEqual(result.returncode, 0, result.stderr)
        props = properties(result)
        self.assertEqual(props["cac.piv.required"], "true")
        self.assertEqual(props["cac.piv.client.auth"], "want")
        self.assertEqual(props["cac.piv.truststore.file"], "/run/secrets/cac-truststore.bcfks")

    def test_required_never_falls_back_to_the_jvm_public_ca_store(self):
        props = properties(cac_options(CAC_PIV_REQUIRED="true"))
        self.assertNotIn("cacerts", props["cac.piv.truststore.file"])

    def test_opting_out_must_be_explicit(self):
        props = properties(cac_options(CAC_PIV_REQUIRED="false"))
        self.assertEqual(props["cac.piv.required"], "false")
        self.assertEqual(props["cac.piv.client.auth"], "false")

    def test_request_without_requiring_still_validates_against_the_cac_store(self):
        props = properties(cac_options(CAC_PIV_REQUIRED="false", CAC_PIV_CLIENT_AUTH="true"))
        self.assertEqual(props["cac.piv.required"], "false")
        self.assertEqual(props["cac.piv.client.auth"], "want")
        self.assertEqual(props["cac.piv.truststore.file"], "/run/secrets/cac-truststore.bcfks")

    def test_a_misspelled_value_does_not_quietly_turn_cac_off(self):
        for value in ("yes", "0", "False", ""):
            result = cac_options(CAC_PIV_REQUIRED=value) if value else cac_options(CAC_PIV_REQUIRED="")
            if value == "":
                # empty is treated as unset, so the default (required) applies
                self.assertEqual(properties(result)["cac.piv.required"], "true")
            else:
                self.assertNotEqual(result.returncode, 0, value)


class ConnectorTests(unittest.TestCase):
    def setUp(self):
        self.root = ET.parse(SERVER_XML).getroot()
        self.connector = self.root.find(".//Connector")
        self.host_config = self.connector.find("SSLHostConfig")

    def test_connector_requests_but_never_hard_requires_a_certificate(self):
        client_auth = self.connector.get("clientAuth")
        self.assertEqual(client_auth, "${cac.piv.client.auth:-false}")
        # a hard requirement would end the handshake and leave no page to show
        self.assertNotIn("true", client_auth.replace("${cac.piv.client.auth:-false}", ""))

    def test_client_certificates_are_validated_against_the_cac_trust_store(self):
        self.assertEqual(self.host_config.get("truststoreFile"),
                         "${cac.piv.truststore.file:-/opt/fips/cacerts.bcfks}")
        self.assertEqual(self.host_config.get("truststoreType"), "BCFKS")
        self.assertEqual(self.host_config.get("truststoreProvider"), "BCFIPS")

    def test_tls_sessions_are_short_so_a_retry_after_inserting_the_card_re_handshakes(self):
        self.assertLessEqual(int(self.host_config.get("sessionTimeout")), 60)


class EntrypointTests(unittest.TestCase):
    def test_start_is_refused_without_the_cac_trust_store_unless_opted_out(self):
        text = ENTRYPOINT.read_text()
        check = text.index('"${CAC_PIV_REQUIRED:-true}" = "true"')
        self.assertIn("/run/secrets/cac-truststore.bcfks", text[check:])
        self.assertIn("/run/secrets/cac-truststore.password", text[check:])
        # the refusal comes before Tomcat is started
        self.assertLess(check, text.index("exec /opt/tomcat/bin/catalina.sh run"))
        self.assertIn("exit 1", text[check:text.index("exec /opt/tomcat/bin/catalina.sh run")])


if __name__ == "__main__":
    unittest.main()
