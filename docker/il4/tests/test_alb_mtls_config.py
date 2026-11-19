import os
import subprocess
import unittest
from pathlib import Path

IL4 = Path(__file__).resolve().parents[1]
SETENV = IL4 / "conf" / "setenv.sh"
WEB_ENV = ("ALB_MTLS_ENABLED", "ALB_MTLS_TRUSTED_CIDRS", "CATALINA_OPTS")


def options(**env):
    """The result of sourcing conf/setenv.sh with the given environment."""
    clean = {k: v for k, v in os.environ.items() if k not in WEB_ENV}
    clean.update(env)
    return subprocess.run(["sh", "-c", f". {SETENV} && printf '%s' \"$CATALINA_OPTS\""],
                          env=clean, capture_output=True, text=True, check=False)


def alb_properties(result):
    return dict(word[2:].split("=", 1) for word in result.stdout.split() if word.startswith("-Dalb.mtls."))


class AlbMutualTlsConfigTests(unittest.TestCase):
    def test_nothing_changes_by_default(self):
        result = options()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(alb_properties(result), {})

    def test_false_is_the_same_as_unset_even_with_a_range_given(self):
        result = options(ALB_MTLS_ENABLED="false", ALB_MTLS_TRUSTED_CIDRS="10.0.0.0/8")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(alb_properties(result), {})

    def test_enabled_passes_the_range_with_spaces_removed(self):
        result = options(ALB_MTLS_ENABLED="true", ALB_MTLS_TRUSTED_CIDRS="192.168.200.0/24, 192.168.201.0/24 ,10.1.2.3")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(alb_properties(result), {
            "alb.mtls.enabled": "true",
            "alb.mtls.trusted.cidrs": "192.168.200.0/24,192.168.201.0/24,10.1.2.3"})

    def test_enabled_accepts_an_ipv6_range(self):
        result = options(ALB_MTLS_ENABLED="true", ALB_MTLS_TRUSTED_CIDRS="2001:db8::/32")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(alb_properties(result)["alb.mtls.trusted.cidrs"], "2001:db8::/32")

    def test_enabled_without_a_range_refuses_to_start(self):
        for cidrs in (None, "", "   ", ", ,"):
            env = {"ALB_MTLS_ENABLED": "true"}
            if cidrs is not None:
                env["ALB_MTLS_TRUSTED_CIDRS"] = cidrs
            result = options(**env)
            self.assertNotEqual(result.returncode, 0, f"range {cidrs!r}")
            self.assertIn("ALB_MTLS_TRUSTED_CIDRS", result.stderr)

    def test_a_range_with_anything_but_address_characters_is_refused(self):
        # catalina.sh re-parses CATALINA_OPTS with eval, so nothing shell-like may get in
        hostile = ["10.0.0.0/8;id", "10.0.0.0/8 $(id)", "10.0.0.0/8`id`", "10.0.0.0/8'", '10.0.0.0/8"',
                   "10.0.0.0/8|x", "10.0.0.0/8&x", "host.example.com", "10.0.0.0/8\nx", "10.0.0.0/8\\"]
        for cidrs in hostile:
            result = options(ALB_MTLS_ENABLED="true", ALB_MTLS_TRUSTED_CIDRS=cidrs)
            self.assertNotEqual(result.returncode, 0, f"range {cidrs!r}")

    def test_a_misspelled_switch_does_not_quietly_leave_it_off(self):
        for value in ("yes", "TRUE", "1", "on", ""):
            if value == "":
                continue  # an empty value is the same as unset
            result = options(ALB_MTLS_ENABLED=value, ALB_MTLS_TRUSTED_CIDRS="10.0.0.0/8")
            self.assertNotEqual(result.returncode, 0, f"value {value!r}")

    def test_the_cac_options_are_untouched(self):
        result = options(ALB_MTLS_ENABLED="true", ALB_MTLS_TRUSTED_CIDRS="10.0.0.0/8")
        self.assertIn("-Dcac.piv.required=true", result.stdout)
        self.assertIn("-Dcac.piv.truststore.file=/run/secrets/cac-truststore.bcfks", result.stdout)


if __name__ == "__main__":
    unittest.main()
