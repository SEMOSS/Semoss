import hashlib
from pathlib import Path
import tempfile
import unittest
import zipfile

from accp_support import NATIVE_ENTRY, install_accp, security_properties


class AccpSupportTests(unittest.TestCase):
    def test_signed_jar_preserved_and_native_immutable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            jar = root / "accp.jar"
            with zipfile.ZipFile(jar, "w") as archive:
                archive.writestr(NATIVE_ENTRY, b"native")
                archive.writestr("META-INF/AMAZON.SF", b"signed")
            digest = hashlib.sha256(jar.read_bytes()).hexdigest()
            report = install_accp(jar, root / "accp")
            self.assertEqual(hashlib.sha256(jar.read_bytes()).hexdigest(), digest)
            self.assertEqual((root / "accp/accp.jar").read_bytes(), jar.read_bytes())
            native = root / "accp/lib/libamazonCorrettoCryptoProvider.so"
            self.assertEqual(native.read_bytes(), b"native")
            self.assertEqual(native.stat().st_mode & 0o777, 0o555)
            self.assertEqual(report["source_jar_sha256"], digest)
            self.assertEqual(report["sha256"], hashlib.sha256(b"native").hexdigest())
            self.assertEqual(report["runtime_path"], "/opt/accp/lib/libamazonCorrettoCryptoProvider.so")

    def test_missing_empty_or_duplicate_native_rejected(self):
        for entries in ([], [b""], [b"one", b"two"]):
            with self.subTest(entries=entries), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                jar = root / "bad.jar"
                with zipfile.ZipFile(jar, "w") as archive:
                    for content in entries:
                        archive.writestr(NATIVE_ENTRY, content)
                with self.assertRaisesRegex(ValueError, "native"):
                    install_accp(jar, root / "accp")

    def test_security_preserves_stock_providers_and_restrictions(self):
        original = ("security.provider.1=SUN\nsecurity.provider.2=SunRsaSign\n"
                    "security.provider.3=SunEC\nsecurity.provider.4=SunJSSE\n"
                    "security.provider.5=SunJCE\nkeystore.type=jks\n"
                    "ssl.KeyManagerFactory.algorithm=SunX509\n"
                    "jdk.tls.disabledAlgorithms=SSLv3, TLSv1\n"
                    "jdk.certpath.disabledAlgorithms=MD5\nother.property=value\n")
        result = security_properties(original)
        self.assertIn("security.provider.1=com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider", result)
        for index, provider in enumerate(("SUN", "SunRsaSign", "SunEC", "SunJSSE", "SunJCE"), 2):
            self.assertIn(f"security.provider.{index}={provider}\n", result)
        self.assertIn("other.property=value\n", result)
        self.assertIn("keystore.type=PKCS12\n", result)
        self.assertNotIn("keystore.type=jks", result)
        self.assertIn("DH keySize < 2048", result)
        self.assertIn("RSA keySize < 2048", result)
        self.assertEqual(result.count("jdk.tls.disabledAlgorithms="), 1)
        self.assertEqual(result.count("ssl.KeyManagerFactory.algorithm="), 1)

    def test_security_multiline_properties_and_unexpected_providers(self):
        stock = ("security.provider.1=SUN\nsecurity.provider.2=SunJSSE\n"
                 "security.provider.3=SunJCE\njdk.tls.disabledAlgorithms=old, \\\n"
                 "    continuation\nunrelated=value\n")
        self.assertNotIn("continuation", security_properties(stock))
        for invalid in ("", stock.replace("SunJSSE", "BCJSSE"),
                        stock + "security.provider.4=org.bouncycastle.Provider\n"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                security_properties(invalid)


if __name__ == "__main__":
    unittest.main()
