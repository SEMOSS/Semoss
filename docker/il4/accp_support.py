"""Build-time ACCP native extraction and experimental Java provider configuration."""
import hashlib
from pathlib import Path
import re
import shutil
import sys
import zipfile


ACCP_COORDINATE = "software.amazon.cryptools:AmazonCorrettoCryptoProvider-FIPS:2.5.0:jar:linux-x86_64"
NATIVE_ENTRY = "com/amazon/corretto/crypto/provider/libamazonCorrettoCryptoProvider.so"


def install_accp(source, destination):
    with zipfile.ZipFile(source) as archive:
        if archive.namelist().count(NATIVE_ENTRY) != 1:
            raise ValueError("Expected exactly one ACCP native library")
        native = archive.read(NATIVE_ENTRY)
        if not native:
            raise ValueError("Empty ACCP native library")
    native_dir = destination / "lib"
    native_dir.mkdir(parents=True, exist_ok=True)
    # Do not rewrite the signed provider JAR; load its native code from a read-only image layer.
    shutil.copy2(source, destination / source.name)
    library = native_dir / "libamazonCorrettoCryptoProvider.so"
    library.write_bytes(native)
    library.chmod(0o555)
    return {
        "source_jar": source.name,
        "source_jar_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "entry": NATIVE_ENTRY,
        "sha256": hashlib.sha256(native).hexdigest(),
        "runtime_path": "/opt/accp/lib/libamazonCorrettoCryptoProvider.so",
    }


def security_properties(text):
    text = re.sub(r"\\\r?\n[ \t]*", "", text)
    providers = re.findall(r"(?m)^security\.provider\.\d+=(.+)$", text)
    if not {"SUN", "SunJSSE", "SunJCE"}.issubset(providers) or any(
            "bouncycastle" in provider.lower() or provider.startswith(("BC", "com.amazon."))
            for provider in providers):
        raise ValueError("Expected stock Java providers, without BC or ACCP registration")
    overrides = {
        "ssl.KeyManagerFactory.algorithm": "SunX509",
        "ssl.TrustManagerFactory.algorithm": "PKIX",
        "keystore.type": "PKCS12",
        "jdk.tls.disabledAlgorithms":
            "SSLv3, TLSv1, TLSv1.1, DTLSv1.0, RC4, DES, MD5withRSA, DH keySize < 2048, "
            "EC keySize < 224, 3DES_EDE_CBC, anon, NULL, ECDH, SHA1, DSA, DHE, CBC, "
            "CHACHA20-POLY1305, TLS_RSA_WITH_AES_256_GCM_SHA384, TLS_RSA_WITH_AES_128_GCM_SHA256, "
            "TLS_RSA_WITH_AES_256_CBC_SHA256, TLS_RSA_WITH_AES_128_CBC_SHA256",
        "jdk.certpath.disabledAlgorithms": "MD2, MD5, SHA1, RSA keySize < 2048, DSA, EC keySize < 224",
        "securerandom.strongAlgorithms":
            "LibCryptoRng:AmazonCorrettoCryptoProvider,NativePRNGBlocking:SUN,DRBG:SUN",
    }
    for key in overrides:
        text = re.sub(r"(?m)^" + re.escape(key) + r"=.*\n?", "", text)
    text = re.sub(r"(?m)^security\.provider\.\d+=.*\n?", "", text)
    providers.insert(0, "com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider")
    return (text.rstrip() + "\n" +
            "".join(f"security.provider.{index}={provider}\n"
                    for index, provider in enumerate(providers, 1)) +
            "".join(f"{key}={value}\n" for key, value in overrides.items()))


if __name__ == "__main__":
    Path(sys.argv[2]).write_text(security_properties(Path(sys.argv[1]).read_text()))
