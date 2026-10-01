import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.Security;
import java.security.Signature;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider;

public final class AccpCheck {
    private static final String ACCP = AmazonCorrettoCryptoProvider.PROVIDER_NAME;

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static void requireAccp(java.security.Provider provider, String operation) {
        require(provider.getName().equals(ACCP), "Default " + operation + " bypasses ACCP: " + provider);
    }

    private static void checkTrustStore() throws Exception {
        KeyStore trust = KeyStore.getInstance("PKCS12", "SUN");
        try (var input = java.nio.file.Files.newInputStream(
                Path.of(System.getProperty("javax.net.ssl.trustStore")))) {
            trust.load(input, "changeit".toCharArray());
        }
        KeyStore original = KeyStore.getInstance(
                Path.of(System.getProperty("java.home"), "lib/security/cacerts").toFile(),
                "changeit".toCharArray());
        require(trust.size() > 0 && trust.size() == original.size(), "Public trust anchor count changed");
        for (var aliases = original.aliases(); aliases.hasMoreElements();) {
            String alias = aliases.nextElement();
            require(trust.isCertificateEntry(alias), "Missing public trust anchor: " + alias);
            require(Arrays.equals(original.getCertificate(alias).getEncoded(),
                    trust.getCertificate(alias).getEncoded()), "Public trust anchor changed: " + alias);
        }
        System.out.println("PASS: experimental/nonvalidated PKCS12 truststore preserves all "
                + trust.size() + " public anchors");
    }

    public static void main(String[] args) throws Exception {
        require(Runtime.version().feature() == 25, "Java 25 required");
        var provider = Security.getProvider(ACCP);
        require(provider instanceof AmazonCorrettoCryptoProvider, "ACCP not registered");
        var accp = (AmazonCorrettoCryptoProvider) provider;
        accp.assertHealthy();
        require(accp.isFips(), "Expected ACCP FIPS artifact");
        require(!accp.isExperimentalFips(), "Unexpected experimental-FIPS artifact");
        require(accp.getVersionStr().equals("2.5.0"), "Unexpected ACCP version: " + accp.getVersionStr());
        require(accp.getAwsLcVersionStr().equals("AWS-LC AWS-LC-FIPS-3.0.0"),
                "Unexpected AWS-LC identity: " + accp.getAwsLcVersionStr());
        require(Security.getProviders()[0] == accp, "ACCP must be first");
        require(Security.getProvider("BCFIPS") == null && Security.getProvider("BCJSSE") == null
                && Security.getProvider("BC") == null, "BC must not be registered");

        SecureRandom random = new SecureRandom();
        byte[] nonce = new byte[12];
        random.nextBytes(nonce);
        requireAccp(random.getProvider(), "SecureRandom");
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        require(Arrays.equals(digest.digest(new byte[0]),
                java.util.HexFormat.of().parseHex("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")),
                "SHA-256 known-answer test failed");
        requireAccp(digest.getProvider(), "SHA-256");
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256, random);
        var key = generator.generateKey();
        requireAccp(generator.getProvider(), "AES key generation");
        Cipher encrypt = Cipher.getInstance("AES/GCM/NoPadding");
        encrypt.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, nonce), random);
        byte[] plaintext = {1, 2, 3};
        byte[] encrypted = encrypt.doFinal(plaintext);
        requireAccp(encrypt.getProvider(), "AES-GCM encryption");
        Cipher decrypt = Cipher.getInstance("AES/GCM/NoPadding");
        decrypt.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(128, nonce));
        require(Arrays.equals(decrypt.doFinal(encrypted), plaintext), "AES-GCM round trip failed");
        requireAccp(decrypt.getProvider(), "AES-GCM decryption");

        KeyPairGenerator rsa = KeyPairGenerator.getInstance("RSA");
        rsa.initialize(2048, random);
        var pair = rsa.generateKeyPair();
        requireAccp(rsa.getProvider(), "RSA key generation");
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(pair.getPrivate(), random);
        signature.update(plaintext);
        byte[] signed = signature.sign();
        requireAccp(signature.getProvider(), "RSA signing");
        signature.initVerify(pair.getPublic());
        signature.update(plaintext);
        require(signature.verify(signed), "RSA signature verification failed");
        requireAccp(signature.getProvider(), "RSA verification");

        for (String algorithm : new String[]{"PBKDF2WithHmacSHA256", "PBKDF2WithHmacSHA512"}) {
            var spec = new PBEKeySpec("development-test-password".toCharArray(), new byte[16], 10000, 256);
            try {
                var factory = SecretKeyFactory.getInstance(algorithm);
                factory.generateSecret(spec);
                require(factory.getProvider().getName().equals("SunJCE"),
                        "Unexpected PBKDF2 fallback provider");
                System.err.println("KNOWN NONVALIDATED FALLBACK: " + algorithm
                        + "=SunJCE; ACCP 2.5.0 does not implement this algorithm");
            } finally {
                spec.clearPassword();
            }
        }
        checkTrustStore();
        require(KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
                .getProvider().getName().equals("SunJSSE"), "Wrong default key manager");
        require(TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                .getProvider().getName().equals("SunJSSE"), "Wrong default trust manager");
        require(SSLContext.getDefault().getProvider().getName().equals("SunJSSE"), "Wrong default TLS");
        // These feature APIs remain available without registering the BC providers.
        Class.forName("org.bouncycastle.asn1.ASN1InputStream");
        Class.forName("org.bouncycastle.openssl.PEMParser");
        System.out.println("PASS: experimental/nonvalidated ACCP " + accp.getVersionStr() + ", "
                + accp.getAwsLcVersionStr() + ", native self-tests, default RNG/SHA-256/AES-GCM/RSA, "
                + "SunJSSE. Not a FIPS compliance or certificate coverage assertion.");
    }
}
