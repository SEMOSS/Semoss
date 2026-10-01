import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.Security;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.crypto.fips.FipsStatus;

public final class FipsCheck {
    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    public static void main(String[] args) throws Exception {
        require(Runtime.version().feature() == 25, "Java 25 required");
        require(FipsStatus.isReady(), "BCFIPS self-tests failed");
        require(CryptoServicesRegistrar.isInApprovedOnlyMode(), "BCFIPS approved-only mode is off");
        require(Security.getProviders()[0].getName().equals("BCFIPS"), "BCFIPS must be first");
        require(Security.getProviders()[1].getName().equals("BCJSSE"), "BCJSSE must be second");
        require(new SecureRandom().getProvider().getName().equals("BCFIPS"), "Default random bypasses BCFIPS");
        SecureRandom random = SecureRandom.getInstance("DEFAULT", "BCFIPS");
        require(MessageDigest.getInstance("SHA-256").getProvider().getName().equals("BCFIPS"),
                "Default SHA-256 bypasses BCFIPS");
        KeyGenerator generator = KeyGenerator.getInstance("AES", "BCFIPS");
        generator.init(256, random);
        var key = generator.generateKey();
        byte[] nonce = new byte[12];
        random.nextBytes(nonce);
        Cipher encrypt = Cipher.getInstance("AES/GCM/NoPadding", "BCFIPS");
        encrypt.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, nonce), random);
        byte[] encrypted = encrypt.doFinal(new byte[] {1, 2, 3});
        Cipher decrypt = Cipher.getInstance("AES/GCM/NoPadding", "BCFIPS");
        decrypt.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(128, nonce));
        require(java.util.Arrays.equals(decrypt.doFinal(encrypted), new byte[] {1, 2, 3}),
                "AES-GCM round trip failed");
        byte[] salt = new byte[16];
        random.nextBytes(salt);
        var spec = new PBEKeySpec("FIPS-test-only-password".toCharArray(), salt, 10000, 256);
        SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256", "BCFIPS").generateSecret(spec);
        spec.clearPassword();
        KeyStore trust = KeyStore.getInstance("BCFKS", "BCFIPS");
        try (var input = Files.newInputStream(Path.of(System.getProperty("javax.net.ssl.trustStore")))) {
            trust.load(input, "changeit".toCharArray());
        }
        require(trust.size() > 0, "Empty truststore");
        require(KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
                .getProvider().getName().equals("BCJSSE"), "Wrong default key manager");
        require(TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                .getProvider().getName().equals("BCJSSE"), "Wrong default trust manager");
        require(SSLContext.getDefault().getProvider().getName().equals("BCJSSE"),
                "Default TLS bypasses BCJSSE");
        System.out.println("PASS: Java 25, BCFIPS self-tests/approved mode, AES-GCM, PBKDF2, BCFKS, BCJSSE");
    }
}
