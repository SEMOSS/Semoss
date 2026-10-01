import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.Provider;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

/** Build-time addition of explicitly reviewed GovCloud RDS roots, not intermediates. */
public class RdsTrust {
    static final Set<String> ROOTS = Set.of(
        "4f9a3f476897123ffd6f4be6950b31f60869848eddc64103d729afa91746b0a8",
        "fbb52646a87a80da5a7e0781219aef06549b7031f58f0faea475839fc113fced",
        "df862cd419975345e6a52009206b24279ee5628089aa74be9a8bf0b553c5e713",
        "27e7166e2530425322a2f2b65e3ed7ad65abc2107d196250657520cd57bfcb63",
        "2ed64ee6c2e6c1a9b886b775eefb1dced7ea2fc4fe95c86af77d0650763df301",
        "0825e2e35ca65df57c6270a5fec61c5cae757ac110664c8c824a26360b7a32dc");

    static String fingerprint(X509Certificate certificate) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()));
    }

    static List<X509Certificate> certificates(byte[] pem) throws Exception {
        var result = new ArrayList<X509Certificate>();
        for (var certificate : CertificateFactory.getInstance("X.509")
                .generateCertificates(new ByteArrayInputStream(pem))) {
            result.add((X509Certificate) certificate);
        }
        if (result.isEmpty()) throw new CertificateException("Empty RDS CA bundle");
        return result;
    }

    static List<X509Certificate> roots(List<X509Certificate> certificates, Date date) throws Exception {
        var selected = new ArrayList<X509Certificate>();
        var fingerprints = new HashSet<String>();
        for (var certificate : certificates) {
            if (!certificate.getSubjectX500Principal().equals(certificate.getIssuerX500Principal())) continue;
            String fingerprint = fingerprint(certificate);
            if (!ROOTS.contains(fingerprint) || !fingerprints.add(fingerprint)) {
                throw new CertificateException("Unexpected or duplicate RDS root");
            }
            certificate.checkValidity(date);
            if (certificate.getBasicConstraints() < 0 || certificate.getKeyUsage() == null
                    || !certificate.getKeyUsage()[5]) {
                throw new CertificateException("RDS root lacks CA/keyCertSign constraints");
            }
            certificate.verify(certificate.getPublicKey());
            selected.add(certificate);
        }
        if (!fingerprints.equals(ROOTS)) throw new CertificateException("Incomplete GovCloud RDS root set");
        return selected;
    }

    static void install(KeyStore store, List<X509Certificate> roots) throws Exception {
        for (var root : roots) {
            String alias = "aws-rds-govcloud-" + fingerprint(root);
            if (store.containsAlias(alias)) throw new IllegalArgumentException("RDS alias already exists");
            store.setCertificateEntry(alias, root);
        }
    }

    static String pem(List<X509Certificate> roots) throws Exception {
        var result = new StringBuilder();
        for (var root : roots) {
            result.append("-----BEGIN CERTIFICATE-----\n")
                .append(Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(root.getEncoded()))
                .append("\n-----END CERTIFICATE-----\n");
        }
        return result.toString();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4 || !(args[1].equals("BCFKS") || args[1].equals("PKCS12"))) {
            throw new IllegalArgumentException("Usage: RdsTrust STORE BCFKS|PKCS12 WEST_PEM EAST_PEM");
        }
        if (args[1].equals("BCFKS")) {
            Security.addProvider((Provider) Class.forName(
                "org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider").getConstructor().newInstance());
        }
        var certificates = new ArrayList<X509Certificate>();
        for (int i = 2; i < 4; i++) certificates.addAll(certificates(Files.readAllBytes(Path.of(args[i]))));
        var roots = roots(certificates, new Date());
        var store = KeyStore.getInstance(args[1]);
        char[] password = "changeit".toCharArray(); // Integrity password for public certificates only.
        try (var input = Files.newInputStream(Path.of(args[0]))) {
            store.load(input, password);
        }
        int previousCount = store.size();
        install(store, roots);
        if (store.size() != previousCount + ROOTS.size()) throw new IllegalStateException("Trust anchor count mismatch");
        try (var output = Files.newOutputStream(Path.of(args[0]))) {
            store.store(output, password);
        }
        Path directory = Path.of(args[2]).getParent();
        Files.writeString(directory.resolve("aws-rds-govcloud.pem"), pem(roots), StandardCharsets.US_ASCII);
        var receipt = new StringBuilder("Existing trust entries preserved: " + previousCount + "\n");
        for (var root : roots) receipt.append(fingerprint(root)).append(" ").append(root.getSubjectX500Principal()).append("\n");
        Files.writeString(directory.resolve("roots-sha256.txt"), receipt);
        System.out.println("PASS: added six pinned GovCloud RDS roots; preserved " + previousCount + " existing entries");
    }
}
