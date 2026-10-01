import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RdsTrustTest {
    interface Checked { void run() throws Exception; }

    static void rejects(Checked operation) throws Exception {
        try {
            operation.run();
        } catch (IllegalArgumentException | java.security.cert.CertificateException expected) {
            return;
        }
        throw new AssertionError("Invalid trust material accepted");
    }

    public static void main(String[] args) throws Exception {
        var date = Date.from(Instant.parse("2026-10-01T00:00:00Z"));
        List<X509Certificate> all = new ArrayList<>();
        for (String file : args) {
            all.addAll(RdsTrust.certificates(Files.readAllBytes(Path.of(file))));
        }
        var roots = RdsTrust.roots(all, date);
        if (roots.size() != 6 || all.size() <= roots.size()) {
            throw new AssertionError("Expected six roots and excluded intermediates");
        }
        rejects(() -> RdsTrust.roots(List.of(), date));
        rejects(() -> RdsTrust.roots(roots.subList(0, 5), date));
        var duplicate = new ArrayList<>(all);
        duplicate.add(roots.getFirst());
        rejects(() -> RdsTrust.roots(duplicate, date));
        rejects(() -> RdsTrust.roots(all, Date.from(Instant.parse("2200-01-01T00:00:00Z"))));
        rejects(() -> RdsTrust.certificates(new byte[]{1, 2, 3}));
        rejects(() -> RdsTrust.certificates(new byte[]{}));

        var store = KeyStore.getInstance("PKCS12");
        store.load(null, "changeit".toCharArray());
        store.setCertificateEntry("existing-root", roots.getFirst());
        RdsTrust.install(store, roots);
        if (store.size() != 7 || !store.containsAlias("existing-root")) {
            throw new AssertionError("Existing trust anchors were not preserved");
        }
        rejects(() -> RdsTrust.install(store, roots));
        String pem = RdsTrust.pem(roots);
        if (CertificateFactory.getInstance("X.509").generateCertificates(
                new ByteArrayInputStream(pem.getBytes(java.nio.charset.StandardCharsets.US_ASCII))).size() != 6) {
            throw new AssertionError("PEM root count mismatch");
        }
        rejects(() -> RdsTrust.main(new String[]{}));
        rejects(() -> RdsTrust.main(new String[]{"store", "JKS", args[0], args[1]}));
        Path directory = Files.createTempDirectory("rds-trust-test-");
        try {
            Path target = directory.resolve("store.p12");
            Path west = directory.resolve("west.pem");
            Path east = directory.resolve("east.pem");
            Files.copy(Path.of(args[0]), west);
            Files.copy(Path.of(args[1]), east);
            var original = KeyStore.getInstance("PKCS12");
            original.load(null, "changeit".toCharArray());
            original.setCertificateEntry("preserved", roots.getFirst());
            try (var output = Files.newOutputStream(target)) {
                original.store(output, "changeit".toCharArray());
            }
            RdsTrust.main(new String[]{target.toString(), "PKCS12", west.toString(), east.toString()});
            var installed = KeyStore.getInstance(target.toFile(), "changeit".toCharArray());
            if (installed.size() != 7 || !installed.containsAlias("preserved")
                    || RdsTrust.certificates(Files.readAllBytes(directory.resolve("aws-rds-govcloud.pem"))).size() != 6) {
                throw new AssertionError("Installer output mismatch");
            }
        } finally {
            for (String file : List.of("store.p12", "west.pem", "east.pem",
                    "aws-rds-govcloud.pem", "roots-sha256.txt")) Files.deleteIfExists(directory.resolve(file));
            Files.delete(directory);
        }
        System.out.println("PASS: RDS root selection, expiry, duplicates, preservation, and PEM output");
    }
}
