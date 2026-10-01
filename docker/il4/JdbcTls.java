import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.SSLContext;

public final class JdbcTls {
    static final String TRUST = "/opt/fips/cacerts.bcfks";

    enum Database {
        POSTGRES("org.postgresql.Driver", 42, 7, 5432,
                Map.of("sslmode", "verify-full", "sslfactory", "org.postgresql.ssl.DefaultJavaSSLFactory")),
        SQL_SERVER("com.microsoft.sqlserver.jdbc.SQLServerDriver", 13, 6, 1433,
                Map.of("encrypt", "true", "trustServerCertificate", "false",
                       "fips", "true", "trustStoreType", "BCFKS")),
        MARIA_DB("org.mariadb.jdbc.Driver", 3, 5, 3306,
                Map.of("sslMode", "verify-full", "trustStoreType", "BCFKS",
                       "allowLocalInfile", "false"));

        final String driver;
        final int major;
        final int minor;
        final int port;
        final Map<String, String> required;

        Database(String driver, int major, int minor, int port, Map<String, String> required) {
            this.driver = driver;
            this.major = major;
            this.minor = minor;
            this.port = port;
            this.required = required;
        }
    }

    static String url(Database type, String host, int port, String database) {
        if (host == null || host.length() > 253 ||
                !host.matches("[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?")) {
            throw new IllegalArgumentException("HOST must be a DNS name or IPv4 address");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("PORT must be between 1 and 65535");
        }
        if (database == null || !database.matches("[A-Za-z0-9_][A-Za-z0-9_.-]*")) {
            throw new IllegalArgumentException("DATABASE must use letters, digits, underscore, dot or hyphen");
        }
        String address = host + ":" + port;
        return switch (type) {
            case POSTGRES -> "jdbc:postgresql://" + address + "/" + database
                    + "?sslmode=verify-full&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory"
                    + "&connectTimeout=10&socketTimeout=30";
            case SQL_SERVER -> "jdbc:sqlserver://" + address + ";databaseName=" + database
                    + ";encrypt=true;trustServerCertificate=false;fips=true;trustStoreType=BCFKS"
                    + ";trustStore=" + TRUST + ";trustStorePassword=changeit"
                    + ";sslProtocol=TLSv1.2;loginTimeout=10;socketTimeout=30000";
            case MARIA_DB -> "jdbc:mariadb://" + address + "/" + database
                    + "?sslMode=verify-full&trustStoreType=BCFKS&trustStore=" + TRUST
                    + "&trustStorePassword=changeit&enabledSslProtocolSuites=TLSv1.2,TLSv1.3"
                    + "&allowLocalInfile=false&connectTimeout=10000&socketTimeout=30000";
        };
    }

    static void checkProfiles() throws Exception {
        if (!SSLContext.getDefault().getProvider().getName().equals("BCJSSE")) {
            throw new IllegalStateException("JDBC checks require the BCJSSE default TLS context");
        }
        for (Database type : Database.values()) {
            Class.forName(type.driver);
            String url = url(type, "database.example.invalid", type.port, "semoss");
            var driver = DriverManager.getDriver(url);
            if (!driver.getClass().getName().equals(type.driver) ||
                    driver.getMajorVersion() != type.major || driver.getMinorVersion() != type.minor) {
                throw new IllegalStateException("Unexpected JDBC driver for " + type);
            }
            Map<String, String> parsed = new java.util.HashMap<>();
            for (var property : driver.getPropertyInfo(url, new Properties())) {
                parsed.put(property.name, property.value);
            }
            for (var entry : type.required.entrySet()) {
                String actual = parsed.get(entry.getKey());
                if (actual == null || !entry.getValue().equalsIgnoreCase(actual.replace('_', '-'))) {
                    throw new IllegalStateException(type + " did not retain required property " + entry.getKey());
                }
            }
            System.out.println("PASS: " + type + " driver and strict TLS URL properties (offline)");
        }
    }

    static void verifySession(java.sql.Connection connection, Database type) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.setQueryTimeout(30);
            try (var result = statement.executeQuery("SELECT 1")) {
                if (!result.next() || result.getInt(1) != 1) {
                    throw new SQLException("SQL probe returned an unexpected result", "HY000");
                }
            }
            String query = switch (type) {
                case POSTGRES -> "SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()";
                case SQL_SERVER -> "SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id=@@SPID";
                case MARIA_DB -> "SHOW SESSION STATUS LIKE 'Ssl_cipher'";
            };
            try (var result = statement.executeQuery(query)) {
                boolean encrypted = result.next() && switch (type) {
                    case POSTGRES -> result.getBoolean(1);
                    case SQL_SERVER -> "TRUE".equalsIgnoreCase(result.getString(1));
                    case MARIA_DB -> result.getString(2) != null && !result.getString(2).isBlank();
                };
                if (!encrypted) {
                    throw new SQLException("Server did not confirm encrypted transport", "08004");
                }
            }
        }
    }

    static String required(Properties input, String key) {
        String value = input.getProperty(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Missing required input: " + key);
        }
        return value;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1 || !(args[0].equals("--templates") || args[0].equals("--live"))) {
            throw new IllegalArgumentException("Usage: JdbcTls --templates|--live");
        }
        checkProfiles();
        if (args[0].equals("--templates")) {
            for (Database type : Database.values()) {
                System.out.println(type + "\t" + url(type, "database.example.invalid", type.port, "semoss"));
            }
            return;
        }
        Properties input = new Properties();
        input.load(System.in);
        Database type = Database.valueOf(required(input, "RDBMS_TYPE"));
        int port = Integer.parseInt(input.getProperty("PORT", Integer.toString(type.port)));
        String url = url(type, required(input, "HOST"), port, required(input, "DATABASE"));
        Properties credentials = new Properties();
        credentials.setProperty("user", required(input, "USERNAME"));
        credentials.setProperty("password", required(input, "PASSWORD"));
        input.clear();
        try (var connection = DriverManager.getConnection(url, credentials)) {
            verifySession(connection, type);
            System.out.println("PASS: " + type + " live SELECT 1 and server-reported encrypted session");
        } catch (SQLException error) {
            // Driver messages can contain endpoints or authentication details.
            System.err.println("JDBC validation failed; SQLState=" + error.getSQLState()
                    + ", vendorCode=" + error.getErrorCode()
                    + ". Review approved server-side diagnostics; no credentials were printed.");
            System.exit(1);
        } finally {
            credentials.clear();
        }
    }
}
