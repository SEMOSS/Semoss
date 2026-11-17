import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Makes sure the shared session table exists before Tomcat starts (SESSION_STORE=jdbc).
 *
 * Tomcat's DataSourceStore reads and writes the table but never creates it. This reads
 * the same SESSION_STORE_URL / SESSION_STORE_USER / SESSION_STORE_PASSWORD settings that
 * session_store.py validated, from the environment rather than the command line, creates
 * the table only if it is missing (so a database user with no CREATE privilege still
 * works once an administrator has made the table), and fails with a message that never
 * contains the user or password.
 *
 * The connection uses the image's providers and trust store, so
 * sslmode=verify-full with the DefaultJavaSSLFactory checks the RDS chain and host name.
 */
public final class SessionStoreInit {
    private static final String TABLE = "tomcat_sessions";
    // serializes first-start creation when several containers start at once
    private static final long LOCK_KEY = 727274L;

    public static void main(String[] args) throws Exception {
        String url = required("SESSION_STORE_URL");
        Properties properties = new Properties();
        properties.setProperty("user", required("SESSION_STORE_USER"));
        properties.setProperty("password", required("SESSION_STORE_PASSWORD"));
        properties.setProperty("connectTimeout", "10");
        properties.setProperty("socketTimeout", "30");
        Class.forName("org.postgresql.Driver");
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            connection.setAutoCommit(true);
            if (tableExists(connection)) {
                System.out.println("PASS: session table " + TABLE + " is present");
                return;
            }
            try (var lock = connection.createStatement()) {
                lock.execute("SELECT pg_advisory_lock(" + LOCK_KEY + ")");
                try {
                    if (!tableExists(connection)) {
                        try (var create = connection.createStatement()) {
                            create.executeUpdate("CREATE TABLE IF NOT EXISTS " + TABLE + " ("
                                    + "id VARCHAR(100) NOT NULL, app VARCHAR(255) NOT NULL, "
                                    + "valid CHAR(1) NOT NULL, maxinactive INT NOT NULL, "
                                    + "lastaccess BIGINT NOT NULL, data BYTEA, PRIMARY KEY (id, app))");
                            create.executeUpdate("CREATE INDEX IF NOT EXISTS " + TABLE + "_app_idx ON "
                                    + TABLE + " (app)");
                        }
                        System.out.println("PASS: session table " + TABLE + " created");
                    } else {
                        System.out.println("PASS: session table " + TABLE + " is present");
                    }
                } finally {
                    lock.execute("SELECT pg_advisory_unlock(" + LOCK_KEY + ")");
                }
            }
        } catch (SQLException error) {
            // the SQL state and message name the problem; neither carries the password
            System.err.println("Session store check failed: SQLState " + error.getSQLState() + ": "
                    + error.getMessage());
            System.exit(1);
        }
    }

    private static boolean tableExists(Connection connection) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
            statement.setString(1, TABLE);
            try (var result = statement.executeQuery()) {
                return result.next() && result.getBoolean(1);
            }
        }
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            System.err.println("Session store check failed: " + name + " is not set");
            System.exit(1);
        }
        return value;
    }
}
