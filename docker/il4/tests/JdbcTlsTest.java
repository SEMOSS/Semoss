import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class JdbcTlsTest {
    static void require(boolean condition) {
        if (!condition) {
            throw new AssertionError("JDBC TLS assertion failed");
        }
    }

    static <T> T fake(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }

    static Connection connection(JdbcTls.Database type, boolean sqlOk, boolean encrypted,
                                 List<String> observed) {
        return fake(Connection.class, (proxy, method, args) -> {
            if (!method.getName().equals("createStatement")) {
                throw new AssertionError("Unexpected connection call: " + method.getName());
            }
            return fake(Statement.class, (statement, call, values) -> {
                if (call.getName().equals("setQueryTimeout")) {
                    require(values[0].equals(30));
                    return null;
                }
                if (call.getName().equals("close")) {
                    observed.add("statement closed");
                    return null;
                }
                if (!call.getName().equals("executeQuery")) {
                    throw new AssertionError("Unexpected statement call: " + call.getName());
                }
                String query = (String) values[0];
                observed.add(query);
                return fake(ResultSet.class, (result, read, columns) -> switch (read.getName()) {
                    case "next" -> true;
                    case "getInt" -> sqlOk ? 1 : 0;
                    case "getBoolean" -> encrypted;
                    case "getString" -> type == JdbcTls.Database.SQL_SERVER
                            ? (encrypted ? "TRUE" : "FALSE")
                            : (encrypted ? "TLS_AES_256_GCM_SHA384" : "");
                    case "close" -> {
                        observed.add("result closed");
                        yield null;
                    }
                    default -> throw new AssertionError("Unexpected result call: " + read.getName());
                });
            });
        });
    }

    static void testSessionChecks() throws SQLException {
        for (var type : JdbcTls.Database.values()) {
            for (boolean sqlOk : new boolean[]{true, false}) {
                for (boolean encrypted : new boolean[]{true, false}) {
                    List<String> observed = new ArrayList<>();
                    try {
                        JdbcTls.verifySession(connection(type, sqlOk, encrypted, observed), type);
                        require(sqlOk && encrypted);
                    } catch (SQLException expected) {
                        require(!sqlOk || !encrypted);
                        require(expected.getSQLState().equals(sqlOk ? "08004" : "HY000"));
                    }
                    require(observed.getFirst().equals("SELECT 1"));
                    require(observed.getLast().equals("statement closed"));
                    require(observed.contains("result closed"));
                    if (sqlOk) {
                        require(observed.size() == 5);
                        String tlsQuery = observed.get(2);
                        require(tlsQuery.equals(switch (type) {
                            case POSTGRES -> "SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()";
                            case SQL_SERVER -> "SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id=@@SPID";
                            case MARIA_DB -> "SHOW SESSION STATUS LIKE 'Ssl_cipher'";
                        }));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        testSessionChecks();
        for (var type : JdbcTls.Database.values()) {
            require(JdbcTls.url(type, "db.example.test", type.port, "semoss")
                    .contains("db.example.test:" + type.port));
        }
        for (String host : new String[]{null, "", "host;encrypt=false", "host/x", "host?sslmode=disable"}) {
            try {
                JdbcTls.url(JdbcTls.Database.POSTGRES, host, 5432, "semoss");
                throw new AssertionError("Accepted invalid host");
            } catch (IllegalArgumentException expected) {
                require(expected.getMessage().startsWith("HOST"));
            }
        }
        for (String database : new String[]{null, "", "db;encrypt=false", "db?sslMode=disable", "db/name"}) {
            try {
                JdbcTls.url(JdbcTls.Database.MARIA_DB, "host", 3306, database);
                throw new AssertionError("Accepted invalid database");
            } catch (IllegalArgumentException expected) {
                require(expected.getMessage().startsWith("DATABASE"));
            }
        }
        for (int port : new int[]{0, -1, 65536}) {
            try {
                JdbcTls.url(JdbcTls.Database.SQL_SERVER, "host", port, "semoss");
                throw new AssertionError("Accepted invalid port");
            } catch (IllegalArgumentException expected) {
                require(expected.getMessage().startsWith("PORT"));
            }
        }
        Properties input = new Properties();
        input.setProperty("USERNAME", "test");
        require(JdbcTls.required(input, "USERNAME").equals("test"));
        for (String value : new String[]{null, ""}) {
            if (value != null) input.setProperty("PASSWORD", value);
            try {
                JdbcTls.required(input, "PASSWORD");
                throw new AssertionError("Accepted missing password");
            } catch (IllegalArgumentException expected) {
                require(expected.getMessage().contains("PASSWORD"));
            }
        }
        try {
            JdbcTls.main(new String[]{});
            throw new AssertionError("Accepted missing mode");
        } catch (IllegalArgumentException expected) {
            require(expected.getMessage().startsWith("Usage"));
        }
        JdbcTls.main(new String[]{"--templates"});
        System.out.println("PASS: JDBC URL injection rejection, session checks and resource cleanup");
    }
}
