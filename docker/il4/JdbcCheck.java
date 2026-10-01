import java.sql.DriverManager;

public final class JdbcCheck {
    public static void main(String[] args) throws Exception {
        JdbcTls.checkProfiles();
        Class.forName("org.sqlite.JDBC");
        try (var connection = DriverManager.getConnection("jdbc:sqlite::memory:");
             var statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE probe (amount INTEGER)");
            statement.executeUpdate("INSERT INTO probe VALUES (2), (4), (6)");
            try (var result = statement.executeQuery("SELECT COUNT(*), SUM(amount) FROM probe")) {
                if (!result.next() || result.getInt(1) != 3 || result.getInt(2) != 12) {
                    throw new IllegalStateException("SQLite JDBC calculation failed");
                }
            }
        }
        System.out.println("PASS: SQLite JDBC native loading and SQL calculation");
    }
}
