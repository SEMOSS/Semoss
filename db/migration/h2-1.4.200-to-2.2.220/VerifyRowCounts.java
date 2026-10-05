import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * Re-runs the per-table "-- N +/- SELECT COUNT(*) FROM SCHEMA.TABLE;"
 * assertions that org.h2.tools.Script embeds as comments in its dump output,
 * against a live database, and fails if any count doesn't match. Invoked by
 * migrate-h2-database.sh after RunScript so a silent partial replay can't
 * pass as a successful migration.
 *
 * Usage: java -cp h2-2.2.220.jar VerifyRowCounts.java <jdbcUrl> <assertionsFile>
 * assertionsFile lines are "<expectedCount>|<query>", one per table.
 */
public class VerifyRowCounts {
	public static void main(String[] args) throws Exception {
		String jdbcUrl = args[0];
		List<String> lines = Files.readAllLines(Path.of(args[1]));
		boolean allOk = true;
		try (Connection conn = DriverManager.getConnection(jdbcUrl, "sa", "")) {
			try (Statement stmt = conn.createStatement()) {
				for (String line : lines) {
					if (line.isBlank()) {
						continue;
					}
					int sep = line.indexOf('|');
					long expected = Long.parseLong(line.substring(0, sep).trim());
					String query = line.substring(sep + 1).trim();
					try (ResultSet rs = stmt.executeQuery(query)) {
						rs.next();
						long actual = rs.getLong(1);
						if (actual == expected) {
							System.out.println("OK   " + query + " = " + actual);
						} else {
							System.out.println("FAIL " + query + " expected=" + expected + " actual=" + actual);
							allOk = false;
						}
					}
				}
			}
		}
		if (!allOk) {
			System.exit(1);
		}
	}
}
