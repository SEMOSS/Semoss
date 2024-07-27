package prerna.util.sql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PGVectorQueryUtil extends PostgresQueryUtil {
	
	private static final Logger classLogger = LogManager.getLogger(PGVectorQueryUtil.class);

	public PGVectorQueryUtil() {
		super();
	}

	public PGVectorQueryUtil(String connectionUrl, String username, String password) {
		super();
	}

	public String createEmbeddingsTable(String table) {
		return "CREATE TABLE IF NOT EXISTS "+table+"("
				+ "ID SERIAL PRIMARY KEY, "
				+ "EMBEDDING vector, "
				+ "SOURCE text, "
				+ "MODALITY text, "
				+ "DIVIDER text, "
				+ "PART text, "
				+ "TOKENS text, "
				+ "CONTENT text "
				+ ");";
	}

	public String addVectorExtension() {
		return "CREATE EXTENSION IF NOT EXISTS vector;";
	}

}
