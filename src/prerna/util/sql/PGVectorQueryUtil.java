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
				+ "ID INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY, "
				+ "EMBEDDING VECTOR, "
				+ "SOURCE TEXT, "
				+ "MODALITY TEXT, "
				+ "DIVIDER TEXT, "
				+ "PART TEXT, "
				+ "TOKENS TEXT, "
				+ "CONTENT TEXT "
				+ ");";
	}
	
	public String createEmbeddingsMetadataTable(String table) {
		return "CREATE TABLE IF NOT EXISTS "+table+"("
				+ "ID INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY, "
				+ "SOURCE TEXT, "
				+ "ATTRIBUTE TEXT, "
				+ "STR_VALUE TEXT, "
				+ "INT_VALUE INTEGER, "
				+ "NUM_VALUE NUMERIC(18,4), "
				+ "BOOL_VALUE BOOLEAN, "
				+ "DATE_VAL DATE, "
				+ "TIMESTAMP_VAL TIMESTAMP "
				+ ");";
	}

	public String addVectorExtension() {
		return "CREATE EXTENSION IF NOT EXISTS vector;";
	}

}
