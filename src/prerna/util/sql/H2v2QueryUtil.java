package prerna.util.sql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class H2v2QueryUtil extends H2QueryUtil{

	private static final Logger classLogger = LogManager.getLogger(H2v2QueryUtil.class);

	
	H2v2QueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.H2_V2_DB);
	}
	
	H2v2QueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.H2_V2_DB);
	}
	
	
	@Override
	public String columnDetailsQuery(String tableName, String columnName, String database, String schema) {
		// do not need to use the schema
		return "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName.toUpperCase() + "' AND COLUMN_NAME='" + columnName.toUpperCase() + "';";
	}
	
	@Override
	public String getAllColumnDetails(String tableName, String database, String schema) {
		// do not need to use the schema
		return "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName.toUpperCase() + "';";
	}
	
	
	@Override
	public String getIndexDetails(String indexName, String tableName, String database, String schema) {
		// do not use the schema
		return "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE INDEX_NAME='" + indexName.toUpperCase() + "' AND TABLE_NAME='" + tableName.toUpperCase() + "';";
	}
	
	@Override
	public String allIndexForTableQuery(String tableName, String database, String schema) {
		// do not need to use the schema
		return "SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME='" + tableName.toUpperCase() + "';";
	}
	
}
