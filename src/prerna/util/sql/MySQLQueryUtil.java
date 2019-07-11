package prerna.util.sql;

public class MySQLQueryUtil extends AnsiSqlQueryUtil {
	
	MySQLQueryUtil() {
		super();
	}
	
	MySQLQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
	}
	
	MySQLQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
	@Override
	public String getEscapeKeyword(String selector) {
		return "`" + selector + "`";
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	@Override
	public boolean allowIfExistsModifyColumnSyntax() {
		return false;
	}
	
	@Override
	public boolean allowIfExistsIndexSyntax() {
		return false;
	}

	/////////////////////////////////////////////////////////////////////////////////////
	
	@Override
	public String modColumnType(String tableName, String columnName, String dataType) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "ALTER TABLE " + tableName + " MODIFY COLUMN " + columnName + " " + dataType + ";";
	}
	
	@Override
	public String dropIndex(String indexName, String tableName) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		return "ALTER TABLE " + tableName + " DROP INDEX " + indexName;
	}
		
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Query database scripts
	 */
	
	@Override
	public String tableExistsQuery(String tableName, String schema) {
		return "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='" + schema + "' AND TABLE_NAME='" + tableName + "';";
	}
	
	@Override
	public String columnDetailsQuery(String tableName, String columnName, String schema) {
		return "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='" + schema + "' AND TABLE_NAME = '" + tableName + "' AND COLUMN_NAME='" + columnName + "';";
	}
	
	@Override
	public String getIndexList(String schema) {
		return "SELECT DISTINCT INDEX_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='" + schema + "';";
	}
	
	@Override
	public String getIndexDetails(String indexName, String tableName, String schema) {
		return "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='" + schema + "' AND INDEX_NAME='" + indexName  + "' AND TABLE_NAME='" + tableName + "';";
	}
	
	@Override
	public String allIndexForTableQuery(String tableName, String schema) {
		return "SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='" + schema + "' AND TABLE_NAME='" + tableName + "';";
	}

}
