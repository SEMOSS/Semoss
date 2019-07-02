package prerna.util.sql;

public class SQLiteQueryUtil extends AnsiSqlQueryUtil {

	SQLiteQueryUtil() {
		super();
	}
	
	SQLiteQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
	}
	
	SQLiteQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
	@Override
	public String getRegexLikeFunctionSyntax() {
		return "REGEXP";
	}

	@Override
	public boolean allowRedefineColumn() {
		return false;
	}
	
	@Override
	public boolean allowDropColumn() {
		return false;
	}
	
	@Override
	public boolean allowIfExistsModifyColumnSyntax() {
		return false;
	}
	
	@Override
	public String dropIndex(String indexName, String tableName) {
		return "DROP INDEX " + indexName;
	}
	
	@Override
	public String dropIndexIfExists(String indexName, String tableName) {
		// sqlite allows this for some reason...
		return "DROP INDEX IF EXISTS " + indexName;
	}
	
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Query database scripts
	 */
		
	@Override
	public String tableExistsQuery(String tableName, String schema) {
		// do not need to use the schema
		return "SELECT NAME, TYPE FROM SQLITE_MASTER WHERE TYPE='table' AND NAME='" + tableName + "';";
	}
	
	@Override
	public String getIndexList(String schema) {
		// do not need to use the schema
		return "SELECT DISTINCT NAME, TBL_NAME FROM SQLITE_MASTER WHERE TYPE='index';";
	}

	@Override
	public String getIndexDetails(String indexName, String tableName, String schema) {
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		return "SELECT DISTINCT TBL_NAME, null AS COLUMN FROM SQLITE_MASTER WHERE TYPE='index' AND NAME='" + indexName + "' AND TBL_NAME='" + tableName + "';";
	}
	
	@Override
	public String allIndexForTableQuery(String tableName, String schema) {
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN

		// do not need to use the schema
		// sadly, sqlite does not provide the columns for the index
		return "SELECT NAME, null AS COLUMN FROM SQLITE_MASTER WHERE TYPE='index' AND TBL_NAME='" + tableName + "';";
	}
}
