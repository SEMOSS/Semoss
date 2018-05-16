package prerna.util.sql;

import java.util.List;

public class MySQLQueryUtil extends SQLQueryUtil {
	public static final String DATABASE_DRIVER = "com.mysql.jdbc.Driver";
	private String connectionBase = "jdbc:mysql://HOST:PORT/SCHEMA";
	private String connectionURL = "";

	public MySQLQueryUtil(){
		setDialect();
		super.setDefaultDbUserName("root");
		super.setDefaultDbPassword("password");
	}
	
	public MySQLQueryUtil(String hostname, String port, String schema, String username, String password) {
		setDialect(schema);
		connectionBase = connectionBase.replace("HOST", hostname).replace("SCHEMA", schema);
		if(port != null && !port.isEmpty()) {
			connectionBase = connectionBase.replace(":PORT", ":" + port);
		} else {
			connectionBase = connectionBase.replace(":PORT", "");
		}
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
	}
	
	public MySQLQueryUtil(String connectionURL, String username, String password) {
		setDialect();
		connectionBase = connectionURL;
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
	}
	
	private void setDialect() {
		super.setDialectAllTables(" SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ");
		super.setDialectAllColumns(" SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ");
		super.setResultAllTablesTableName("TABLE_NAME");//
		super.setResultAllColumnsColumnName("COLUMN_NAME");
		super.setResultAllColumnsColumnType("DATA_TYPE");
		super.setDialectOuterJoinLeft(" LEFT OUTER JOIN ");
		super.setDialectOuterJoinRight(" RIGHT OUTER JOIN ");
	}
	
	private void setDialect(String schema) {
		setDialect();
		super.setDialectAllTables("SELECT table_name FROM information_schema.tables where table_schema='" + schema + "'");
		super.setDialectAllColumns(" SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_NAME = ");
	}

	@Override
	public SQLQueryUtil.DB_TYPE getDatabaseType(){
		return SQLQueryUtil.DB_TYPE.MYSQL;
	}
	public String getDialectAllColumns(String tableName){
		return super.getDialectAllColumns() + "'" + tableName + "'" ;
	}
	@Override
	public String getDialectAllIndexesInDB(String schema){
		return super.getDialectAllIndexesInDB();
	}
	
	//jdbc:mysql://<hostname>[:port]/<DBname>?user=username&password=pw
	@Override
	public String getConnectionURL(String baseFolder, String dbname){
		return connectionBase;
	}
	
	@Override
	public String getTempConnectionURL(){
		return connectionBase;
	}

	@Override
	public String getDatabaseDriverClassName(){
		return DATABASE_DRIVER;
	}

	@Override
	public String getDialectIndexInfo(String indexName, String dbName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDialectFullOuterJoinQuery(boolean distinct,
			String selectors, List<String> rightJoinsArr,
			List<String> leftJoinsArr, List<String> joinsArr, String filters,
			int limit, String groupBy) {
		// TODO Auto-generated method stub
		return null;
	}
}
