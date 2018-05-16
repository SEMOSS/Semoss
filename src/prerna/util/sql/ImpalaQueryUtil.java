package prerna.util.sql;

import java.util.List;

public class ImpalaQueryUtil extends SQLQueryUtil {
	public static final String DATABASE_DRIVER = "com.cloudera.impala.jdbc4.Driver";
	private String connectionBase = "jdbc:impala://HOST:PORT/SCHEMA";
	private String connectionURL = "";

	public ImpalaQueryUtil(){
		setDialect();
		super.setDefaultDbUserName("root");
		super.setDefaultDbPassword("password");
	}
	
	public ImpalaQueryUtil(String hostname, String port, String schema, String username, String password) {
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
	
	public ImpalaQueryUtil(String connectionURL, String username, String password) {
		setDialect();
		connectionBase = connectionURL;
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
	}
	
	private void setDialect() {
		super.setDialectAllTables(" SHOW TABLES " );
		super.setDialectAllColumns(" DESCRIBE ");
		super.setResultAllTablesTableName("name");//
		super.setResultAllColumnsColumnName("name");
		super.setResultAllColumnsColumnType("type");
		super.setDialectOuterJoinLeft(" LEFT OUTER JOIN ");
		super.setDialectOuterJoinRight(" RIGHT OUTER JOIN ");
		super.setResultAllColumnsColumnType("row_count");

	}
	
	private void setDialect(String schema) {
		setDialect();
		super.setDialectAllTables("SELECT table_name FROM information_schema.tables where table_schema='" + schema + "'");
		super.setDialectAllColumns(" SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + schema + "' AND TABLE_NAME = ");
	}

	@Override
	public SQLQueryUtil.DB_TYPE getDatabaseType(){
		return SQLQueryUtil.DB_TYPE.IMPALA;
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
