package prerna.util.sql;

import java.util.List;

public class OracleQueryUtil extends SQLQueryUtil {
	public static final String DATABASE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private String connectionBase = "jdbc:oracle:thin:@HOST:PORT:SERVICE";

	public OracleQueryUtil(){
		setDialect();
		super.setDefaultDbUserName("root");
		super.setDefaultDbPassword("password");
	}
	
	public OracleQueryUtil(String hostname, String port, String schema, String username, String password) {
		setDialect();
		connectionBase = connectionBase.replace("HOST", hostname).replace("SERVICE", schema);
		if(port != null && !port.isEmpty()) {
			connectionBase = connectionBase.replace(":PORT", ":" + port);
		} else {
			connectionBase = connectionBase.replace(":PORT", "");
		}
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
	}
	
	public OracleQueryUtil(String connectionURL, String username, String password) {
		setDialect();
		connectionBase = connectionURL;
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
	}
	
	private void setDialect() {
		super.setDialectAllTables("SELECT DISTINCT OBJECT_NAME FROM USER_OBJECTS WHERE OBJECT_TYPE = 'TABLE' OR OBJECT_TYPE = 'VIEW'");
		super.setDialectAllColumns("SELECT COLUMN_NAME, DATA_TYPE FROM user_tab_cols WHERE TABLE_NAME=");
		super.setResultAllTablesTableName("TABLE_NAME");
		super.setResultAllColumnsColumnName("COLUMN_NAME");
		super.setResultAllColumnsColumnType("DATA_TYPE");
		super.setDialectOuterJoinLeft(" LEFT OUTER JOIN ");
		super.setDialectOuterJoinRight(" RIGHT OUTER JOIN ");
	}
	
//	private void setDialect(String schema) {
//		setDialect();
//	}

	@Override
	public SQLQueryUtil.DB_TYPE getDatabaseType(){
		return SQLQueryUtil.DB_TYPE.ORACLE;
	}
	
	public String getDialectAllColumns(String tableName){
		return super.getDialectAllColumns() + "'" + tableName.toUpperCase() + "'" ;
	}
	
	@Override
	public String getDialectAllIndexesInDB(String schema){
		return super.getDialectAllIndexesInDB();
	}
	
	//jdbc:oracle:thin:@<hostname>[:port]/<service or sid>[-schema name]
	@Override
	public String getConnectionURL(String baseFolder,String dbname){
		return connectionBase;// + "-" + dbname;
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
	
	@Override
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {
		if(offset > 0) {
			query = query.append(" OFFSET "+offset+" ROWS ");
		}
		
		if(limit > 0) {
			query = query.append(" FETCH NEXT " + limit+" ROWS ONLY ");
		}
		
		return query;
	}
}
