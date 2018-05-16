package prerna.util.sql;

public class MySQLQueryUtil extends SQLQueryUtil {
	
	public static final String DATABASE_DRIVER = "com.mysql.jdbc.Driver";
	private String connectionBase = "jdbc:mysql://HOST:PORT/SCHEMA";

	public MySQLQueryUtil(){
		super.setDefaultDbUserName("root");
		super.setDefaultDbPassword("password");
	}
	
	public MySQLQueryUtil(String hostname, String port, String schema, String username, String password) {
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
		connectionBase = connectionURL;
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
	}
	
	@Override
	public SQLQueryUtil.DB_TYPE getDatabaseType(){
		return SQLQueryUtil.DB_TYPE.MYSQL;
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
	public String getDatabaseDriverClassName(){
		return DATABASE_DRIVER;
	}

	@Override
	public String getDialectIndexInfo(String indexName, String dbName) {
		// TODO Auto-generated method stub
		return null;
	}
}
