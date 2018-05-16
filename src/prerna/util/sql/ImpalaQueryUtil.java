package prerna.util.sql;

public class ImpalaQueryUtil extends SQLQueryUtil {
	
	public static final String DATABASE_DRIVER = "com.cloudera.impala.jdbc4.Driver";
	private String connectionBase = "jdbc:impala://HOST:PORT/SCHEMA";

	public ImpalaQueryUtil(){
		super.setDefaultDbUserName("root");
		super.setDefaultDbPassword("password");
	}
	
	public ImpalaQueryUtil(String hostname, String port, String schema, String username, String password) {
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
		connectionBase = connectionURL;
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
	}
	
	@Override
	public SQLQueryUtil.DB_TYPE getDatabaseType(){
		return SQLQueryUtil.DB_TYPE.IMPALA;
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
