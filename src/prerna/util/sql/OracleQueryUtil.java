package prerna.util.sql;

public class OracleQueryUtil extends SQLQueryUtil {

	private String connectionBase = "jdbc:oracle:thin:@HOST:PORT:SERVICE";

	public OracleQueryUtil(){
		super.setDefaultDbUserName("root");
		super.setDefaultDbPassword("password");
	}
	
	public OracleQueryUtil(String hostname, String port, String schema, String username, String password) {
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
		connectionBase = connectionURL;
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
	}
	
	@Override
	public RdbmsTypeEnum getDatabaseType(){
		return RdbmsTypeEnum.ORACLE;
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
	public String getDatabaseDriverClassName(){
		return RdbmsTypeEnum.ORACLE.getDriver();
	}

	@Override
	public String getDialectIndexInfo(String indexName, String dbName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {
		if(offset > 0) {
			query = query.append(" OFFSET " + offset + " ROWS ");
		}
		if(limit > 0) {
			query = query.append(" FETCH NEXT " + limit+" ROWS ONLY ");
		}
		return query;
	}
}
