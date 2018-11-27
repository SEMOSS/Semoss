package prerna.util.sql;

public class TibcoQueryUtil extends SQLQueryUtil {

	private String connectionBase = "jdbc:compositesw:dbapi@HOST:PORT?SCHEMA";

	public TibcoQueryUtil(){
		super.setDefaultDbUserName("root");
		super.setDefaultDbPassword("password");
	}
	
	public TibcoQueryUtil(String hostname, String port, String schema, String username, String password) {
		connectionBase = connectionBase.replace("HOST", hostname).replace("SCHEMA", schema);
		if(port != null && !port.isEmpty()) {
			connectionBase = connectionBase.replace(":PORT", ":" + port);
		} else {
			connectionBase = connectionBase.replace(":PORT", "");
		}
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
	}
	
	public TibcoQueryUtil(String connectionURL, String username, String password) {
		connectionBase = connectionURL;
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
	}
	
	@Override
	public RdbmsTypeEnum getDatabaseType(){
		return RdbmsTypeEnum.TIBCO;
	}
	
	@Override
	public String getDialectAllIndexesInDB(String schema){
		return super.getDialectAllIndexesInDB();
	}
	
	@Override
	public String getConnectionURL(String baseFolder,String dbname){
		return connectionBase;
	}
	
	@Override
	public String getDatabaseDriverClassName(){
		return RdbmsTypeEnum.TIBCO.getDriver();
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
			query = query.append(" FETCH NEXT " + limit + " ROWS ONLY ");
		}
		return query;
	}
}
