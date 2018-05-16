package prerna.util.sql;

import java.sql.SQLException;
import java.util.List;

import prerna.engine.impl.rdbms.RdbmsConnectionHelper;

public class AnsiSqlQueryUtil extends SQLQueryUtil {

	private DB_TYPE dbType = null;
	private String hostname;
	private String port;
	private String schema;
	private String username;
	private String password;
	private String connectionUrl;
	
	public AnsiSqlQueryUtil() {
		
	}
	
	public AnsiSqlQueryUtil(DB_TYPE dbType, String hostname, String port, String schema, String username, String password) {
		this.dbType = dbType;
		this.hostname = hostname;
		this.port = port;
		this.schema = schema;
		this.username = username;
		this.password = password;
		try {
			this.connectionUrl = RdbmsConnectionHelper.getConnectionUrl(dbType.name(), hostname, port, schema, "");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void setDbType(DB_TYPE dbType) {
		this.dbType = dbType;
	}
	
	@Override
	public DB_TYPE getDatabaseType() {
		return this.dbType;
	}

	@Override
	public String getConnectionURL(String baseFolder, String dbname) {
		return this.connectionUrl;
	}
	
	public String getDefaultDBUserName(){
		return this.username;
	}

	public String getDefaultDBPassword(){
		return this.password;
	}

	@Override
	public String getDatabaseDriverClassName() {
		return RdbmsConnectionHelper.getDriver(this.dbType.name());
	}
	
	@Override
	public String getDialectIndexInfo(String indexName, String dbName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDialectFullOuterJoinQuery(boolean distinct, String selectors, List<String> rightJoinsArr,
			List<String> leftJoinsArr, List<String> joinsArr, String filters, int limit, String groupBy) {
		// TODO Auto-generated method stub
		return null;
	}

}
