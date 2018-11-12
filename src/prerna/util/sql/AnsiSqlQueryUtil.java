package prerna.util.sql;

import java.sql.SQLException;

import prerna.engine.impl.rdbms.RdbmsConnectionHelper;

public class AnsiSqlQueryUtil extends SQLQueryUtil {

	private RdbmsTypeEnum dbType = null;
	private String hostname;
	private String port;
	private String schema;
	private String username;
	private String password;
	private String connectionUrl;
	
	public AnsiSqlQueryUtil() {
		
	}
	
	public AnsiSqlQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
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

	public void setDbType(RdbmsTypeEnum dbType) {
		this.dbType = dbType;
	}
	
	@Override
	public RdbmsTypeEnum getDatabaseType() {
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
		return dbType.getDriver();
	}
	
	@Override
	public String getDialectIndexInfo(String indexName, String dbName) {
		// TODO Auto-generated method stub
		return null;
	}

}
