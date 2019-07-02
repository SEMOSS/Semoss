package prerna.util.sql;

public class RdbmsQueryUtilFactor {

	/**
	 * Get the appropriate query util class
	 * @param dbType
	 * @return
	 */
	public static AbstractRdbmsQueryUtil initialize(RdbmsTypeEnum dbType) {
		AbstractRdbmsQueryUtil queryUtil = null;
		if(dbType == RdbmsTypeEnum.H2_DB) {
			queryUtil = new H2QueryUtil();
		} else if(dbType == RdbmsTypeEnum.MARIADB){
			queryUtil = new MariaDbQueryUtil();
		} else if(dbType == RdbmsTypeEnum.SQLSERVER){
			queryUtil = new MicrosoftSqlServerUtil();
		} else if(dbType == RdbmsTypeEnum.MYSQL) {
			queryUtil = new MySQLQueryUtil();
		} else if(dbType == RdbmsTypeEnum.ORACLE) {
			queryUtil = new OracleQueryUtil();
		} else if(dbType == RdbmsTypeEnum.IMPALA) {
			queryUtil = new ImpalaQueryUtil();
		} else if(dbType == RdbmsTypeEnum.TIBCO) {
			queryUtil = new TibcoQueryUtil();
		} else if(dbType == RdbmsTypeEnum.SQLITE) {
			queryUtil = new SQLiteQueryUtil();
		}
		// base will work for most situations
		else {
			queryUtil = new AnsiSqlQueryUtil();
		}
		
		queryUtil.setDbType(dbType);
		return queryUtil;
	}
	
	public static AbstractRdbmsQueryUtil initialize(RdbmsTypeEnum dbtype, String connectionUrl, String username, String password) {
		AbstractRdbmsQueryUtil queryUtil = null;
		if(dbtype == RdbmsTypeEnum.H2_DB) {
			return new H2QueryUtil(connectionUrl, username, password);
		} else if(dbtype == RdbmsTypeEnum.SQLSERVER){
			return new MicrosoftSqlServerUtil(connectionUrl, username, password);
		} else if(dbtype == RdbmsTypeEnum.MYSQL) {
			return new MySQLQueryUtil(connectionUrl, username, password);
		} else if(dbtype == RdbmsTypeEnum.ORACLE) {
			return new OracleQueryUtil(connectionUrl, username, password);
		} else if(dbtype == RdbmsTypeEnum.IMPALA) {
			return new ImpalaQueryUtil(connectionUrl, username, password);
		} else if(dbtype == RdbmsTypeEnum.TIBCO) {
			return new TibcoQueryUtil(connectionUrl, username, password);
		} else if(dbtype == RdbmsTypeEnum.SQLITE) {
			return new SQLiteQueryUtil(connectionUrl, username, password);
		}
		// base will work for most situations
		else {
			queryUtil = new AnsiSqlQueryUtil(connectionUrl, username, password);
		}
		
		queryUtil.setDbType(dbtype);
		return queryUtil;
	}
	
	public static AbstractRdbmsQueryUtil initialize(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		AbstractRdbmsQueryUtil queryUtil = null;
		if(dbType == RdbmsTypeEnum.H2_DB) {
			queryUtil = new H2QueryUtil(dbType, hostname, port, schema, username, password);
		} else if(dbType == RdbmsTypeEnum.SQLSERVER){
			queryUtil = new MicrosoftSqlServerUtil(dbType, hostname, port, schema, username, password);
		} else if(dbType == RdbmsTypeEnum.MYSQL) {
			queryUtil = new MySQLQueryUtil(dbType, hostname, port, schema, username, password);
		} else if(dbType == RdbmsTypeEnum.ORACLE) {
			queryUtil = new OracleQueryUtil(dbType, hostname, port, schema, username, password);
		} else if(dbType == RdbmsTypeEnum.IMPALA) {
			queryUtil = new ImpalaQueryUtil(dbType, hostname, port, schema, username, password);
		} else if(dbType == RdbmsTypeEnum.TIBCO) {
			queryUtil = new TibcoQueryUtil(dbType, hostname, port, schema, username, password);
		} else if(dbType == RdbmsTypeEnum.SQLITE) {
			queryUtil = new SQLiteQueryUtil(dbType, hostname, port, schema, username, password);
		}
		// base will work for most situations
		else {
			queryUtil = new AnsiSqlQueryUtil(dbType, hostname, port, schema, username, password);
		}
		
		return queryUtil;
	}
	

}
