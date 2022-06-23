package prerna.util.sql;

import java.util.Map;
import java.util.Properties;

public class MySQLQueryUtil extends AnsiSqlQueryUtil {
	
	MySQLQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.MYSQL);
	}
	
	MySQLQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.MYSQL);
	}
	
	@Override
	public String buildConnectionString(Map<String, Object> configMap) throws RuntimeException {
		if(configMap.isEmpty()){
			throw new RuntimeException("Configuration map is empty");
		}
		
		String connectionString = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_STRING);
		if(connectionString != null && !connectionString.isEmpty()) {
			return connectionString;
		}
		
		String urlPrefix = this.dbType.getUrlPrefix();
		String hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if(hostname == null || hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		String port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		String schema = (String) configMap.get(AbstractSqlQueryUtil.SCHEMA);
		if(schema == null || schema.isEmpty()) {
			throw new RuntimeException("Must pass in schema name");
		}
		
		connectionString = urlPrefix+"://"+hostname+port+"/"+schema; 
		
		String additonalProperties = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);
		if(additonalProperties != null && !additonalProperties.isEmpty()) {
			if(!additonalProperties.startsWith(";") && !additonalProperties.startsWith("&")) {
				connectionString += ";" + additonalProperties;
			} else {
				connectionString += additonalProperties;
			}
		}
		
		return connectionString;
	}

	@Override
	public String buildConnectionString(Properties prop) throws RuntimeException {
		if(prop == null){
			throw new RuntimeException("Properties ojbect is null");
		}
		
		String connectionString = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_STRING);
		if(connectionString != null && !connectionString.isEmpty()) {
			return connectionString;
		}
		
		String urlPrefix = this.dbType.getUrlPrefix();
		String hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if(hostname == null || hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		String port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		String schema = (String) prop.get(AbstractSqlQueryUtil.SCHEMA);
		if(schema == null || schema.isEmpty()) {
			throw new RuntimeException("Must pass in schema name");
		}
		
		connectionString = urlPrefix+"://"+hostname+port+"/"+schema;
		
		String additonalProperties = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);
		if(additonalProperties != null && !additonalProperties.isEmpty()) {
			if(!additonalProperties.startsWith(";") && !additonalProperties.startsWith("&")) {
				connectionString += ";" + additonalProperties;
			} else {
				connectionString += additonalProperties;
			}
		}
		
		return connectionString;
	}
	
	@Override
	public String getEscapeKeyword(String selector) {
		return "`" + selector + "`";
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	@Override
	public boolean allowIfExistsModifyColumnSyntax() {
		return false;
	}
	
	@Override
	public boolean allowIfExistsIndexSyntax() {
		return false;
	}

	/////////////////////////////////////////////////////////////////////////////////////
	
	@Override
	public String modColumnType(String tableName, String columnName, String dataType) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(columnName)) {
			columnName = getEscapeKeyword(columnName);
		}
		return "ALTER TABLE " + tableName + " MODIFY COLUMN " + columnName + " " + dataType + ";";
	}
	
	@Override
	public String dropIndex(String indexName, String tableName) {
		// should escape keywords
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		return "ALTER TABLE " + tableName + " DROP INDEX " + indexName;
	}
		
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Query database scripts
	 */
	
	@Override
	public String tableExistsQuery(String tableName, String database, String schema) {
		return "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='" + schema + "' AND TABLE_NAME='" + tableName + "';";
	}
	
	@Override
	public String tableConstraintExistsQuery(String constraintName, String tableName, String database, String schema) {
		return "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME = '" + constraintName + "' AND TABLE_NAME = '" + tableName + "' AND CONSTRAINT_SCHEMA='" + schema + "'";
	}

	@Override
	public String referentialConstraintExistsQuery(String constraintName, String database, String schema) {
		return "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_NAME = '" + constraintName + "' AND CONSTRAINT_SCHEMA='" + schema + "'";
	}
	
	@Override
	public String getAllColumnDetails(String tableName, String database, String schema) {
		return "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='" + schema + "' AND TABLE_NAME = '" + tableName + "';";
	}
	
	@Override
	public String columnDetailsQuery(String tableName, String columnName, String database, String schema) {
		return "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='" + schema + "' AND TABLE_NAME = '" + tableName + "' AND COLUMN_NAME='" + columnName + "';";
	}
	
	@Override
	public String getIndexList(String database, String schema) {
		return "SELECT DISTINCT INDEX_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='" + schema + "';";
	}
	
	@Override
	public String getIndexDetails(String indexName, String tableName, String database, String schema) {
		return "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='" + schema + "' AND INDEX_NAME='" + indexName  + "' AND TABLE_NAME='" + tableName + "';";
	}
	
	@Override
	public String allIndexForTableQuery(String tableName, String database, String schema) {
		return "SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='" + schema + "' AND TABLE_NAME='" + tableName + "';";
	}
}
