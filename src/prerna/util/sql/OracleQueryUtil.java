package prerna.util.sql;

import java.util.Map;
import java.util.Properties;

public class OracleQueryUtil extends AnsiSqlQueryUtil {
	
	OracleQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.ORACLE);
	}
	
	OracleQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.ORACLE);
	}
	
	OracleQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
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
		
		connectionString = urlPrefix+":@"+hostname+port+"/"+schema;
		
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
		
		connectionString = urlPrefix+":@"+hostname+port+"/"+schema; 
		return connectionString;
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
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	@Override
	public boolean allowIfExistsModifyColumnSyntax() {
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
		return "ALTER TABLE " + tableName + " MODIFY " + columnName + " " + dataType + ";";
	}
	
	@Override
	public String dropIndex(String indexName, String tableName) {
		return "DROP INDEX " + indexName;
	}
}
