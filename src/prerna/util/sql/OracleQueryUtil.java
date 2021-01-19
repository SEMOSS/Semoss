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
		
		String service = (String) configMap.get(AbstractSqlQueryUtil.SERVICE);
		if(service == null || service.isEmpty()) {
			throw new RuntimeException("Must pass in a sid / service name");
		}
		
		connectionString = urlPrefix+":@"+hostname+port+"/"+service;
		
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
		
		String service = (String) prop.get(AbstractSqlQueryUtil.SERVICE);
		if(service == null || service.isEmpty()) {
			throw new RuntimeException("Must pass in a sid / service name");
		}
		
		connectionString = urlPrefix+":@"+hostname+port+"/"+service; 
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
