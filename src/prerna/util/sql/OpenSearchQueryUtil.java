package prerna.util.sql;

import java.util.Map;
import java.util.Properties;

public class OpenSearchQueryUtil extends AnsiSqlQueryUtil {
	
	OpenSearchQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.OPEN_SEARCH);
	}
	
	OpenSearchQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.OPEN_SEARCH);
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
		String httpType = (String) configMap.get(AbstractSqlQueryUtil.HTTP_TYPE);
		if(httpType == null || httpType.isEmpty()) {
			httpType = "https";
		}
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
		
		// example:
		// jdbc:elasticsearch://https://remote-host-name?auth=aws_sigv4&region=us-west-1
		connectionString = urlPrefix+"://"+httpType+"://"+hostname+port;
		
		String additonalProperties = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);
		if(additonalProperties != null && !additonalProperties.isEmpty()) {
			if(!additonalProperties.startsWith("?")) {
				connectionString += "?" + additonalProperties;
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
		String httpType = (String) prop.get(AbstractSqlQueryUtil.HTTP_TYPE);
		if(httpType == null || httpType.isEmpty()) {
			httpType = "https";
		}
		
		String port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		// example:
		// jdbc:es://http://server:3456/?timezone=UTC&page.size=250
		connectionString = urlPrefix+"://"+httpType+"://"+hostname+port;
		
		String additonalProperties = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);
		if(additonalProperties != null && !additonalProperties.isEmpty()) {
			if(!additonalProperties.startsWith("?")) {
				connectionString += "?" + additonalProperties;
			} else {
				connectionString += additonalProperties;
			}
		}
		
		return connectionString;
	}
	
}
