package prerna.util.sql;

import java.util.Map;
import java.util.Properties;

public class AthenaQueryUtil extends AnsiSqlQueryUtil {

	AthenaQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.ATHENA);
	}
	
	AthenaQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.ATHENA);
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
		String region = (String) configMap.get(AbstractSqlQueryUtil.REGION);
		if(region == null || region.isEmpty()) {
			throw new RuntimeException("Must pass in a region");
		}
		
		String accessKey = (String) configMap.get(AbstractSqlQueryUtil.ACCESS_KEY);
		if(accessKey == null || accessKey.isEmpty()) {
			throw new RuntimeException("Must pass in an access key");
		}
		
		String secretKey = (String) configMap.get(AbstractSqlQueryUtil.SECRET_KEY);
		if(secretKey == null || secretKey.isEmpty()) {
			throw new RuntimeException("Must pass in a secret key");
		}
		
		String output = (String) configMap.get(AbstractSqlQueryUtil.OUTPUT);
		if(output == null || output.isEmpty()) {
			throw new RuntimeException("Must pass in an S3 bucket location for query outputs to be stored");
		}
		
		connectionString = urlPrefix+"://AwsRegion="+region+";User="+accessKey+";Password="+secretKey+";S3OutputLocation="+output;
		
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
		String region = (String) prop.get(AbstractSqlQueryUtil.REGION);
		if(region == null || region.isEmpty()) {
			throw new RuntimeException("Must pass in a region");
		}
		
		String accessKey = (String) prop.get(AbstractSqlQueryUtil.ACCESS_KEY);
		if(accessKey == null || accessKey.isEmpty()) {
			throw new RuntimeException("Must pass in an access key");
		}
		
		String secretKey = (String) prop.get(AbstractSqlQueryUtil.SECRET_KEY);
		if(secretKey == null || secretKey.isEmpty()) {
			throw new RuntimeException("Must pass in a secret key");
		}
		
		String output = (String) prop.get(AbstractSqlQueryUtil.OUTPUT);
		if(output == null || output.isEmpty()) {
			throw new RuntimeException("Must pass in an S3 bucket location for query outputs to be stored");
		}
		
		connectionString = urlPrefix+"://AwsRegion="+region+";User="+accessKey+";Password="+secretKey+";S3OutputLocation="+output;
		
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
	
}
