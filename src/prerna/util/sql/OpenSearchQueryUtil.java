package prerna.util.sql;

import java.util.Map;
import java.util.Properties;

public class OpenSearchQueryUtil extends AnsiSqlQueryUtil {
	
	private String httpType;
	
	OpenSearchQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.OPEN_SEARCH);
	}
	
	OpenSearchQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.OPEN_SEARCH);
	}
	
	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if(configMap == null || configMap.isEmpty()){
			throw new RuntimeException("Configuration map is null or empty");
		}
		
		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.httpType = (String) configMap.get(AbstractSqlQueryUtil.HTTP_TYPE);
		if(this.httpType == null || this.httpType.isEmpty()) {
			this.httpType = "https";
		}
		
		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.hostname == null || this.hostname.isEmpty())
			) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		this.port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);
		
		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			// example:
			// jdbc:opensearch://https://remote-host-name?auth=aws_sigv4&region=us-west-1
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+httpType+"://"+hostname+port;
			
			if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
				if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
					this.connectionUrl += ";" + this.additionalProps;
				} else {
					this.connectionUrl += this.additionalProps;
				}
			}
		}
		
		return this.connectionUrl;
	}

	@Override
	public String setConnectionDetailsFromSMSS(Properties prop) throws RuntimeException {
		if(prop == null || prop.isEmpty()){
			throw new RuntimeException("Properties object is null or empty");
		}
		
		this.connectionUrl = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.httpType = (String) prop.get(AbstractSqlQueryUtil.HTTP_TYPE);
		if(this.httpType == null || this.httpType.isEmpty()) {
			this.httpType = "https";
		}
		
		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.hostname == null || this.hostname.isEmpty())
			) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		this.port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);
		
		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			// example:
			// jdbc:opensearch://https://remote-host-name?auth=aws_sigv4&region=us-west-1
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+httpType+"://"+hostname+port;
			
			if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
				if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
					this.connectionUrl += ";" + this.additionalProps;
				} else {
					this.connectionUrl += this.additionalProps;
				}
			}
		}
		
		return this.connectionUrl;
	}

	@Override
	public String buildConnectionString() {
		if(this.connectionUrl != null && !this.connectionUrl.isEmpty()) {
			return this.connectionUrl;
		}
		
		if(this.hostname == null || this.hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		if(this.httpType == null || this.httpType.isEmpty()) {
			this.httpType = "https";
		}
		
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		// example:
		// jdbc:opensearch://https://remote-host-name?auth=aws_sigv4&region=us-west-1
		this.connectionUrl = this.dbType.getUrlPrefix()+"://"+httpType+"://"+hostname+port;
		
		if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
			if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
				this.connectionUrl += ";" + this.additionalProps;
			} else {
				this.connectionUrl += this.additionalProps;
			}
		}
		
		return this.connectionUrl;
	}
	
}
