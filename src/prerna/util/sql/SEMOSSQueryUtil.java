package prerna.util.sql;

import java.util.Map;
import java.util.Properties;

public class SEMOSSQueryUtil extends AnsiSqlQueryUtil {

	private String protocol = null;
	private String endpoint = null;
	private String subURL = null;
	
	private String projectId = null;
	private String insightId = null;
	
	SEMOSSQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.SEMOSS);
	}
	
	SEMOSSQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.SEMOSS);
	}
	
	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if(configMap == null || configMap.isEmpty()){
			throw new RuntimeException("Configuration map is null or empty");
		}
		
		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(hostname == null || hostname.isEmpty())
			) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		this.port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "443";
		}
		
		this.projectId = (String) configMap.get(AbstractSqlQueryUtil.PROJECT);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.projectId == null || this.projectId.isEmpty())
				){
			throw new RuntimeException("Must pass in project id");
		}
		
		this.insightId = (String) configMap.get(AbstractSqlQueryUtil.INSIGHT);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.insightId == null || this.insightId.isEmpty())
				){
			throw new RuntimeException("Must pass in insight id");
		}
		
		this.protocol = (String) configMap.get(AbstractSqlQueryUtil.PROTOCOL);
		if (this.protocol == null || this.protocol.isEmpty()) {
			this.protocol = "https";
		}
		
		this.endpoint = (String) configMap.get(AbstractSqlQueryUtil.ENDPOINT);
		if (this.endpoint == null || this.endpoint.isEmpty()) {
			this.endpoint = "Monolith";
		}
		
		this.subURL = (String) configMap.get(AbstractSqlQueryUtil.SUB_URL);
		if(this.subURL == null) {
			this.subURL = "";
		}
		
		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+":"+this.hostname+port
					+";sub_url="+subURL+";endpoint="+this.endpoint+";protocol="+this.protocol
					+";project="+projectId+";insight="+insightId;
			
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
		
		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(hostname == null || hostname.isEmpty())
			) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		this.port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "443";
		}
		
		this.projectId = (String) prop.get(AbstractSqlQueryUtil.PROJECT);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.projectId == null || this.projectId.isEmpty())
				){
			throw new RuntimeException("Must pass in project id");
		}
		
		this.insightId = (String) prop.get(AbstractSqlQueryUtil.INSIGHT);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.insightId == null || this.insightId.isEmpty())
				){
			throw new RuntimeException("Must pass in insight id");
		}
		
		this.protocol = (String) prop.get(AbstractSqlQueryUtil.PROTOCOL);
		if (this.protocol == null || this.protocol.isEmpty()) {
			this.protocol = "https";
		}
		
		this.endpoint = (String) prop.get(AbstractSqlQueryUtil.ENDPOINT);
		if (this.endpoint == null || this.endpoint.isEmpty()) {
			this.endpoint = "Monolith";
		}
		
		this.subURL = (String) prop.get(AbstractSqlQueryUtil.SUB_URL);
		if(this.subURL == null) {
			this.subURL = "";
		}
		
		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+":"+this.hostname+port
					+";sub_url="+subURL+";endpoint="+this.endpoint+";protocol="+this.protocol
					+";project="+projectId+";insight="+insightId;
			
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
		
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "443";
		}
		
		this.connectionUrl = this.dbType.getUrlPrefix()+":"+this.hostname+port
				+";sub_url="+subURL+";endpoint="+this.endpoint+";protocol="+this.protocol
				+";project="+projectId+";insight="+insightId;
		
		if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
			if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
				this.connectionUrl += ";" + this.additionalProps;
			} else {
				this.connectionUrl += this.additionalProps;
			}
		}
		
		return this.connectionUrl;
	}
	
	@Override
	public String getDatabaseMetadataCatalogFilter() {
		return this.projectId;
	}
	
	@Override
	public String getDatabaseMetadataSchemaFilter() {
		return this.insightId;
	}
}
