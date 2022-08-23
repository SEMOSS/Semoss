package prerna.util.sql;

import java.util.Map;
import java.util.Properties;

public class AthenaQueryUtil extends AnsiSqlQueryUtil {

	private String region;
	private String accessKey;
	private String secretKey;
	private String output;
	
	AthenaQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.ATHENA);
	}
	
	AthenaQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.ATHENA);
	}
	
	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if(configMap == null || configMap.isEmpty()){
			throw new RuntimeException("Configuration map is null or empty");
		}
		
		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.region = (String) configMap.get(AbstractSqlQueryUtil.REGION);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.region == null || this.region.isEmpty()) 
				){
			throw new RuntimeException("Must pass in a region");
		}
		
		this.accessKey = (String) configMap.get(AbstractSqlQueryUtil.ACCESS_KEY);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.accessKey == null || this.accessKey.isEmpty()) 
				){
			throw new RuntimeException("Must pass in an access key");
		}
		
		this.secretKey = (String) configMap.get(AbstractSqlQueryUtil.SECRET_KEY);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.secretKey == null || this.secretKey.isEmpty()) 
				){
			throw new RuntimeException("Must pass in a secret key");
		}
		
		this.output = (String) configMap.get(AbstractSqlQueryUtil.OUTPUT);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.output == null || this.output.isEmpty()) 
				){
			throw new RuntimeException("Must pass in an S3 bucket location for query outputs to be stored");
		}
		
		this.schema = (String) configMap.get(AbstractSqlQueryUtil.SCHEMA);
		if(this.schema == null || this.schema.isEmpty()) {
			this.schema = "default";
		}

		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+"://AwsRegion="+this.region
					+";User="+this.accessKey+";Password="+this.secretKey
					+";S3OutputLocation="+this.output+";Schema="+this.schema;
			
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
		
		this.region = (String) prop.get(AbstractSqlQueryUtil.REGION);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.region == null || this.region.isEmpty()) 
				){
			throw new RuntimeException("Must pass in a region");
		}
		
		this.accessKey = (String) prop.get(AbstractSqlQueryUtil.ACCESS_KEY);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.accessKey == null || this.accessKey.isEmpty()) 
				){
			throw new RuntimeException("Must pass in an access key");
		}
		
		this.secretKey = (String) prop.get(AbstractSqlQueryUtil.SECRET_KEY);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.secretKey == null || this.secretKey.isEmpty()) 
				){
			throw new RuntimeException("Must pass in a secret key");
		}
		
		this.output = (String) prop.get(AbstractSqlQueryUtil.OUTPUT);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.output == null || this.output.isEmpty()) 
				){
			throw new RuntimeException("Must pass in an S3 bucket location for query outputs to be stored");
		}
		
		this.schema = (String) prop.get(AbstractSqlQueryUtil.SCHEMA);
		if(this.schema == null || this.schema.isEmpty()) {
			this.schema = "default";
		}
		
		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+"://AwsRegion="+this.region
					+";User="+this.accessKey+";Password="+this.secretKey
					+";S3OutputLocation="+this.output+";Schema="+this.schema;
			
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
		
		if(this.schema == null || this.schema.isEmpty()) {
			this.schema = "default";
		}
		
		this.connectionUrl = this.dbType.getUrlPrefix()+"://AwsRegion="+region
				+";User="+accessKey+";Password="+secretKey
				+";S3OutputLocation="+output+";Schema="+schema;
		
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
	public String getConnectionUserKey() {
		return AbstractSqlQueryUtil.ACCESS_KEY;
	}
	
	@Override
	public String getConnectionPasswordKey() {
		return AbstractSqlQueryUtil.SECRET_KEY;
	}
	
	@Override
	public String getUsername() {
		return this.accessKey;
	}
	
	@Override
	public String getPassword() {
		return this.secretKey;
	}
}
