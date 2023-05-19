package prerna.util.sql;

import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.BigQuerySqlInterpreter;

public class BigQueryQueryUtil extends AnsiSqlQueryUtil {

	private String projectId;
	private String oauthType;
	
	private String oauthServiceAcctEmail = null;
	private String oauthPrivateKeyPath = null;
	private String oauthAccessToken = null;
	private String oauthRefreshToken = null;
	private String oauthClientId = null;
	private String oauthClientSecret = null;
	
	@Override
	public void initTypeConverstionMap() {
		super.initTypeConverstionMap();
		typeConversionMap.put("INT", "INT64");
		typeConversionMap.put("LONG", "INT64");
		
		typeConversionMap.put("DOUBLE", "FLOAT64");
		typeConversionMap.put("NUMBER", "FLOAT64");
		typeConversionMap.put("FLOAT", "FLOAT64");

		typeConversionMap.put("DATE", "DATE");
		typeConversionMap.put("TIMESTAMP", "TIMESTAMP");
		
		typeConversionMap.put("STRING", "STRING");
		typeConversionMap.put("FACTOR", "STRING");

		typeConversionMap.put("BOOLEAN", "BOOL");
	}
	
	BigQueryQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.BIG_QUERY);
	}
	
	BigQueryQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.BIG_QUERY);
	}
	
	@Override
	public IQueryInterpreter getInterpreter(IEngine engine) {
		return new BigQuerySqlInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new BigQuerySqlInterpreter(frame);
	}
	
	@Override
	public String getVarcharDataTypeName() {
		return "STRING";
	}
	
	@Override
	public String getIntegerDataTypeName() {
		return "INT64";
	}
	
	@Override
	public String getDoubleDataTypeName() {
		return "FLOAT64";
	}
	
	@Override
	public String getBooleanDataTypeName() {
		return "BOOL";
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
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if(configMap == null || configMap.isEmpty()){
			throw new RuntimeException("Configuration map is null or empty");
		}
		
		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);

		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
				(this.hostname == null || this.hostname.isEmpty())
				){
			throw new RuntimeException("Must pass in a host");
		}
		
		this.port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		if(this.port == null || this.port.isEmpty()) {
			this.port = "443";
		}
		
		this.projectId = (String) configMap.get(AbstractSqlQueryUtil.PROJECT_ID);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
				(this.projectId == null || this.projectId.isEmpty())
				){
			throw new RuntimeException("Must pass in a project id");
		}
		
		this.oauthType = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_TYPE);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
				(this.oauthType == null || this.oauthType.isEmpty())
				){
			throw new RuntimeException("Must pass in an OAuth Type");
		}

		if(this.oauthType.equals("0")) {
			this.oauthServiceAcctEmail = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_SERVICE_ACCT_EMAIL);
			if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
					(this.oauthServiceAcctEmail == null || this.oauthServiceAcctEmail.isEmpty())
					){
				throw new RuntimeException("Must pass in an OAuth Service Account Email");
			}
			
			this.oauthPrivateKeyPath = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_PRIVATE_KEY_PATH);
			if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
					(this.oauthPrivateKeyPath == null || this.oauthPrivateKeyPath.isEmpty())
					){
				throw new RuntimeException("Must pass in an OAuth Private Key Path");
			}
		} else if(this.oauthType.equals("1")) {
			throw new IllegalArgumentException("This authentication protocol is not currently supported");
		} else if(this.oauthType.equals("2")) {
			this.oauthAccessToken = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_ACCESS_TOKEN);
			this.oauthRefreshToken = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_REFRESH_TOKEN);

			if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
					( (this.oauthAccessToken == null || this.oauthAccessToken.isEmpty()) &&
							(this.oauthRefreshToken == null || this.oauthRefreshToken.isEmpty())
					)
					){
				throw new RuntimeException("Must pass in an OAuth Access Token or Refresh Token");
			}
			
			this.oauthClientId = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_CLIENT_ID);
			if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
					(this.oauthClientId == null || this.oauthClientId.isEmpty())
					){
				throw new RuntimeException("Must pass in an OAuth Client ID");
			}
			
			this.oauthClientSecret = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_CLIENT_SECRET);
			if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
					(this.oauthClientSecret == null || this.oauthClientSecret.isEmpty())
					){
				throw new RuntimeException("Must pass in an OAuth Client Secret");
			}
		} else if(this.oauthType.equals("3")) {
			// don't require any other inputs
		} else {
			throw new IllegalArgumentException("OAuth Type can only contain one of the following values: '0', '1', '2', '3'");
		}
		
		this.schema = (String) configMap.get(AbstractSqlQueryUtil.SCHEMA);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
				(this.schema == null || this.schema.isEmpty())
				){
			throw new RuntimeException("Must pass in the default dataset");
		}

		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+":"+this.port+";ProjectId="+this.projectId
					+";OAuthType="+oauthType+";DefaultDataset="+this.schema;
			if(this.oauthType.equals("0")) {
				this.connectionUrl += ";OAuthServiceAcctEmail="+this.oauthServiceAcctEmail
						+ ";OAuthPvtKeyPath="+this.oauthPrivateKeyPath;
	
			} else if(this.oauthType.equals("1")) {
				throw new IllegalArgumentException("This authentication protocol is not currently supported");
			
			} else if(this.oauthType.equals("2")) {
				if(this.oauthAccessToken != null && !this.oauthAccessToken.isEmpty()) {
					this.connectionUrl += ";OAuthAccessToken="+this.oauthAccessToken;
				}
				if(this.oauthRefreshToken != null && !this.oauthRefreshToken.isEmpty()) {
					this.connectionUrl += ";OAuthRefreshToken="+this.oauthRefreshToken;
				}
				
				this.connectionUrl += ";OAuthClientId="+this.oauthClientId
						+ ";OAuthClientSecret="+this.oauthClientSecret;
			} else if(oauthType.equals("3")) {
				// don't require any other inputs
			}
			
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
	public String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException {
		if(prop == null || prop.isEmpty()){
			throw new RuntimeException("Properties object is null or empty");
		}
		
		this.connectionUrl = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_URL);
				
		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
				(this.hostname == null || this.hostname.isEmpty())
				){
			throw new RuntimeException("Must pass in a host");
		}
		
		this.port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		if(this.port == null || this.port.isEmpty()) {
			this.port = "443";
		}
		
		this.projectId = (String) prop.get(AbstractSqlQueryUtil.PROJECT_ID);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
				(this.projectId == null || this.projectId.isEmpty())
				){
			throw new RuntimeException("Must pass in a project id");
		}
		
		this.oauthType = (String) prop.get(AbstractSqlQueryUtil.OAUTH_TYPE);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
				(this.oauthType == null || this.oauthType.isEmpty())
				){
			throw new RuntimeException("Must pass in an OAuth Type");
		}

		if(this.oauthType.equals("0")) {
			this.oauthServiceAcctEmail = (String) prop.get(AbstractSqlQueryUtil.OAUTH_SERVICE_ACCT_EMAIL);
			if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
					(this.oauthServiceAcctEmail == null || this.oauthServiceAcctEmail.isEmpty())
					){
				throw new RuntimeException("Must pass in an OAuth Service Account Email");
			}
			
			this.oauthPrivateKeyPath = (String) prop.get(AbstractSqlQueryUtil.OAUTH_PRIVATE_KEY_PATH);
			if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
					(this.oauthPrivateKeyPath == null || this.oauthPrivateKeyPath.isEmpty())
					){
				throw new RuntimeException("Must pass in an OAuth Private Key Path");
			}
		} else if(this.oauthType.equals("1")) {
			throw new IllegalArgumentException("This authentication protocol is not currently supported");
		} else if(this.oauthType.equals("2")) {
			this.oauthAccessToken = (String) prop.get(AbstractSqlQueryUtil.OAUTH_ACCESS_TOKEN);
			this.oauthRefreshToken = (String) prop.get(AbstractSqlQueryUtil.OAUTH_REFRESH_TOKEN);

			if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
					( (this.oauthAccessToken == null || this.oauthAccessToken.isEmpty()) &&
							(this.oauthRefreshToken == null || this.oauthRefreshToken.isEmpty())
					)
					){
				throw new RuntimeException("Must pass in an OAuth Access Token or Refresh Token");
			}
			
			this.oauthClientId = (String) prop.get(AbstractSqlQueryUtil.OAUTH_CLIENT_ID);
			if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
					(this.oauthClientId == null || this.oauthClientId.isEmpty())
					){
				throw new RuntimeException("Must pass in an OAuth Client ID");
			}
			
			this.oauthClientSecret = (String) prop.get(AbstractSqlQueryUtil.OAUTH_CLIENT_SECRET);
			if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
					(this.oauthClientSecret == null || this.oauthClientSecret.isEmpty())
					){
				throw new RuntimeException("Must pass in an OAuth Client Secret");
			}
		} else if(this.oauthType.equals("3")) {
			// don't require any other inputs
		} else {
			throw new IllegalArgumentException("OAuth Type can only contain one of the following values: '0', '1', '2', '3'");
		}
		
		this.schema = (String) prop.get(AbstractSqlQueryUtil.SCHEMA);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) &&  
				(this.schema == null || this.schema.isEmpty())
				){
			throw new RuntimeException("Must pass in the default dataset");
		}

		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+":"+this.port+";ProjectId="+this.projectId
					+";OAuthType="+oauthType+";DefaultDataset="+this.schema;
			if(this.oauthType.equals("0")) {
				this.connectionUrl += ";OAuthServiceAcctEmail="+this.oauthServiceAcctEmail
						+ ";OAuthPvtKeyPath="+this.oauthPrivateKeyPath;
	
			} else if(this.oauthType.equals("1")) {
				throw new IllegalArgumentException("This authentication protocol is not currently supported");
			
			} else if(this.oauthType.equals("2")) {
				if(this.oauthAccessToken != null && !this.oauthAccessToken.isEmpty()) {
					this.connectionUrl += ";OAuthAccessToken="+this.oauthAccessToken;
				}
				if(this.oauthRefreshToken != null && !this.oauthRefreshToken.isEmpty()) {
					this.connectionUrl += ";OAuthRefreshToken="+this.oauthRefreshToken;
				}
				
				this.connectionUrl += ";OAuthClientId="+this.oauthClientId
						+ ";OAuthClientSecret="+this.oauthClientSecret;
			} else if(oauthType.equals("3")) {
				// don't require any other inputs
			}
			
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
		
		if(this.port == null || this.port.isEmpty()) {
			this.port = "443";
		}
		
		this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+":"+this.port+";ProjectId="+this.projectId
				+";OAuthType="+this.oauthType+";DefaultDataset="+this.schema;
		if(this.oauthType.equals("0")) {
			this.connectionUrl += ";OAuthServiceAcctEmail="+this.oauthServiceAcctEmail
					+ ";OAuthPvtKeyPath="+this.oauthPrivateKeyPath;

		} else if(this.oauthType.equals("1")) {
			throw new IllegalArgumentException("This authentication protocol is not currently supported");
		
		} else if(this.oauthType.equals("2")) {
			if(this.oauthAccessToken != null && !this.oauthAccessToken.isEmpty()) {
				this.connectionUrl += ";OAuthAccessToken="+this.oauthAccessToken;
			}
			if(this.oauthRefreshToken != null && !this.oauthRefreshToken.isEmpty()) {
				this.connectionUrl += ";OAuthRefreshToken="+this.oauthRefreshToken;
			}
			
			this.connectionUrl += ";OAuthClientId="+this.oauthClientId
					+ ";OAuthClientSecret="+this.oauthClientSecret;
		} else if(this.oauthType.equals("3")) {
			// don't require any other inputs
		}
		
		if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
			if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
				this.additionalProps += ";" + this.additionalProps;
			} else {
				this.connectionUrl += this.additionalProps;
			}
		}
		
		return this.connectionUrl;
	}
	
}
