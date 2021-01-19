package prerna.util.sql;

import java.util.Map;
import java.util.Properties;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.BigQuerySqlInterpreter;

public class BigQueryQueryUtil extends AnsiSqlQueryUtil {

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
	public String buildConnectionString(Map<String, Object> configMap) throws RuntimeException {
		if(configMap.isEmpty()){
			throw new RuntimeException("Configuration map is empty");
		}
		
		String connectionString = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_STRING);
		if(connectionString != null && !connectionString.isEmpty()) {
			return connectionString;
		}
		
		String urlPrefix = this.dbType.getUrlPrefix();
		String host = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if(host == null || host.isEmpty()) {
			throw new RuntimeException("Must pass in a host");
		}
		
		String port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		if(port == null || port.isEmpty()) {
			port = "443";
		}
		
		String projectId = (String) configMap.get(AbstractSqlQueryUtil.PROJECT_ID);
		if(projectId == null || projectId.isEmpty()) {
			throw new RuntimeException("Must pass in a project id");
		}
		
		String oauthType = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_TYPE);
		if(oauthType == null || oauthType.isEmpty()) {
			throw new RuntimeException("Must pass in an OAuth Type");
		}
		String oauthServiceAcctEmail = null;
		String oauthPrivateKeyPath = null;
		String oauthAccessToken = null;
		String oauthRefreshToken = null;
		String oauthClientId = null;
		String oauthClientSecret = null;
		if(oauthType.equals("0")) {
			oauthServiceAcctEmail = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_SERVICE_ACCT_EMAIL);
			if(oauthServiceAcctEmail == null || oauthServiceAcctEmail.isEmpty()) {
				throw new RuntimeException("Must pass in an OAuth Service Account Email");
			}
			
			oauthPrivateKeyPath = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_PRIVATE_KEY_PATH);
			if(oauthPrivateKeyPath == null || oauthPrivateKeyPath.isEmpty()) {
				throw new RuntimeException("Must pass in an OAuth Private Key Path");
			}
		} else if(oauthType.equals("1")) {
			throw new IllegalArgumentException("This authentication protocol is not currently supported");
		} else if(oauthType.equals("2")) {
			oauthAccessToken = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_ACCESS_TOKEN);
			oauthRefreshToken = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_REFRESH_TOKEN);

			if( (oauthAccessToken == null || oauthAccessToken.isEmpty()) &&
				(oauthRefreshToken == null || oauthRefreshToken.isEmpty())
					) {
				throw new RuntimeException("Must pass in an OAuth Access Token or Refresh Token");
			}
			
			oauthClientId = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_CLIENT_ID);
			if(oauthClientId == null || oauthClientId.isEmpty()) {
				throw new RuntimeException("Must pass in an OAuth Client ID");
			}
			
			oauthClientSecret = (String) configMap.get(AbstractSqlQueryUtil.OAUTH_CLIENT_SECRET);
			if(oauthClientSecret == null || oauthClientSecret.isEmpty()) {
				throw new RuntimeException("Must pass in an OAuth Client Secret");
			}
		} else if(oauthType.equals("3")) {
			// don't require any other inputs
		} else {
			throw new IllegalArgumentException("OAuth Type can only contain one of the following values: '0', '1', '2', '3'");
		}
		
		String defaultDataSet = (String) configMap.get(AbstractSqlQueryUtil.SCHEMA);
		if(defaultDataSet == null || defaultDataSet.isEmpty()) {
			throw new RuntimeException("Must pass in the default dataset");
		}
		
		connectionString = urlPrefix+"://"+host+":"+port+";ProjectId="+projectId
				+";OAuthType="+oauthType+";DefaultDataset="+defaultDataSet;
		if(oauthType.equals("0")) {
			connectionString += ";OAuthServiceAcctEmail="+oauthServiceAcctEmail
					+ ";OAuthPvtKeyPath="+oauthPrivateKeyPath;

		} else if(oauthType.equals("1")) {
			throw new IllegalArgumentException("This authentication protocol is not currently supported");
		
		} else if(oauthType.equals("2")) {
			if(oauthAccessToken != null && !oauthAccessToken.isEmpty()) {
				connectionString += ";OAuthAccessToken="+oauthAccessToken;
			}
			if(oauthRefreshToken != null && !oauthRefreshToken.isEmpty()) {
				connectionString += ";OAuthRefreshToken="+oauthRefreshToken;
			}
			
			connectionString += ";OAuthClientId="+oauthClientId
					+ ";OAuthClientSecret="+oauthClientSecret;
		} else if(oauthType.equals("3")) {
			// don't require any other inputs
		}
		
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
		String host = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if(host == null || host.isEmpty()) {
			throw new RuntimeException("Must pass in a host");
		}
		
		String port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		if(port == null || port.isEmpty()) {
			port = "443";
		}
		
		String projectId = (String) prop.get(AbstractSqlQueryUtil.PROJECT_ID);
		if(projectId == null || projectId.isEmpty()) {
			throw new RuntimeException("Must pass in a project id");
		}
		
		String oauthType = (String) prop.get(AbstractSqlQueryUtil.OAUTH_TYPE);
		if(oauthType == null || oauthType.isEmpty()) {
			throw new RuntimeException("Must pass in an OAuth Type");
		}
		String oauthServiceAcctEmail = null;
		String oauthPrivateKeyPath = null;
		String oauthAccessToken = null;
		String oauthRefreshToken = null;
		String oauthClientId = null;
		String oauthClientSecret = null;
		if(oauthType.equals("0")) {
			oauthServiceAcctEmail = (String) prop.get(AbstractSqlQueryUtil.OAUTH_SERVICE_ACCT_EMAIL);
			if(oauthServiceAcctEmail == null || oauthServiceAcctEmail.isEmpty()) {
				throw new RuntimeException("Must pass in an OAuth Service Account Email");
			}
			
			oauthPrivateKeyPath = (String) prop.get(AbstractSqlQueryUtil.OAUTH_PRIVATE_KEY_PATH);
			if(oauthPrivateKeyPath == null || oauthPrivateKeyPath.isEmpty()) {
				throw new RuntimeException("Must pass in an OAuth Private Key Path");
			}
		} else if(oauthType.equals("1")) {
			throw new IllegalArgumentException("This authentication protocol is not currently supported");
		} else if(oauthType.equals("2")) {
			oauthAccessToken = (String) prop.get(AbstractSqlQueryUtil.OAUTH_ACCESS_TOKEN);
			oauthRefreshToken = (String) prop.get(AbstractSqlQueryUtil.OAUTH_REFRESH_TOKEN);

			if( (oauthAccessToken == null || oauthAccessToken.isEmpty()) &&
				(oauthRefreshToken == null || oauthRefreshToken.isEmpty())
					) {
				throw new RuntimeException("Must pass in an OAuth Access Token or Refresh Token");
			}
			
			oauthClientId = (String) prop.get(AbstractSqlQueryUtil.OAUTH_CLIENT_ID);
			if(oauthClientId == null || oauthClientId.isEmpty()) {
				throw new RuntimeException("Must pass in an OAuth Client ID");
			}
			
			oauthClientSecret = (String) prop.get(AbstractSqlQueryUtil.OAUTH_CLIENT_SECRET);
			if(oauthClientSecret == null || oauthClientSecret.isEmpty()) {
				throw new RuntimeException("Must pass in an OAuth Client Secret");
			}
		} else if(oauthType.equals("3")) {
			// don't require any other inputs
		} else {
			throw new IllegalArgumentException("OAuth Type can only contain one of the following values: '0', '1', '2', '3'");
		}
		
		String defaultDataSet = (String) prop.get(AbstractSqlQueryUtil.SCHEMA);
		if(defaultDataSet == null || defaultDataSet.isEmpty()) {
			throw new RuntimeException("Must pass in the default dataset");
		}
		
		connectionString = urlPrefix+"://"+host+":"+port+";ProjectId="+projectId
				+";OAuthType="+oauthType+";DefaultDataset="+defaultDataSet;
		if(oauthType.equals("0")) {
			connectionString += ";OAuthServiceAcctEmail="+oauthServiceAcctEmail
					+ ";OAuthPvtKeyPath="+oauthPrivateKeyPath;

		} else if(oauthType.equals("1")) {
			throw new IllegalArgumentException("This authentication protocol is not currently supported");
		
		} else if(oauthType.equals("2")) {
			if(oauthAccessToken != null && !oauthAccessToken.isEmpty()) {
				connectionString += ";OAuthAccessToken="+oauthAccessToken;
			}
			if(oauthRefreshToken != null && !oauthRefreshToken.isEmpty()) {
				connectionString += ";OAuthRefreshToken="+oauthRefreshToken;
			}
			
			connectionString += ";OAuthClientId="+oauthClientId
					+ ";OAuthClientSecret="+oauthClientSecret;
		} else if(oauthType.equals("3")) {
			// don't require any other inputs
		}
		
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
	
}
