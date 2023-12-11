package prerna.io.connector.secrets.hashicorp.vault;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bettercloud.vault.SslConfig;
import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.json.JsonArray;
import com.bettercloud.vault.json.JsonValue;
import com.google.common.primitives.Bytes;

import prerna.engine.impl.SmssUtilities;
import prerna.io.connector.secrets.ISecrets;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class HashiCorpVaultUtil implements ISecrets {

	private static final Logger logger = LogManager.getLogger(HashiCorpVaultUtil.class);

	private static final String VAULT_ADDR = "VAULT_ADDR";
	private static final String VAULT_TOKEN = "VAULT_TOKEN";
	private static final String VAULT_TOKEN_HEADER_KEY = "X-Vault-Token";
	
	// root url for db and projects
	private static final String VAULT_DB_PATH = "VAULT_DB_PATH";
	private static final String VAULT_PROJECT_PATH = "VAULT_PROJECT_PATH";
	private static final String INSIGHT_ENCRYPTION_PATH = "/encrypt";
	
	private static final String DEBUG_VAULT_TOKEN = "***REMOVED***";
	private static final String DEBUG_VAULT_ADDR = "http://127.0.0.1:8200";
	private static final String DEBUG_VAULT_DB_PATH = "semoss_db";
	private static final String DEBUG_VAULT_PROJECT_PATH = "semoss_project";
	
	private static HashiCorpVaultUtil instance;
	
	private Vault vault;
	private VaultConfig config;

	private HashiCorpVaultUtil() throws VaultException {
		createVault();
	}
	
	private void createVault() throws VaultException {
		this.config = new VaultConfig()
				.address(getVaultAddr())			// Defaults to "VAULT_ADDR" environment variable
				.token(getVaultToken())				// Defaults to "VAULT_TOKEN" environment variable
				.openTimeout(5)						// Defaults to "VAULT_OPEN_TIMEOUT" environment variable
				.readTimeout(30)					// Defaults to "VAULT_READ_TIMEOUT" environment variable
				.sslConfig(new SslConfig().build())
				.build();
		this.vault = new Vault(this.config);
	}

	public static HashiCorpVaultUtil getInstance() {
		if(instance != null) {
			return instance;
		}

		if(instance == null) {
			synchronized(HashiCorpVaultUtil.class) {
				if(instance == null) {
					try {
						instance = new HashiCorpVaultUtil();
					} catch (VaultException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

		return instance;
	}

	/**
	 * Get the vault authorization token
	 * @return
	 */
	public String getVaultToken() {
		String token = getInput(VAULT_TOKEN);
		if(token == null) {
			token = DEBUG_VAULT_TOKEN;
		}
		return token;
	}

	/**
	 * Get the vault address
	 * @return
	 */
	public String getVaultAddr() {
		String addr = getInput(VAULT_ADDR);
		if(addr == null) {
			addr = DEBUG_VAULT_ADDR;
		}
		return addr;
	}
	
	/**
	 * Get the full path for the database secrets
	 * @param path
	 * @return
	 */
	private String getDbPath(String path) {
		String dbPath = getInput(VAULT_DB_PATH);
		if(dbPath == null) {
			dbPath = DEBUG_VAULT_DB_PATH;
		}
		return dbPath + path;
	}
	
	/**
	 * Get the full path for the project secrets
	 * @param path
	 * @return
	 */
	private String getProjectPath(String path) {
		String projectPath = getInput(VAULT_PROJECT_PATH);
		if(projectPath == null) {
			projectPath = DEBUG_VAULT_PROJECT_PATH;
		}
		return projectPath + "/" + path;
	}
	
	/**
	 * Get the full path for the insight secrets
	 * @param projectPath
	 * @param insightId
	 * @return
	 */
	private String getInsightPath(String projectPath, String insightId) {
		return getProjectPath(projectPath) + "/" + insightId;
	}
	
	/**
	 * General method to grab input from environment variable or RDF_Map
	 * @param key
	 * @return
	 */
	private String getInput(String key) {
		String value = System.getenv(key);
		if(value == null || value.isEmpty()) {
			value = DIHelper.getInstance().getProperty(key);
		}

		return value;
	}
	
	@Override
	public Map<String, String> getDatabaseSecrets(String databaseName, String databaseId) {
		String secretPath = SmssUtilities.getUniqueName(databaseName, databaseId);
		secretPath = Utility.encodeURIComponent(secretPath);
		try {
			return this.vault.logical().read(getDbPath(secretPath)).getData();
		} catch (VaultException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		return null;
	}
	
	@Override
	public Map<String, String> getProjectSecrets(String projectName, String projectId) {
		String secretPath = SmssUtilities.getUniqueName(projectName, projectId);
		secretPath = Utility.encodeURIComponent(secretPath);
		try {
			return this.vault.logical().read(getProjectPath(secretPath)).getData();
		} catch (VaultException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		return null;
	}
	
	@Override
	public Map<String, String> getInsightSecrets(String insightId, String projectName, String projectId) {
		String secretPath = SmssUtilities.getUniqueName(projectName, projectId);
		secretPath = Utility.encodeURIComponent(secretPath);
		try {
			return this.vault.logical().read(getInsightPath(secretPath, insightId)).getData();
		} catch (VaultException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		return null;
	}
	
	@Override
	public Map<String, Object> getInsightEncryptionSecrets(String insightId, String projectName, String projectId) {
		String secretPath = SmssUtilities.getUniqueName(projectName, projectId);
		secretPath = Utility.encodeURIComponent(secretPath);
		try {
			com.bettercloud.vault.json.JsonObject jsonObject = this.vault.logical().read(getInsightPath(secretPath, insightId + INSIGHT_ENCRYPTION_PATH)).getDataObject();
			String secret = jsonObject.getString(ISecrets.SECRET);
			String salt = jsonObject.getString(ISecrets.SALT);
			Iterator<JsonValue> ivIterator = jsonObject.get(ISecrets.IV).asArray().iterator();
			List<Byte> iv = new ArrayList<>();
			while(ivIterator.hasNext()) {
				iv.add( (byte) ivIterator.next().asInt());
			}
			Map<String, Object> cacheData = new HashMap<>();
			cacheData.put(ISecrets.SECRET, secret);
			cacheData.put(ISecrets.SALT, salt);
			cacheData.put(ISecrets.IV, Bytes.toArray(iv));
			return cacheData;
		} catch (VaultException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		return null;
	}
	
	@Override
	public boolean writeDatabaseSecret(String databaseName, String databaseId, String key, Object value) {
		Map<String, Object> nameValuePairs = new HashMap<>();
		nameValuePairs.put(key, value);
		return writeDatabaseSecrets(databaseName, databaseId, nameValuePairs);
	}
	
	@Override
	public boolean writeDatabaseSecrets(String databaseName, String databaseId, Map<String, Object> nameValuePairs) {
		String secretPath = SmssUtilities.getUniqueName(databaseName, databaseId);
		secretPath = Utility.encodeURIComponent(secretPath);
		
		try {
			this.vault.logical().write(getDbPath(secretPath), nameValuePairs);
			return true;
		} catch (VaultException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}
	}

	@Override
	public boolean writeProjectSecret(String projectName, String projectId, String key, Object value) {
		Map<String, Object> nameValuePairs = new HashMap<>();
		nameValuePairs.put(key, value);
		return writeProjectSecrets(projectName, projectId, nameValuePairs);
	}

	@Override
	public boolean writeProjectSecrets(String projectName, String projectId, Map<String, Object> nameValuePairs) {
		String secretPath = SmssUtilities.getUniqueName(projectName, projectId);
		secretPath = Utility.encodeURIComponent(secretPath);
		
		try {
			this.vault.logical().write(getProjectPath(secretPath), nameValuePairs);
			return true;
		} catch (VaultException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}
	}

	@Override
	public boolean writeInsightSecret(String insightId, String projectName, String projectId, String key, Object value) {
		Map<String, Object> nameValuePairs = new HashMap<>();
		nameValuePairs.put(key, value);
		return writeInsightSecrets(insightId, projectName, projectId, nameValuePairs);
	}

	@Override
	public boolean writeInsightSecrets(String insightId, String projectName, String projectId, Map<String, Object> nameValuePairs) {
		String secretPath = SmssUtilities.getUniqueName(projectName, projectId);
		secretPath = Utility.encodeURIComponent(secretPath);
		
		try {
			this.vault.logical().write(getInsightPath(secretPath, insightId), nameValuePairs);
			return true;
		} catch (VaultException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}
	}

	@Override
	public boolean writeInsightEncryptionSecrets(String insightId, String projectName, String projectId, Map<String, Object> nameValuePairs) {
		String secretPath = SmssUtilities.getUniqueName(projectName, projectId);
		secretPath = Utility.encodeURIComponent(secretPath);
		byte[] iv = (byte[]) nameValuePairs.get(ISecrets.IV);
		com.bettercloud.vault.json.JsonArray jsonArray = new JsonArray();
		for(byte i : iv) {
			jsonArray.add(i);
		}
		nameValuePairs.put(ISecrets.IV, jsonArray);
		try {
			this.vault.logical().write(getInsightPath(secretPath, insightId + INSIGHT_ENCRYPTION_PATH), nameValuePairs);
			return true;
		} catch (VaultException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}
	}
	
	/**
	 * Create a database KV engine
	 * @throws ParseException
	 * @throws IOException
	 */
	public void createDatabaseSecretEngine() throws ParseException, IOException {
		HttpPost post = new HttpPost(getVaultAddr() + "/v1/sys/mounts/semoss_db");
		post.setHeader(VAULT_TOKEN_HEADER_KEY, getVaultToken());
		HttpEntity body = new StringEntity("{"
				+ "\"type\":\"kv\", "
				+ "\"description\":\"the secrets for database smss files\","
				+ "\"options\":{\"version\":\"2\"}"
				+ "}") ;
		post.setEntity(body);

		CloseableHttpClient client = HttpClientBuilder.create().build();
		HttpResponse response = client.execute(post);

		String responseBody = null;
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			responseBody = EntityUtils.toString(entity);
		}

		StatusLine statusLine = response.getStatusLine();
		System.out.println("status line = " + statusLine.getStatusCode());
		System.out.println("response body = " + responseBody);
	}

	/**
	 * Create a project KV engine
	 * @throws ParseException
	 * @throws IOException
	 */
	public void createProjectSecretEngine() throws ParseException, IOException {
		HttpPost post = new HttpPost(getVaultAddr() + "/v1/sys/mounts/semoss_project");
		post.setHeader(VAULT_TOKEN_HEADER_KEY, getVaultToken());
		HttpEntity body = new StringEntity("{"
				+ "\"type\":\"kv\", "
				+ "\"description\":\"the secrets for projects and insights\","
				+ "\"options\":{\"version\":\"2\"}"
				+ "}") ;
		post.setEntity(body);

		CloseableHttpClient client = HttpClientBuilder.create().build();
		HttpResponse response = client.execute(post);

		String responseBody = null;
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			responseBody = EntityUtils.toString(entity);
		}

		StatusLine statusLine = response.getStatusLine();
		System.out.println("status line = " + statusLine.getStatusCode());
		System.out.println("response body = " + responseBody);
	}

	
	public void unwrapToken(String wrappingToken) throws ClientProtocolException, IOException {
		HttpPost post = new HttpPost(getVaultAddr() + "/v1/sys/wrapping/unwrap");
		post.setHeader(VAULT_TOKEN_HEADER_KEY, wrappingToken);

		CloseableHttpClient client = HttpClientBuilder.create().build();
		HttpResponse response = client.execute(post);

		String responseBody = null;
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			responseBody = EntityUtils.toString(entity);
		}

		StatusLine statusLine = response.getStatusLine();
		System.out.println("status line = " + statusLine.getStatusCode());
		System.out.println("response body = " + responseBody);
	}
	
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////

	
//	public static void main(String[] args) throws VaultException, ParseException, IOException {
//		HashiCorpVaultUtil instance = HashiCorpVaultUtil.getInstance();
//		instance.createDatabaseSecretEngine();
//		instance.createProjectSecretEngine();
//		
//		Map<String, Object> nameValuePairs = new HashMap<>();
//		nameValuePairs.put("PASSWORD","password");
//		instance.writeDatabaseSecrets("***REMOVED***", "fe5e2c23-59e6-42ae-939d-b2ca9699f38c", nameValuePairs);
//		
//		Map<String, String> dbSecrets = instance.getDatabaseSecrets("***REMOVED***", "fe5e2c23-59e6-42ae-939d-b2ca9699f38c");
//		System.out.println(dbSecrets);
//	}

}
