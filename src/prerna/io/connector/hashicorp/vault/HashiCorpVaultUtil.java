package prerna.io.connector.hashicorp.vault;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.StatusLine;
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

import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.Utility;

public class HashiCorpVaultUtil {

	private static final Logger logger = LogManager.getLogger(HashiCorpVaultUtil.class);

	private static final String VAULT_ADDR = "VAULT_ADDR";
	private static final String VAULT_TOKEN = "VAULT_TOKEN";
	private static final String VAULT_TOKEN_HEADER_KEY = "X-Vault-Token";
	
	private static final String DEBUG_TOKEN = "***REMOVED***";
	private static final String DEBUG_ADDR = "http://0.0.0.0:8200";
	
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
				.sslConfig(new SslConfig().build())	// See "SSL Config" section below
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

	public String getVaultToken() {
		String token = System.getenv(VAULT_TOKEN);
		if(token == null || token.isEmpty()) {
			return DEBUG_TOKEN;
		}

		return token;
	}

	public String getVaultAddr() {
		String addr = System.getenv(VAULT_ADDR);
		if(addr == null || addr.isEmpty()) {
			return DEBUG_ADDR;
		}

		return addr;
	}
	
	public Map<String, String> getDatabaseSecrets(String name, String id) {
		String secretPath = SmssUtilities.getUniqueName(name, id);
		secretPath = Utility.encodeURIComponent(secretPath);
		try {
			return this.vault.logical().read("db/" + secretPath).getData();
		} catch (VaultException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		return null;
	}
	
	public void createDatabaseSecretEngine() throws ParseException, IOException {
		HttpPost post = new HttpPost(getVaultAddr() + "/v1/sys/mounts/db");
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

	public void createProjectSecretEngine() throws ParseException, IOException {
		HttpPost post = new HttpPost(getVaultAddr() + "/v1/sys/mounts/project");
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

	
	/////////////////////////////////////////////
	/////////////////////////////////////////////
	/////////////////////////////////////////////
	/////////////////////////////////////////////

	
	public static void main(String[] args) throws VaultException, ParseException, IOException {
		HashiCorpVaultUtil instance = HashiCorpVaultUtil.getInstance();
		instance.createDatabaseSecretEngine();
		instance.createProjectSecretEngine();
		
		Map<String, String> dbSecrets = instance.getDatabaseSecrets("***REMOVED***", "fe5e2c23-59e6-42ae-939d-b2ca9699f38c");
		System.out.println(dbSecrets);
	}

}
