package prerna.io.connector.secrets.azure.keyvault;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;

import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.io.connector.secrets.AbstractSecrets;
import prerna.io.connector.secrets.hashicorp.vault.HashiCorpVaultUtil;
import prerna.util.Constants;

public class AzureKeyVaultUtil extends AbstractSecrets {

	private static final Logger classLogger = LogManager.getLogger(AzureKeyVaultUtil.class);

	private static final String AZURE_AUTHENTICATE_MODE = "AZURE_AUTHENTICATE_MODE";

	private static final String AZURE_KEYVAULT_NAME = "AZURE_KEYVAULT_NAME";
	private static final String AZURE_CLIENT_ID = "AZURE_CLIENT_ID";
	private static final String AZURE_CLIENT_SECRET = "AZURE_CLIENT_SECRET";
	private static final String AZURE_TENANT_ID = "AZURE_TENANT_ID";

	private static AzureKeyVaultUtil instance;

	private SecretClient secretClient;
	
	private AzureKeyVaultUtil() {
		createSecretClient();
	}

	private void createSecretClient() {
		String keyVaultName = getInput(AZURE_KEYVAULT_NAME);
		String keyVaultUri = "https://" + keyVaultName + ".vault.azure.net";

		TokenCredential creds = null;

		// TODO: build out additional modes for authentication
		// TODO: build out additional modes for authentication
		// TODO: build out additional modes for authentication
		String authMode = getInput(AZURE_AUTHENTICATE_MODE);
		if(authMode.equals("ClientSecretCredential")) {
			String clientId = getInput(AZURE_CLIENT_ID);
			String clientSecret = getInput(AZURE_CLIENT_SECRET);
			String tenantId = getInput(AZURE_TENANT_ID);

			creds = new ClientSecretCredentialBuilder()
					.clientId(clientId)
					.clientSecret(clientSecret)
					.tenantId(tenantId)
					.build();
		} else {
			creds = new DefaultAzureCredentialBuilder()
					.build();
		}

		this.secretClient = new SecretClientBuilder()
				.vaultUrl(keyVaultUri)
				.credential(creds)
				.buildClient();
	}

	public static AzureKeyVaultUtil getInstance() {
		if(instance != null) {
			return instance;
		}

		if(instance == null) {
			synchronized(HashiCorpVaultUtil.class) {
				if(instance == null) {
					try {
						instance = new AzureKeyVaultUtil();
					} catch (Exception e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

		return instance;
	}
	
	@Override
	public Map<String, String> getEngineSecrets(CATALOG_TYPE arg0, String arg1, String arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getInsightSecrets(String projectId, String projectName, String insightId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getInsightEncryptionSecrets(String projectId, String projectName, String insightId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean writeEngineSecret(CATALOG_TYPE arg0, String arg1, String arg2, String arg3, Object arg4) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean writeEngineSecrets(CATALOG_TYPE arg0, String arg1, String arg2, Map<String, Object> arg3) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean writeInsightSecret(String projectId, String projectName, String insightId, String key,
			Object value) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean writeInsightSecrets(String projectId, String projectName, String insightId,
			Map<String, Object> nameValuePairs) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean writeInsightEncryptionSecrets(String projectId, String projectName, String insightId,
			Map<String, Object> nameValuePairs) {
		// TODO Auto-generated method stub
		return false;
	}

}
