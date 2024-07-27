package prerna.io.connector.secrets.azure.keyvault;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azure.core.credential.TokenCredential;
import com.azure.core.exception.ResourceNotFoundException;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IEngine;
import prerna.io.connector.secrets.AbstractSecrets;
import prerna.util.Constants;

public class AzureKeyVaultUtil extends AbstractSecrets {

	/**
	 * 
	 * Azure KeyVault only allows alphanumeric characters and dashes
	 * 
	 */
	
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
		if(keyVaultName == null || (keyVaultName=keyVaultName.trim()).isEmpty() ) {
			throw new NullPointerException("Must define the keyvault name using " + AZURE_KEYVAULT_NAME);
		}
		String keyVaultUri = "https://" + keyVaultName + ".vault.azure.net";

		TokenCredential creds = null;

		// TODO: build out additional modes for authentication
		// TODO: build out additional modes for authentication
		// TODO: build out additional modes for authentication
		String authMode = getInput(AZURE_AUTHENTICATE_MODE);
		if(authMode == null) {
			authMode = "";
		}
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
			synchronized(AzureKeyVaultUtil.class) {
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
	
	/**
	 * 
	 * @param eType
	 * @param enginePath
	 * @return
	 */
	private String getPathForEngine(IEngine.CATALOG_TYPE eType, String enginePath) {
		return getBaseForEngine(eType) + "-" + enginePath;
	}
	
	/**
	 * Get the full path for the insight secrets
	 * @param projectPath
	 * @param insightId
	 * @return
	 */
	private String getInsightPath(String projectPath, String insightId) {
		return getPathForEngine(IEngine.CATALOG_TYPE.PROJECT, projectPath) + "-" + insightId;
	}
	
	@Override
	public Map<String, Object> getEngineSecrets(IEngine.CATALOG_TYPE eType, String engineId, String engineName) {
		// due to restrictions on path - only using engine id
		String secretPath = getPathForEngine(eType, engineId);
		try {
			KeyVaultSecret secret = this.secretClient.getSecret(secretPath);
			String value = secret.getValue();
			// we assume this is a map
			try {
				Gson gson = new GsonBuilder().disableHtmlEscaping().create();
				Map<String, Object> data = gson.fromJson(value, new TypeToken<Map<String, Object>>() {}.getType());
				return data;
			} catch(JsonSyntaxException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Invalid format for secret storage. Must be a valid string representation of a map");
			}
		} catch(ResourceNotFoundException e) {
			classLogger.warn(Constants.STACKTRACE, e);
			return new HashMap<>();
		}
	}

	@Override
	public Map<String, Object> getInsightSecrets(String projectId, String projectName, String insightId) {
		// due to restrictions on path - only using project id
		String secretPath = getInsightPath(projectId, insightId);
		try {
			KeyVaultSecret secret = this.secretClient.getSecret(secretPath);
			String value = secret.getValue();
			// we assume this is a map
			try {
				Gson gson = new GsonBuilder().disableHtmlEscaping().create();
				Map<String, Object> data = gson.fromJson(value, new TypeToken<Map<String, Object>>() {}.getType());
				return data;
			} catch(JsonSyntaxException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Invalid format for secret storage. Must be a valid string representation of a map");
			}
		} catch(ResourceNotFoundException e) {
			classLogger.warn(Constants.STACKTRACE, e);
			return new HashMap<>();
		}
	}

	@Override
	public Map<String, Object> getInsightEncryptionSecrets(String projectId, String projectName, String insightId) {
		// due to restrictions on path - only using project id
		String secretPath = getInsightPath(projectId, insightId) + "-" + INSIGHT_ENCRYPTION_NAME;
		try {
			KeyVaultSecret secret = this.secretClient.getSecret(secretPath);
			String value = secret.getValue();
			// we assume this is a map
			try {
				Gson gson = new GsonBuilder().disableHtmlEscaping().create();
				Map<String, Object> data = gson.fromJson(value, new TypeToken<Map<String, Object>>() {}.getType());
				return data;
			} catch(JsonSyntaxException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Invalid format for secret storage. Must be a valid string representation of a map");
			}
		} catch(ResourceNotFoundException e) {
			classLogger.warn(Constants.STACKTRACE, e);
			return new HashMap<>();
		}
	}

	@Override
	public boolean writeEngineSecret(IEngine.CATALOG_TYPE eType, String engineId, String engineName, String key, Object value) {
		// pull the current values
		// and add to it the new one
		// since we cannot add a single value at a time
		Map<String, Object> nameValuePairs = getEngineSecrets(eType, engineId, engineName);
		nameValuePairs.put(key, value);
		return writeEngineSecrets(eType, engineId, engineName, nameValuePairs);
	}

	@Override
	public boolean writeEngineSecrets(IEngine.CATALOG_TYPE eType, String engineId, String engineName, Map<String, Object> nameValuePairs) {
		// due to restrictions on path - only using engine id
		String secretPath = getPathForEngine(eType, engineId);
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String data = gson.toJson(nameValuePairs);
        secretClient.setSecret(new KeyVaultSecret(secretPath, data));	
		return true;
	}

	@Override
	public boolean writeInsightSecret(String projectId, String projectName, String insightId, String key, Object value) {
		// pull the current values
		// and add to it the new one
		// since we cannot add a single value at a time
		Map<String, Object> nameValuePairs = getInsightSecrets(projectId, projectName, insightId);
		nameValuePairs.put(key, value);
		return writeInsightSecrets(projectId, projectName, insightId, nameValuePairs);
	}

	@Override
	public boolean writeInsightSecrets(String projectId, String projectName, String insightId, Map<String, Object> nameValuePairs) {
		// due to restrictions on path - only using project id
		String secretPath = getInsightPath(projectId, insightId);
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String data = gson.toJson(nameValuePairs);
        secretClient.setSecret(new KeyVaultSecret(secretPath, data));
		return true;
	}

	@Override
	public boolean writeInsightEncryptionSecrets(String projectId, String projectName, String insightId, Map<String, Object> nameValuePairs) {
		// due to restrictions on path - only using project id
		String secretPath = getInsightPath(projectId, insightId) + "-" + INSIGHT_ENCRYPTION_NAME;
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String data = gson.toJson(nameValuePairs);
        secretClient.setSecret(new KeyVaultSecret(secretPath, data));
		return true;
	}
	
}
