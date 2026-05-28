/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.io.connector.secrets.azure.keyvault;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azure.core.credential.TokenCredential;
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
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.io.connector.secrets.AbstractSecrets;

public final class AzureKeyVaultUtil extends AbstractSecrets {

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

	private static volatile AzureKeyVaultUtil instance;

	private SecretClient secretClient;

	private AzureKeyVaultUtil() {
		createSecretClient();
	}

	private void createSecretClient() {
		String keyVaultName = getInput(AZURE_KEYVAULT_NAME);
		if (keyVaultName == null || (keyVaultName = keyVaultName.trim()).isEmpty()) {
			throw new NullPointerException("Must define the keyvault name using " + AZURE_KEYVAULT_NAME);
		}
		String keyVaultUri = "https://" + keyVaultName + ".vault.azure.net";

		TokenCredential creds = null;

		// TODO: build out additional modes for authentication
		// TODO: build out additional modes for authentication
		// TODO: build out additional modes for authentication
		String authMode = getInput(AZURE_AUTHENTICATE_MODE);
		if (authMode == null) {
			authMode = "";
		}
		if (authMode.equals("ClientSecretCredential")) {
			String clientId = getInput(AZURE_CLIENT_ID);
			String clientSecret = getInput(AZURE_CLIENT_SECRET);
			String tenantId = getInput(AZURE_TENANT_ID);

			creds = new ClientSecretCredentialBuilder().clientId(clientId).clientSecret(clientSecret).tenantId(tenantId)
					.build();
		} else {
			creds = new DefaultAzureCredentialBuilder().build();
		}

		this.secretClient = new SecretClientBuilder().vaultUrl(keyVaultUri).credential(creds).buildClient();
	}

	public static AzureKeyVaultUtil getInstance() {
		if (instance != null) {
			return instance;
		}

		if (instance == null) {
				synchronized (AzureKeyVaultUtil.class) {
					if (instance == null) {
						try {
							instance = new AzureKeyVaultUtil();
						} catch (Exception e) {
							classLogger.error(
									"Failed to initialize AzureKeyVaultUtil singleton. Verify configuration for {} and {}.",
									AZURE_KEYVAULT_NAME, AZURE_AUTHENTICATE_MODE, e);
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
		String base = getBaseForEngine(eType);
		if (base != null && !(base = base.trim()).isEmpty()) {
			return base + "-" + enginePath;
		}
		return enginePath;
	}

	/**
	 * Get the full path for the insight secrets
	 * 
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
					Map<String, Object> data = gson.fromJson(value, new TypeToken<Map<String, Object>>() {
					}.getType());
					return data;
				} catch (JsonSyntaxException e) {
					classLogger.error(
							"Failed to parse engine secret payload from Azure Key Vault for secret path '{}' (engineId={}, catalogType={}). Expected a JSON object map.",
							secretPath, engineId, eType, e);
					throw new IllegalArgumentException(
							"Invalid format for secret storage. Must be a valid string representation of a map");
				}
			} catch (Exception e) {
				classLogger.warn(
						"Failed to retrieve engine secrets from Azure Key Vault for secret path '{}' (engineId={}, catalogType={}). Returning empty secrets map.",
						secretPath, engineId, eType, e);
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
					Map<String, Object> data = gson.fromJson(value, new TypeToken<Map<String, Object>>() {
					}.getType());
					return data;
				} catch (JsonSyntaxException e) {
					classLogger.error(
							"Failed to parse insight secret payload from Azure Key Vault for secret path '{}' (projectId={}, insightId={}). Expected a JSON object map.",
							secretPath, projectId, insightId, e);
					throw new IllegalArgumentException(
							"Invalid format for secret storage. Must be a valid string representation of a map");
				}
			} catch (Exception e) {
				classLogger.warn(
						"Failed to retrieve insight secrets from Azure Key Vault for secret path '{}' (projectId={}, insightId={}). Returning empty secrets map.",
						secretPath, projectId, insightId, e);
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
					Map<String, Object> data = gson.fromJson(value, new TypeToken<Map<String, Object>>() {
					}.getType());
					return data;
				} catch (JsonSyntaxException e) {
					classLogger.error(
							"Failed to parse insight encryption secret payload from Azure Key Vault for secret path '{}' (projectId={}, insightId={}). Expected a JSON object map.",
							secretPath, projectId, insightId, e);
					throw new IllegalArgumentException(
							"Invalid format for secret storage. Must be a valid string representation of a map");
				}
			} catch (Exception e) {
				classLogger.warn(
						"Failed to retrieve insight encryption secrets from Azure Key Vault for secret path '{}' (projectId={}, insightId={}). Returning empty secrets map.",
						secretPath, projectId, insightId, e);
				return new HashMap<>();
			}
		}

	@Override
	public boolean appendEngineSecret(IEngine.CATALOG_TYPE eType, String engineId, String engineName, String key,
			Object value) {
		// pull the current values
		// and add to it the new one
		// since we cannot add a single value at a time
		Map<String, Object> nameValuePairs = getEngineSecrets(eType, engineId, engineName);
		if (nameValuePairs == null) {
			nameValuePairs = new HashMap<>();
		}
		nameValuePairs.put(key, value);
		return writeEngineSecrets(eType, engineId, engineName, nameValuePairs);
	}

	@Override
	public boolean writeEngineSecrets(IEngine.CATALOG_TYPE eType, String engineId, String engineName,
			Map<String, Object> nameValuePairs) {
		// due to restrictions on path - only using engine id
		String secretPath = getPathForEngine(eType, engineId);
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String data = gson.toJson(nameValuePairs);
		secretClient.setSecret(new KeyVaultSecret(secretPath, data));
		return true;
	}

	@Override
	public boolean deleteEngineSecrets(CATALOG_TYPE eType, String engineId, String engineName) {
		String secretPath = getPathForEngine(eType, engineId);
		try {
			secretClient.beginDeleteSecret(secretPath);
			return true;
		} catch (Exception e) {
			classLogger.warn(
					"Failed to delete engine secret from Azure Key Vault for secret path '{}' (engineId={}, catalogType={}).",
					secretPath, engineId, eType, e);
			return false;
		}
	}

	@Override
	public boolean writeInsightSecret(String projectId, String projectName, String insightId, String key,
			Object value) {
		// pull the current values
		// and add to it the new one
		// since we cannot add a single value at a time
		Map<String, Object> nameValuePairs = getInsightSecrets(projectId, projectName, insightId);
		nameValuePairs.put(key, value);
		return writeInsightSecrets(projectId, projectName, insightId, nameValuePairs);
	}

	@Override
	public boolean writeInsightSecrets(String projectId, String projectName, String insightId,
			Map<String, Object> nameValuePairs) {
		// due to restrictions on path - only using project id
		String secretPath = getInsightPath(projectId, insightId);
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String data = gson.toJson(nameValuePairs);
		secretClient.setSecret(new KeyVaultSecret(secretPath, data));
		return true;
	}

	@Override
	public boolean writeInsightEncryptionSecrets(String projectId, String projectName, String insightId,
			Map<String, Object> nameValuePairs) {
		// due to restrictions on path - only using project id
		String secretPath = getInsightPath(projectId, insightId) + "-" + INSIGHT_ENCRYPTION_NAME;
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String data = gson.toJson(nameValuePairs);
		secretClient.setSecret(new KeyVaultSecret(secretPath, data));
		return true;
	}

}
