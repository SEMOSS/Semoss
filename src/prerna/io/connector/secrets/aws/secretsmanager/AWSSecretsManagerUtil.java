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
package prerna.io.connector.secrets.aws.secretsmanager;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.io.connector.secrets.AbstractSecrets;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.CreateSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.DeleteSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

public final class AWSSecretsManagerUtil extends AbstractSecrets {

	/**
	 *
	 * AWS Secrets Manager secret names only allow alphanumeric characters and the
	 * following: /_+=.@- (this is a superset of what Vault/Azure allow, so
	 * engine/insight ids - which are UUIDs - are always safe to use directly)
	 *
	 */

	private static final Logger classLogger = LogManager.getLogger(AWSSecretsManagerUtil.class);

	private static final String AWS_REGION = "AWS_REGION";

	private static volatile AWSSecretsManagerUtil instance;

	private SecretsManagerClient secretsManagerClient;

	private AWSSecretsManagerUtil() {
		createSecretsManagerClient();
	}

	private void createSecretsManagerClient() {
		String region = getInput(AWS_REGION);
		if (region == null || (region = region.trim()).isEmpty()) {
			throw new NullPointerException("Must define the AWS region using " + AWS_REGION);
		}

		// use the standard AWS credential chain (env vars, shared config/profile, or
		// IAM role) rather than static keys - mirrors Azure's DefaultAzureCredentialBuilder
		// branch and avoids hardcoding secret-store credentials
		this.secretsManagerClient = SecretsManagerClient.builder().region(Region.of(region))
				.credentialsProvider(DefaultCredentialsProvider.create()).build();
	}

	public static AWSSecretsManagerUtil getInstance() {
		if (instance != null) {
			return instance;
		}

		if (instance == null) {
			synchronized (AWSSecretsManagerUtil.class) {
				if (instance == null) {
					try {
						instance = new AWSSecretsManagerUtil();
					} catch (Exception e) {
						classLogger.error(
								"Failed to initialize AWSSecretsManagerUtil singleton. Verify configuration for {}.",
								AWS_REGION, e);
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
			return base + "/" + enginePath;
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
		return getPathForEngine(IEngine.CATALOG_TYPE.PROJECT, projectPath) + "/" + insightId;
	}

	/**
	 * Read a secret from AWS Secrets Manager and parse its string value as a JSON
	 * object map.
	 *
	 * @param secretPath the Secrets Manager secret name/id to read
	 * @return the parsed name-value pairs, or {@code null} if the stored value is
	 *         not valid JSON
	 */
	private Map<String, Object> readSecretMap(String secretPath) {
		String value = this.secretsManagerClient
				.getSecretValue(GetSecretValueRequest.builder().secretId(secretPath).build()).secretString();
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		return gson.fromJson(value, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	/**
	 * Write a secret to AWS Secrets Manager as a JSON object map. Unlike Vault/Key
	 * Vault, Secrets Manager's {@code PutSecretValue} fails with
	 * {@link ResourceNotFoundException} if the secret name does not already
	 * exist, so this falls back to {@code CreateSecret} on first write for that
	 * path.
	 *
	 * @param secretPath     the Secrets Manager secret name/id to write
	 * @param nameValuePairs the name-value pairs to serialize as the secret value
	 */
	private void writeSecretMap(String secretPath, Map<String, Object> nameValuePairs) {
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String data = gson.toJson(nameValuePairs);
		try {
			this.secretsManagerClient
					.putSecretValue(PutSecretValueRequest.builder().secretId(secretPath).secretString(data).build());
		} catch (ResourceNotFoundException e) {
			this.secretsManagerClient
					.createSecret(CreateSecretRequest.builder().name(secretPath).secretString(data).build());
		}
	}

	@Override
	public Map<String, Object> getEngineSecrets(IEngine.CATALOG_TYPE eType, String engineId, String engineName) {
		String secretPath = getPathForEngine(eType, engineId);
		try {
			Map<String, Object> data = readSecretMap(secretPath);
			return data == null ? new HashMap<>() : data;
		} catch (JsonSyntaxException e) {
			classLogger.error(
					"Failed to parse engine secret payload from AWS Secrets Manager for secret path '{}' (engineId={}, catalogType={}). Expected a JSON object map.",
					secretPath, engineId, eType, e);
			throw new IllegalArgumentException(
					"Invalid format for secret storage. Must be a valid string representation of a map");
		} catch (Exception e) {
			classLogger.warn(
					"Failed to retrieve engine secrets from AWS Secrets Manager for secret path '{}' (engineId={}, catalogType={}). Returning empty secrets map.",
					secretPath, engineId, eType, e);
			return new HashMap<>();
		}
	}

	@Override
	public Map<String, Object> getInsightSecrets(String projectId, String projectName, String insightId) {
		String secretPath = getInsightPath(projectId, insightId);
		try {
			Map<String, Object> data = readSecretMap(secretPath);
			return data == null ? new HashMap<>() : data;
		} catch (JsonSyntaxException e) {
			classLogger.error(
					"Failed to parse insight secret payload from AWS Secrets Manager for secret path '{}' (projectId={}, insightId={}). Expected a JSON object map.",
					secretPath, projectId, insightId, e);
			throw new IllegalArgumentException(
					"Invalid format for secret storage. Must be a valid string representation of a map");
		} catch (Exception e) {
			classLogger.warn(
					"Failed to retrieve insight secrets from AWS Secrets Manager for secret path '{}' (projectId={}, insightId={}). Returning empty secrets map.",
					secretPath, projectId, insightId, e);
			return new HashMap<>();
		}
	}

	@Override
	public Map<String, Object> getInsightEncryptionSecrets(String projectId, String projectName, String insightId) {
		String secretPath = getInsightPath(projectId, insightId) + "-" + INSIGHT_ENCRYPTION_NAME;
		try {
			Map<String, Object> data = readSecretMap(secretPath);
			return data == null ? new HashMap<>() : data;
		} catch (JsonSyntaxException e) {
			classLogger.error(
					"Failed to parse insight encryption secret payload from AWS Secrets Manager for secret path '{}' (projectId={}, insightId={}). Expected a JSON object map.",
					secretPath, projectId, insightId, e);
			throw new IllegalArgumentException(
					"Invalid format for secret storage. Must be a valid string representation of a map");
		} catch (Exception e) {
			classLogger.warn(
					"Failed to retrieve insight encryption secrets from AWS Secrets Manager for secret path '{}' (projectId={}, insightId={}). Returning empty secrets map.",
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
		String secretPath = getPathForEngine(eType, engineId);
		try {
			writeSecretMap(secretPath, nameValuePairs);
			return true;
		} catch (Exception e) {
			classLogger.error(
					"Failed to write engine secrets to AWS Secrets Manager for secret path '{}' (engineId={}, catalogType={}, keys={}).",
					secretPath, engineId, eType, nameValuePairs.keySet(), e);
			return false;
		}
	}

	@Override
	public boolean deleteEngineSecrets(CATALOG_TYPE eType, String engineId, String engineName) {
		String secretPath = getPathForEngine(eType, engineId);
		try {
			this.secretsManagerClient.deleteSecret(DeleteSecretRequest.builder().secretId(secretPath).build());
			return true;
		} catch (Exception e) {
			classLogger.warn(
					"Failed to delete engine secret from AWS Secrets Manager for secret path '{}' (engineId={}, catalogType={}).",
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
		String secretPath = getInsightPath(projectId, insightId);
		try {
			writeSecretMap(secretPath, nameValuePairs);
			return true;
		} catch (Exception e) {
			classLogger.error(
					"Failed to write insight secrets to AWS Secrets Manager for secret path '{}' (projectId={}, insightId={}, keys={}).",
					secretPath, projectId, insightId, nameValuePairs.keySet(), e);
			return false;
		}
	}

	@Override
	public boolean writeInsightEncryptionSecrets(String projectId, String projectName, String insightId,
			Map<String, Object> nameValuePairs) {
		String secretPath = getInsightPath(projectId, insightId) + "-" + INSIGHT_ENCRYPTION_NAME;
		try {
			writeSecretMap(secretPath, nameValuePairs);
			return true;
		} catch (Exception e) {
			classLogger.error(
					"Failed to write insight encryption secrets to AWS Secrets Manager for secret path '{}' (projectId={}, insightId={}, keys={}).",
					secretPath, projectId, insightId, nameValuePairs.keySet(), e);
			return false;
		}
	}

}
