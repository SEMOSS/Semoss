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

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
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
import prerna.util.ValueUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClientBuilder;
import software.amazon.awssdk.services.secretsmanager.model.DescribeSecretResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceExistsException;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

/**
 * Secret store backed by AWS Secrets Manager.
 *
 * Each engine, insight and insight encryption key is one secret whose value is
 * the JSON object of its name value pairs. Secrets are named by id, with the
 * catalog base path from {@code SECRETS_DB_PATH}, {@code SECRETS_PROJECT_PATH}
 * and the other secret path inputs as the prefix, such as
 * {@code semoss/database/<engineId>} or
 * {@code semoss/project/<projectId>/<insightId>/insightencrypt}.
 *
 * Inputs, read from the environment first and then RDF_Map:
 * <ul>
 * <li>{@code AWS_SECRETS_MANAGER_REGION} the region of the secrets, otherwise
 * the default AWS region chain</li>
 * <li>{@code AWS_SECRETS_MANAGER_ENDPOINT} an endpoint override, such as a VPC
 * interface endpoint</li>
 * <li>{@code AWS_SECRETS_MANAGER_ACCESS_KEY} and
 * {@code AWS_SECRETS_MANAGER_SECRET_KEY} static credentials, with
 * {@code AWS_SECRETS_MANAGER_SESSION_TOKEN} for temporary ones. Without an
 * access key the default AWS credential chain signs the calls: environment
 * variables, the shared credentials file, web identity, container and instance
 * profile credentials.</li>
 * <li>{@code AWS_SECRETS_MANAGER_KMS_KEY_ID} the KMS key that encrypts new
 * secrets, otherwise the account's {@code aws/secretsmanager} key</li>
 * </ul>
 *
 * Secret names allow letters, digits and {@code /_+=.@-}.
 */
public final class AwsSecretsManagerUtil extends AbstractSecrets {

	private static final Logger classLogger = LogManager.getLogger(AwsSecretsManagerUtil.class);

	private static final String AWS_SECRETS_MANAGER_REGION = "AWS_SECRETS_MANAGER_REGION";
	private static final String AWS_SECRETS_MANAGER_ENDPOINT = "AWS_SECRETS_MANAGER_ENDPOINT";
	private static final String AWS_SECRETS_MANAGER_ACCESS_KEY = "AWS_SECRETS_MANAGER_ACCESS_KEY";
	private static final String AWS_SECRETS_MANAGER_SECRET_KEY = "AWS_SECRETS_MANAGER_SECRET_KEY";
	private static final String AWS_SECRETS_MANAGER_SESSION_TOKEN = "AWS_SECRETS_MANAGER_SESSION_TOKEN";
	private static final String AWS_SECRETS_MANAGER_KMS_KEY_ID = "AWS_SECRETS_MANAGER_KMS_KEY_ID";

	private static final String PATH_SEPARATOR = "/";

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
	private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {
	}.getType();

	private static volatile AwsSecretsManagerUtil instance;

	private SecretsManagerClient secretsClient;
	private String kmsKeyId;

	private AwsSecretsManagerUtil() {
		createSecretsClient();
	}

	/**
	 * Builds the Secrets Manager client from the region, endpoint, credential and
	 * KMS inputs.
	 */
	private void createSecretsClient() {
		SecretsManagerClientBuilder builder = SecretsManagerClient.builder();

		String region = ValueUtils.trimToNull(getInput(AWS_SECRETS_MANAGER_REGION));
		if (region != null) {
			builder.region(Region.of(region));
		}

		String endpoint = ValueUtils.trimToNull(getInput(AWS_SECRETS_MANAGER_ENDPOINT));
		if (endpoint != null) {
			try {
				builder.endpointOverride(new URI(endpoint));
			} catch (URISyntaxException e) {
				throw new IllegalArgumentException(
						"Invalid endpoint for " + AWS_SECRETS_MANAGER_ENDPOINT + ": " + endpoint, e);
			}
		}

		String accessKey = ValueUtils.trimToNull(getInput(AWS_SECRETS_MANAGER_ACCESS_KEY));
		if (accessKey != null) {
			String secretKey = ValueUtils.requireNonBlank(getInput(AWS_SECRETS_MANAGER_SECRET_KEY), "Must define "
					+ AWS_SECRETS_MANAGER_SECRET_KEY + " when " + AWS_SECRETS_MANAGER_ACCESS_KEY + " is set");
			String sessionToken = ValueUtils.trimToNull(getInput(AWS_SECRETS_MANAGER_SESSION_TOKEN));
			AwsCredentials creds = sessionToken == null ? AwsBasicCredentials.create(accessKey, secretKey)
					: AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
			builder.credentialsProvider(StaticCredentialsProvider.create(creds));
		} else {
			builder.credentialsProvider(DefaultCredentialsProvider.builder().build());
			classLogger.info("No {} configured, using the default AWS credential chain for Secrets Manager",
					AWS_SECRETS_MANAGER_ACCESS_KEY);
		}

		this.kmsKeyId = ValueUtils.trimToNull(getInput(AWS_SECRETS_MANAGER_KMS_KEY_ID));
		this.secretsClient = builder.build();
	}

	/**
	 * Returns the shared Secrets Manager connector, creating it on first use.
	 *
	 * @return the connector, or null when the client could not be created from the
	 *         configured inputs
	 */
	public static AwsSecretsManagerUtil getInstance() {
		if (instance == null) {
			synchronized (AwsSecretsManagerUtil.class) {
				if (instance == null) {
					try {
						instance = new AwsSecretsManagerUtil();
					} catch (Exception e) {
						classLogger.error(
								"Failed to initialize AwsSecretsManagerUtil singleton. Verify the configuration for {}, {} and the AWS credentials.",
								AWS_SECRETS_MANAGER_REGION, AWS_SECRETS_MANAGER_ENDPOINT, e);
					}
				}
			}
		}

		return instance;
	}

	/**
	 * Gets the secret name for an engine, prefixed with the base path configured
	 * for its catalog type.
	 *
	 * @param eType    the catalog type of the engine
	 * @param engineId the engine id
	 * @return the secret name
	 */
	private String getPathForEngine(IEngine.CATALOG_TYPE eType, String engineId) {
		String base = ValueUtils.trimToNull(getBaseForEngine(eType));
		if (base != null) {
			return base + PATH_SEPARATOR + engineId;
		}
		return engineId;
	}

	/**
	 * Gets the secret name for an insight, nested under its project.
	 *
	 * @param projectId the project id
	 * @param insightId the insight id
	 * @return the secret name
	 */
	private String getInsightPath(String projectId, String insightId) {
		return getPathForEngine(IEngine.CATALOG_TYPE.PROJECT, projectId) + PATH_SEPARATOR + insightId;
	}

	/**
	 * Gets the secret name for the encryption key of an insight.
	 *
	 * @param projectId the project id
	 * @param insightId the insight id
	 * @return the secret name
	 */
	private String getInsightEncryptionPath(String projectId, String insightId) {
		return getInsightPath(projectId, insightId) + PATH_SEPARATOR + INSIGHT_ENCRYPTION_NAME;
	}

	@Override
	public Map<String, Object> getEngineSecrets(IEngine.CATALOG_TYPE eType, String engineId, String engineName) {
		String secretName = getPathForEngine(eType, engineId);
		try {
			return readSecret(secretName);
		} catch (Exception e) {
			classLogger.warn(
					"Failed to retrieve engine secrets from AWS Secrets Manager for secret '{}' (engineId={}, catalogType={}). Returning empty secrets map.",
					secretName, engineId, eType, e);
			return new HashMap<>();
		}
	}

	@Override
	public Map<String, Object> getInsightSecrets(String projectId, String projectName, String insightId) {
		String secretName = getInsightPath(projectId, insightId);
		try {
			return readSecret(secretName);
		} catch (Exception e) {
			classLogger.warn(
					"Failed to retrieve insight secrets from AWS Secrets Manager for secret '{}' (projectId={}, insightId={}). Returning empty secrets map.",
					secretName, projectId, insightId, e);
			return new HashMap<>();
		}
	}

	@Override
	public Map<String, Object> getInsightEncryptionSecrets(String projectId, String projectName, String insightId) {
		String secretName = getInsightEncryptionPath(projectId, insightId);
		try {
			Map<String, Object> data = readSecret(secretName);
			if (data.containsKey(IV)) {
				data.put(IV, toInitializationVector(data.get(IV)));
			}
			return data;
		} catch (Exception e) {
			classLogger.warn(
					"Failed to retrieve insight encryption secrets from AWS Secrets Manager for secret '{}' (projectId={}, insightId={}). Returning empty secrets map.",
					secretName, projectId, insightId, e);
			return new HashMap<>();
		}
	}

	@Override
	public boolean appendEngineSecret(IEngine.CATALOG_TYPE eType, String engineId, String engineName, String key,
			Object value) {
		Map<String, Object> nameValuePairs = getEngineSecrets(eType, engineId, engineName);
		nameValuePairs.put(key, value);
		return writeEngineSecrets(eType, engineId, engineName, nameValuePairs);
	}

	@Override
	public boolean writeEngineSecrets(IEngine.CATALOG_TYPE eType, String engineId, String engineName,
			Map<String, Object> nameValuePairs) {
		writeSecret(getPathForEngine(eType, engineId), nameValuePairs);
		return true;
	}

	@Override
	public boolean deleteEngineSecrets(CATALOG_TYPE eType, String engineId, String engineName) {
		String secretName = getPathForEngine(eType, engineId);
		try {
			this.secretsClient.deleteSecret(r -> r.secretId(secretName));
			return true;
		} catch (ResourceNotFoundException e) {
			classLogger.debug("No engine secret in AWS Secrets Manager to delete for secret '{}' (engineId={}).",
					secretName, engineId);
			return true;
		} catch (Exception e) {
			classLogger.warn(
					"Failed to delete engine secret from AWS Secrets Manager for secret '{}' (engineId={}, catalogType={}).",
					secretName, engineId, eType, e);
			return false;
		}
	}

	@Override
	public boolean writeInsightSecret(String projectId, String projectName, String insightId, String key,
			Object value) {
		Map<String, Object> nameValuePairs = getInsightSecrets(projectId, projectName, insightId);
		nameValuePairs.put(key, value);
		return writeInsightSecrets(projectId, projectName, insightId, nameValuePairs);
	}

	@Override
	public boolean writeInsightSecrets(String projectId, String projectName, String insightId,
			Map<String, Object> nameValuePairs) {
		writeSecret(getInsightPath(projectId, insightId), nameValuePairs);
		return true;
	}

	@Override
	public boolean writeInsightEncryptionSecrets(String projectId, String projectName, String insightId,
			Map<String, Object> nameValuePairs) {
		writeSecret(getInsightEncryptionPath(projectId, insightId), nameValuePairs);
		return true;
	}

	/**
	 * Reads a secret and parses its value as a JSON object.
	 *
	 * @param secretName the secret name
	 * @return the name value pairs of the secret, or an empty map when the secret
	 *         does not exist
	 * @throws IllegalArgumentException when the secret value is not a JSON object
	 */
	private Map<String, Object> readSecret(String secretName) {
		String value;
		try {
			value = this.secretsClient.getSecretValue(r -> r.secretId(secretName)).secretString();
		} catch (ResourceNotFoundException e) {
			classLogger.debug("No secret in AWS Secrets Manager for '{}'", secretName);
			return new HashMap<>();
		}

		Map<String, Object> data;
		try {
			data = GSON.fromJson(value, MAP_TYPE);
		} catch (JsonSyntaxException e) {
			throw new IllegalArgumentException("Invalid format for secret '" + secretName
					+ "'. Must be a valid string representation of a map", e);
		}
		if (data == null) {
			throw new IllegalArgumentException(
					"Secret '" + secretName + "' has no string value. Must be a valid string representation of a map");
		}
		return new HashMap<>(data);
	}

	/**
	 * Saves the name value pairs as the value of a secret. A secret that does not
	 * exist yet is created, and a secret scheduled for deletion is restored before
	 * its new value is saved.
	 *
	 * @param secretName     the secret name
	 * @param nameValuePairs the values to store as a JSON object
	 */
	private void writeSecret(String secretName, Map<String, Object> nameValuePairs) {
		String value = GSON.toJson(nameValuePairs);

		DescribeSecretResponse description;
		try {
			description = this.secretsClient.describeSecret(r -> r.secretId(secretName));
		} catch (ResourceNotFoundException e) {
			try {
				this.secretsClient.createSecret(r -> {
					r.name(secretName).secretString(value);
					if (this.kmsKeyId != null) {
						r.kmsKeyId(this.kmsKeyId);
					}
				});
				return;
			} catch (ResourceExistsException e2) {
				classLogger.debug("Secret '{}' was created by another request, saving the value on it", secretName);
				description = null;
			}
		}

		if (description != null && description.deletedDate() != null) {
			this.secretsClient.restoreSecret(r -> r.secretId(secretName));
			classLogger.info("Restored secret '{}' in AWS Secrets Manager that was scheduled for deletion",
					secretName);
		}
		this.secretsClient.putSecretValue(r -> r.secretId(secretName).secretString(value));
	}

}
