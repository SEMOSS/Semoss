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
package prerna.io.connector.secrets.hashicorp.vault;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.primitives.Bytes;
import com.google.gson.JsonObject;

import io.github.jopenlibs.vault.SslConfig;
import io.github.jopenlibs.vault.Vault;
import io.github.jopenlibs.vault.VaultConfig;
import io.github.jopenlibs.vault.json.JsonArray;
import io.github.jopenlibs.vault.json.JsonValue;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.SmssUtilities;
import prerna.io.connector.secrets.AbstractSecrets;
import prerna.io.connector.secrets.ISecrets;
import prerna.security.HttpHelperUtility;
import prerna.util.Utility;

public final class HashiCorpVaultUtil extends AbstractSecrets {

	private static final Logger classLogger = LogManager.getLogger(HashiCorpVaultUtil.class);

	private static final String VAULT_ADDR = "VAULT_ADDR";
	private static final String VAULT_TOKEN = "VAULT_TOKEN";
	private static final String VAULT_TOKEN_HEADER_KEY = "X-Vault-Token";

	private static volatile HashiCorpVaultUtil instance;

	private Vault vault;
	private VaultConfig config;

	private HashiCorpVaultUtil() throws Exception {
		createVault();
	}

	private void createVault() throws Exception {
		this.config = new VaultConfig().address(getInput(VAULT_ADDR)) // Defaults to "VAULT_ADDR" environment variable
				.token(getInput(VAULT_TOKEN)) // Defaults to "VAULT_TOKEN" environment variable
				.openTimeout(5) // Defaults to "VAULT_OPEN_TIMEOUT" environment variable
				.readTimeout(30) // Defaults to "VAULT_READ_TIMEOUT" environment variable
				.sslConfig(new SslConfig().build()).build();
		this.vault = Vault.create(config);
	}

	public static HashiCorpVaultUtil getInstance() {
		if (instance != null) {
			return instance;
		}

		if (instance == null) {
			synchronized (HashiCorpVaultUtil.class) {
				if (instance == null) {
					try {
						instance = new HashiCorpVaultUtil();
					} catch (Exception e) {
						classLogger.error(
								"Failed to initialize HashiCorpVaultUtil singleton. Verify secrets inputs {} and {} are configured correctly.",
								VAULT_ADDR, VAULT_TOKEN, e);
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

	@Override
	public Map<String, Object> getEngineSecrets(IEngine.CATALOG_TYPE eType, String engineId, String engineName) {
		String secretPath = SmssUtilities.getUniqueName(engineName, engineId);
		secretPath = Utility.encodeURIComponent(secretPath);
		try {
			return new HashMap<String, Object>(
					this.vault.logical().read(getPathForEngine(eType, secretPath)).getData());
		} catch (Exception e) {
			classLogger.error(
					"Failed to read engine secrets from Vault for engineId={}, engineName={}, catalogType={}, path={}.",
					engineId, engineName, eType, getPathForEngine(eType, secretPath), e);
		}

		return null;
	}

	@Override
	public Map<String, Object> getInsightSecrets(String projectId, String projectName, String insightId) {
		String secretPath = SmssUtilities.getUniqueName(projectName, projectId);
		secretPath = Utility.encodeURIComponent(secretPath);
		try {
			return new HashMap<String, Object>(
					this.vault.logical().read(getInsightPath(secretPath, insightId)).getData());
		} catch (Exception e) {
			classLogger.error(
					"Failed to read insight secrets from Vault for projectId={}, projectName={}, insightId={}, path={}.",
					projectId, projectName, insightId, getInsightPath(secretPath, insightId), e);
		}

		return null;
	}

	@Override
	public Map<String, Object> getInsightEncryptionSecrets(String projectId, String projectName, String insightId) {
		String secretPath = SmssUtilities.getUniqueName(projectName, projectId);
		secretPath = Utility.encodeURIComponent(secretPath);
		try {
			io.github.jopenlibs.vault.json.JsonObject jsonObject = this.vault.logical()
					.read(getInsightPath(secretPath, insightId + "/" + ISecrets.INSIGHT_ENCRYPTION_NAME))
					.getDataObject();
			String secret = jsonObject.getString(ISecrets.SECRET);
			String salt = jsonObject.getString(ISecrets.SALT);
			Iterator<JsonValue> ivIterator = jsonObject.get(ISecrets.IV).asArray().iterator();
			List<Byte> iv = new ArrayList<>();
			while (ivIterator.hasNext()) {
				iv.add((byte) ivIterator.next().asInt());
			}
			Map<String, Object> cacheData = new HashMap<>();
			cacheData.put(ISecrets.SECRET, secret);
			cacheData.put(ISecrets.SALT, salt);
			cacheData.put(ISecrets.IV, Bytes.toArray(iv));
			return cacheData;
		} catch (Exception e) {
			classLogger.error(
					"Failed to read insight encryption secrets from Vault for projectId={}, projectName={}, insightId={}, path={}.",
					projectId, projectName, insightId,
					getInsightPath(secretPath, String.format("%s/%s", insightId, ISecrets.INSIGHT_ENCRYPTION_NAME)), e);
		}

		return null;
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
		String secretPath = SmssUtilities.getUniqueName(engineName, engineId);
		secretPath = Utility.encodeURIComponent(secretPath);
		try {
			this.vault.logical().write(getPathForEngine(eType, secretPath), nameValuePairs);
			return true;
		} catch (Exception e) {
			classLogger.error(
					"Failed to write engine secrets to Vault for engineId={}, engineName={}, catalogType={}, path={}, keys={}.",
					engineId, engineName, eType, getPathForEngine(eType, secretPath), nameValuePairs.keySet(), e);
			return false;
		}
	}

	@Override
	public boolean deleteEngineSecrets(CATALOG_TYPE eType, String engineId, String engineName) {
		String secretPath = SmssUtilities.getUniqueName(engineName, engineId);
		secretPath = Utility.encodeURIComponent(secretPath);
		try {
			this.vault.logical().delete(getPathForEngine(eType, secretPath));
			return true;
		} catch (Exception e) {
			classLogger.error(
					"Failed to delete engine secrets from Vault for engineId={}, engineName={}, catalogType={}, path={}.",
					engineId, engineName, eType, getPathForEngine(eType, secretPath), e);
			return false;
		}
	}

	@Override
	public boolean writeInsightSecret(String projectId, String projectName, String insightId, String key,
			Object value) {
		Map<String, Object> nameValuePairs = new HashMap<>();
		nameValuePairs.put(key, value);
		return writeInsightSecrets(insightId, projectName, projectId, nameValuePairs);
	}

	@Override
	public boolean writeInsightSecrets(String projectId, String projectName, String insightId,
			Map<String, Object> nameValuePairs) {
		String secretPath = SmssUtilities.getUniqueName(projectName, projectId);
		secretPath = Utility.encodeURIComponent(secretPath);

		try {
			this.vault.logical().write(getInsightPath(secretPath, insightId), nameValuePairs);
			return true;
		} catch (Exception e) {
			classLogger.error(
					"Failed to write insight secrets to Vault for projectId={}, projectName={}, insightId={}, path={}, keys={}.",
					projectId, projectName, insightId, getInsightPath(secretPath, insightId), nameValuePairs.keySet(),
					e);
			return false;
		}
	}

	@Override
	public boolean writeInsightEncryptionSecrets(String projectId, String projectName, String insightId,
			Map<String, Object> nameValuePairs) {
		String secretPath = SmssUtilities.getUniqueName(projectName, projectId);
		secretPath = Utility.encodeURIComponent(secretPath);
		byte[] iv = (byte[]) nameValuePairs.get(ISecrets.IV);
		io.github.jopenlibs.vault.json.JsonArray jsonArray = new JsonArray();
		for (byte i : iv) {
			jsonArray.add(i);
		}
		nameValuePairs.put(ISecrets.IV, jsonArray);
		try {
			this.vault.logical().write(getInsightPath(secretPath, insightId + "/" + ISecrets.INSIGHT_ENCRYPTION_NAME),
					nameValuePairs);
			return true;
		} catch (Exception e) {
			classLogger.error(
					"Failed to write insight encryption secrets to Vault for projectId={}, projectName={}, insightId={}, path={}, keys={}.",
					projectId, projectName, insightId,
					getInsightPath(secretPath, String.format("%s/%s", insightId, ISecrets.INSIGHT_ENCRYPTION_NAME)),
					nameValuePairs.keySet(), e);
			return false;
		}
	}

	/**
	 * Create an engine KV engine
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */
	public void createEngineSecretEngine(IEngine.CATALOG_TYPE eType) {
		String lookup = getInputNameForEngine(eType);
		String name = getInput(lookup);

		JsonObject json = new JsonObject();
		json.addProperty("type", "kv");
		json.addProperty("description", "Secrets for " + eType + " smss files");
		JsonObject version = new JsonObject();
		json.addProperty("version", "2");
		json.add("options", version);

		Map<String, String> headerMap = new HashMap<>();
		headerMap.put(VAULT_TOKEN_HEADER_KEY, getInput(VAULT_TOKEN));

		String response = HttpHelperUtility.postRequestStringBody(getInput(VAULT_ADDR) + "/v1/sys/mounts/" + name,
				headerMap, json.toString(), ContentType.APPLICATION_JSON, null, null, null);

		classLogger.info("Response for creating {} = {}", eType, response);
	}

	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////

//	public static void main(String[] args) throws Exception, ParseException, IOException {
//		TestUtilityMethods.loadDIHelper();
//		
//		HashiCorpVaultUtil instance = HashiCorpVaultUtil.getInstance();
//		instance.createEngineSecretEngine(IEngine.CATALOG_TYPE.DATABASE);
//		
//		Map<String, Object> nameValuePairs = new HashMap<>();
//		nameValuePairs.put("PASSWORD","password");
//		instance.writeEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, "fe5e2c23-59e6-42ae-939d-b2ca9699f38c", "test-name", nameValuePairs);
//		Map<String, String> dbSecrets = instance.getEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, "fe5e2c23-59e6-42ae-939d-b2ca9699f38c", "test-name");
//		System.out.println(dbSecrets);
//	}

}
