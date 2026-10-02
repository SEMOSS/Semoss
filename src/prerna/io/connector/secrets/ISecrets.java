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
package prerna.io.connector.secrets;

import java.util.Map;

import prerna.engine.api.IEngine;

public interface ISecrets {

	String SECRETS_DB_PATH = "SECRETS_DB_PATH";
	String SECRETS_STORAGE_PATH = "SECRETS_STORAGE_PATH";
	String SECRETS_MODEL_PATH = "SECRETS_MODEL_PATH";
	String SECRETS_VECTOR_PATH = "SECRETS_VECTOR_PATH";
	String SECRETS_FUNCTION_PATH = "SECRETS_FUNCTION_PATH";
	String SECRETS_GUARDRAIL_PATH = "SECRETS_GUARDRAIL_PATH";
	String SECRETS_PROJECT_PATH = "SECRETS_PROJECT_PATH";
	String SECRETS_VENV_PATH = "SECRETS_VENV_PATH";

	String INSIGHT_ENCRYPTION_NAME = "insightencrypt";

	String AZURE_KEYVAULT = "AZURE_KEYVAULT";
	String HASHICORP_VAULT = "HASHICORP_VAULT";
	String AWS_SECRETS_MANAGER = "AWS_SECRETS_MANAGER";
	String IV = "iv";
	String SECRET = "secret";
	String SALT = "salt";

	/**
	 * 
	 * @param eType
	 * @param engineId
	 * @param engineName
	 * @return
	 */
	Map<String, Object> getEngineSecrets(IEngine.CATALOG_TYPE eType, String engineId, String engineName);

	/**
	 * Get the secrets associated with an insight
	 * 
	 * @param projectId
	 * @param projectName
	 * @param insightId
	 * @return
	 */
	Map<String, Object> getInsightSecrets(String projectId, String projectName, String insightId);

	/**
	 * Get the insight encryption key
	 * 
	 * @param projectId
	 * @param projectName
	 * @param insightId
	 * @return
	 */
	Map<String, Object> getInsightEncryptionSecrets(String projectId, String projectName, String insightId);

	/**
	 * Write a secret key-value pair for an engine
	 * 
	 * @param eType
	 * @param engineId
	 * @param engineName
	 * @param key
	 * @param value
	 * @return
	 */
	boolean appendEngineSecret(IEngine.CATALOG_TYPE eType, String engineId, String engineName, String key,
			Object value);

	/**
	 * Write a set of secret key-value pairs for an engine
	 * 
	 * @param eType
	 * @param engineId
	 * @param engineName
	 * @param nameValuePairs
	 * @return
	 */
	boolean writeEngineSecrets(IEngine.CATALOG_TYPE eType, String engineId, String engineName,
			Map<String, Object> nameValuePairs);

	/**
	 * Delete the secret for an engine
	 * 
	 * @param eType
	 * @param engineId
	 * @param engineName
	 * @return
	 */
	boolean deleteEngineSecrets(IEngine.CATALOG_TYPE eType, String engineId, String engineName);

	/**
	 * Write a secret key-value pair for a insight
	 * 
	 * @param projectId
	 * @param projectName
	 * @param insightId
	 * @param key
	 * @param value
	 * @return
	 */
	boolean writeInsightSecret(String projectId, String projectName, String insightId, String key, Object value);

	/**
	 * Write a set of secret key-value pairs for a insight
	 * 
	 * @param projectId
	 * @param projectName
	 * @param insightId
	 * @param nameValuePairs
	 * @return
	 */
	boolean writeInsightSecrets(String projectId, String projectName, String insightId,
			Map<String, Object> nameValuePairs);

	/**
	 * Write the secret for the insight encryption
	 * 
	 * @param projectId
	 * @param projectName
	 * @param insightId
	 * @param nameValuePairs
	 * @return
	 */
	boolean writeInsightEncryptionSecrets(String projectId, String projectName, String insightId,
			Map<String, Object> nameValuePairs);

}