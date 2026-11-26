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
package prerna.engine.api;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.logging.IgnoreEngineLogging;
import prerna.util.Constants;
import prerna.util.InternalUtility;

public interface IEngine extends Closeable {

	String METADATA_FILE_SUFFIX = "_metadata.json";

	/**
	 * Written into a model engine export so the MODELMETADATA values travel with
	 * the zip. Consumed and deleted on upload.
	 */
	String MODEL_METADATA_FILE_SUFFIX = "_modelmetadata.json";

	String PIPELINE = "PIPELINE";

	/**
	 * The settings an editor may change with {@link #updateSetting(String, String)}
	 */
	Set<String> SETTINGS_EDITORS_CHANGE = Set.of(PIPELINE, Constants.MCP_ENABLED);

	enum CATALOG_TYPE {
		DATABASE, STORAGE, MODEL, VECTOR, FUNCTION, GUARDRAIL,
		// not really used anymore
		VENV,
		// special kind for IProject
		PROJECT,
	};

	/**
	 * Sets the unique id for the engine
	 * 
	 * @param engineId - id to set the engine
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	void setEngineId(String engineId);

	/**
	 * Gets the engine name for this engine
	 * 
	 * @return Name of the engine
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	String getEngineId();

	/**
	 * Sets the name of the engine. This may be a lot of times the same as the
	 * Repository Name
	 * 
	 * @param engineName - Name of the engine that this is being set to
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	void setEngineName(String engineName);

	/**
	 * Gets the engine name for this engine
	 *
	 * @return Name of the engine
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	String getEngineName();

	/**
	 * Sets the user-facing display name for this engine
	 *
	 * @param displayName - Display name of the engine
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	void setDisplayName(String displayName);

	/**
	 * Gets the user-facing display name for this engine. Falls back to
	 * {@link #getEngineName()} if no display name has been set.
	 *
	 * @return Display name of the engine
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	String getDisplayName();

	/**
	 * Opens an engine as defined by its properties file. What is included in the
	 * properties file is dependent on the type of engine that is being initiated.
	 * It also includes the ENGINE and ENGINE_ALIAS which coincide with the engineId
	 * and engineName This is the function that first initializes the connection to
	 * the engine or at least defines how to connect if done in lazy fashion.
	 * 
	 * @param smssFilePath The file path to the smss file containing the engine
	 *                     connection details
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	void open(String smssFilePath) throws Exception;

	/**
	 * Opens an engine as defined by its properties file. What is included in the
	 * properties file is dependent on the type of engine that is being initiated.
	 * It also includes the ENGINE and ENGINE_ALIAS which coincide with the engineId
	 * and engineName This is the function that first initializes the connection to
	 * the engine or at least defines how to connect if done in lazy fashion.
	 * 
	 * @param smssProp The properties object loaded from the smss file containing
	 *                 the engine connection details
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	void open(Properties smssProp) throws Exception;

	/**
	 * Closes the engine and opens it again from its own smss file, so changes saved
	 * to the file take effect. It takes no path, so a caller never picks the file
	 * the engine opens from.
	 *
	 * @throws Exception if the engine cannot be opened again
	 */
	@IgnoreEngineLogging
	@EngineAccess(value = AccessPermissionEnum.EDIT, projectCode = AccessPermissionEnum.OWNER)
	default void reload() throws Exception {
		String smssFilePath = getSmssFilePath();
		close();
		open(smssFilePath);
	}

	/**
	 * Saves one setting an editor may change, such as {@link #PIPELINE} or
	 * {@link Constants#MCP_ENABLED}, to the engine's smss file and its open
	 * settings. Every other setting is changed by an owner, through the smss.
	 *
	 * @param key   the setting, one of {@link #SETTINGS_EDITORS_CHANGE}
	 * @param value its new value
	 * @throws IOException              if the smss file cannot be written
	 * @throws IllegalArgumentException if the setting is not one an editor may
	 *                                  change
	 */
	@IgnoreEngineLogging
	@EngineAccess(value = AccessPermissionEnum.EDIT, projectCode = AccessPermissionEnum.OWNER)
	default void updateSetting(String key, String value) throws IOException {
		if (key == null || !SETTINGS_EDITORS_CHANGE.contains(key)) {
			throw new IllegalArgumentException(
					"Only the engine's owners can change the setting " + key + ", through its smss");
		}
		InternalUtility.changePropertiesFileValue(getSmssFilePath(), key, value);
		Properties smssProp = getSmssProp();
		if (smssProp != null) {
			smssProp.setProperty(key, value);
		}
	}

	/**
	 * 
	 * @param smssFilePath
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	void setSmssFilePath(String smssFilePath);

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	String getSmssFilePath();

	/**
	 * Sets the properties object
	 *
	 * @param prop
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	void setSmssProp(Properties smssProp);

	/**
	 * Returns the engine's settings, secrets included, so only its owners read
	 * them. Code that needs one setting uses the getter for it, such as
	 * {@link #getCatalogSubType()} or {@link #getPipelineFile()}.
	 *
	 * @return the engine's settings
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	Properties getSmssProp();

	/**
	 * The name of the engine's pipeline file, its {@link #PIPELINE} setting.
	 *
	 * @return the pipeline file name, or null when the engine has none
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	default String getPipelineFile() {
		Properties smssProp = getSmssProp();
		return smssProp == null ? null : smssProp.getProperty(PIPELINE);
	}

	/**
	 * Get the original prop file content - w/o additional alterations during
	 * opening (change primarily happens in H2 Server DB where we alter the
	 * connection URL to tcp with dynamic open port)
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	Properties getOrigSmssProp();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	CATALOG_TYPE getCatalogType();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	String getCatalogSubType(Properties smssProp);

	/**
	 * The engine's catalog subtype, such as a model's brand, read from its own
	 * settings.
	 *
	 * @return the subtype, or null when the engine has none
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	default String getCatalogSubType() {
		return getCatalogSubType(getSmssProp());
	}

	/**
	 * Deletes the engine and any stored configuration
	 * 
	 * @throws IOException
	 */
	@EngineAccess(AccessPermissionEnum.OWNER)
	void delete() throws IOException;

	/**
	 * Does this engine hold any file locks that would require a close to
	 * export/perform other operations
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	boolean holdsFileLocks();

	/**
	 * True when engine should not have assets
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	boolean isBasic();

	/**
	 * True when engine should not have assets
	 * 
	 * @param isBasic
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.OWNER)
	void setBasic(boolean isBasic);

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	boolean isMCPEnabled();

	/**
	 * 
	 * @param loggerName
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	Logger getEngineLogger(String loggerName);

	/**
	 * Whether the engine's calls keep their input and output. Only then does the
	 * engine proxy write each call's request and response to the audit logs, a
	 * model or vector database keep the prompt and response in the model inference
	 * logs, and a model keep a room's history. Most engines follow
	 * KEEP_INPUT_OUTPUT in their SMSS, which is meant for models and vector
	 * databases, whose calls carry prompts and document text. A database always
	 * returns true, so the audit log row of each of its calls holds the SQL and its
	 * DATABASE_WRITE events always hold the SQL's hash.
	 *
	 * @return true when the engine's calls keep their input and output
	 */
	@IgnoreEngineLogging
	@EngineAccess(AccessPermissionEnum.READ_ONLY)
	boolean keepInputOutput();
}
