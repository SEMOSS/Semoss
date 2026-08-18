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

import org.apache.logging.log4j.Logger;

import prerna.logging.IgnoreEngineLogging;

public interface IEngine extends Closeable {

	String METADATA_FILE_SUFFIX = "_metadata.json";

	/**
	 * Written into a model engine export so the MODELMETADATA values travel with
	 * the zip. Consumed and deleted on upload.
	 */
	String MODEL_METADATA_FILE_SUFFIX = "_modelmetadata.json";

	String PIPELINE = "PIPELINE";

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
	void setEngineId(String engineId);

	/**
	 * Gets the engine name for this engine
	 * 
	 * @return Name of the engine
	 */
	@IgnoreEngineLogging
	String getEngineId();

	/**
	 * Sets the name of the engine. This may be a lot of times the same as the
	 * Repository Name
	 * 
	 * @param engineName - Name of the engine that this is being set to
	 */
	@IgnoreEngineLogging
	void setEngineName(String engineName);

	/**
	 * Gets the engine name for this engine
	 *
	 * @return Name of the engine
	 */
	@IgnoreEngineLogging
	String getEngineName();

	/**
	 * Sets the user-facing display name for this engine
	 *
	 * @param displayName - Display name of the engine
	 */
	@IgnoreEngineLogging
	void setDisplayName(String displayName);

	/**
	 * Gets the user-facing display name for this engine. Falls back to
	 * {@link #getEngineName()} if no display name has been set.
	 *
	 * @return Display name of the engine
	 */
	@IgnoreEngineLogging
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
	void open(Properties smssProp) throws Exception;

	/**
	 * 
	 * @param smssFilePath
	 */
	@IgnoreEngineLogging
	void setSmssFilePath(String smssFilePath);

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	String getSmssFilePath();

	/**
	 * Sets the properties object
	 * 
	 * @param prop
	 */
	@IgnoreEngineLogging
	void setSmssProp(Properties smssProp);

	/**
	 * Return the prop file
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	Properties getSmssProp();

	/**
	 * Get the original prop file content - w/o additional alterations during
	 * opening (change primarily happens in H2 Server DB where we alter the
	 * connection URL to tcp with dynamic open port)
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	Properties getOrigSmssProp();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	CATALOG_TYPE getCatalogType();

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	String getCatalogSubType(Properties smssProp);

	/**
	 * Deletes the engine and any stored configuration
	 * 
	 * @throws IOException
	 */
	void delete() throws IOException;

	/**
	 * Does this engine hold any file locks that would require a close to
	 * export/perform other operations
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	boolean holdsFileLocks();

	/**
	 * True when engine should not have assets
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	boolean isBasic();

	/**
	 * True when engine should not have assets
	 * 
	 * @param isBasic
	 */
	@IgnoreEngineLogging
	void setBasic(boolean isBasic);

	/**
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	boolean isMCPEnabled();

	/**
	 * 
	 * @param loggerName
	 */
	@IgnoreEngineLogging
	Logger getEngineLogger(String loggerName);

	/**
	 * True if we are to log the input/output for an engine
	 * 
	 * @return
	 */
	@IgnoreEngineLogging
	boolean keepInputOutput();
}
