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
package prerna.engine.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.storage.AzureBlobStorageEngine;
import prerna.engine.impl.storage.MinioStorageEngine;
import prerna.engine.impl.storage.S3StorageEngine;
import prerna.project.api.IProject;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class SmssUtilities {

	private static final Logger classLogger = LogManager.getLogger(SmssUtilities.class);

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	public static final String ENGINE_REPLACEMENT = "@" + Constants.ENGINE + "@";
	public static final String PROJECT_REPLACEMENT = "@" + Constants.PROJECT + "@";

	// @formatter:off
	public static final List<String> SENSITIVE_KEYWORDS = Arrays.asList(
			// standard
			AbstractSqlQueryUtil.PASSWORD.toUpperCase(), 
			AbstractSqlQueryUtil.SECRET_KEY.toUpperCase(),
			Constants.API_KEY,

			// model
			AbstractModelEngine.OPEN_AI_KEY.toUpperCase(), 
			AbstractModelEngine.AWS_SECRET_KEY.toUpperCase(),
			AbstractModelEngine.AWS_ACCESS_KEY.toUpperCase(),
			AbstractModelEngine.GCP_SERVICE_ACCOUNT_KEY.toUpperCase(),

			// storage
			S3StorageEngine.S3_SECRET_KEY.toUpperCase(), 
			MinioStorageEngine.MINIO_SECRET_KEY.toUpperCase(),
			AzureBlobStorageEngine.AZ_PRIMARY_KEY.toUpperCase(),
			AzureBlobStorageEngine.AZ_CONN_STRING.toUpperCase(),

			// TODO should create a constants for this
			"SERVICE_ACCOUNT_CREDENTIALS"
		);
	// @formatter:on

	private SmssUtilities() {

	}

	/**
	 * Get the owl file
	 * 
	 * @param smssFilePath
	 * @param prop
	 * @return
	 */
	public static File getOwlFile(String smssFilePath, Properties prop) {
		if (prop.getProperty(Constants.OWL) == null) {
			return null;
		}
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
		prop = updateOWLSmssLegacyStructure(smssFilePath, engineId, prop);
		String owl = Utility.normalizePath(prop.getProperty(Constants.OWL));

		return new File(
				EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.DATABASE, engineId, engineName),
				Utility.normalizePath(owl));
	}

	/**
	 * This method is to update the legacy structure where the OWL was not assumed
	 * within the assets folder of an engine Marking as deprecated to remove for
	 * next major release
	 * 
	 * @param smssFilePath
	 * @param engineId
	 * @param prop
	 */
	@Deprecated
	public static Properties updateOWLSmssLegacyStructure(String smssFilePath, String engineId, Properties prop) {
		if (smssFilePath == null || prop.getProperty(Constants.OWL) == null) {
			return prop;
		}
		String owlPropStr = prop.getProperty(Constants.OWL);
		if (owlPropStr.equalsIgnoreCase("REMAKE")) {
			return prop;
		}
		if (owlPropStr.startsWith("db")) {
			// this is legacy format
			// we will now assume it is in the assets directory instead
			// of assuming another path
			Path path = Paths.get(owlPropStr);
			String filename = path.getFileName().toString();
			try {
				Utility.changePropertiesFileValue(smssFilePath, Constants.OWL, filename);
			} catch (IOException e) {
				classLogger.error("Failed to update the OWL file location in the smss file {} for engine {}",
						smssFilePath, engineId, e);
			}
			prop = Utility.loadProperties(smssFilePath);
		}
		return prop;
	}

	/**
	 * Get the insights rdbms file
	 * 
	 * @param prop
	 * @return
	 */
	public static File getInsightsRdbmsFile(Properties prop) {
		if (prop.getProperty(Constants.RDBMS_INSIGHTS) == null) {
			return null;
		}
		String rdbmsInsightsType = prop.getProperty(Constants.RDBMS_INSIGHTS_TYPE, "H2_DB");
		RdbmsTypeEnum rdbmsType = RdbmsTypeEnum.valueOf(rdbmsInsightsType);

		String baseFolder = Utility.getBaseFolder();
		String rdbmsInsights = Utility.normalizePath(baseFolder) + DIR_SEPARATOR
				+ Utility.normalizePath(prop.getProperty(Constants.RDBMS_INSIGHTS));
		String engineId = prop.getProperty(Constants.PROJECT);
		String engineName = prop.getProperty(Constants.PROJECT_ALIAS);

		rdbmsInsights = rdbmsInsights.replace(PROJECT_REPLACEMENT, getUniqueName(engineName, engineId));
		File rdbms = null;
		if (rdbmsType == RdbmsTypeEnum.SQLITE) {
			if (rdbmsInsights.endsWith(".sqlite")) {
				rdbms = new File(rdbmsInsights);
			} else {
				rdbms = new File(rdbmsInsights + ".sqlite");
			}
		} else {
			// must be H2
			rdbms = new File(rdbmsInsights + ".mv.db");
		}
		return rdbms;
	}

	/**
	 * Get the engine properties file
	 * 
	 * @param prop
	 * @return
	 */
	public static File getEngineProperties(Properties prop) {
		if (prop.getProperty(Constants.ENGINE_PROPERTIES) == null) {
			return null;
		}
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
		String engineAssets = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.DATABASE, engineId,
				engineName);
		String engineProps = Utility.normalizePath(prop.getProperty(Constants.ENGINE_PROPERTIES));
		return new File(engineAssets, engineProps);
	}

	/**
	 * Get the unique name for the engine This is the engine id __ engine name
	 * 
	 * @param prop
	 * @return
	 */
	public static String getUniqueName(Properties prop) {
		String id = prop.getProperty(Constants.ENGINE);
		String name = prop.getProperty(Constants.ENGINE_ALIAS);

		if (id == null && name == null) {
			id = prop.getProperty(Constants.PROJECT);
			name = prop.getProperty(Constants.PROJECT_ALIAS);
		}

		return getUniqueName(name, id);
	}

	/**
	 * Get unique name__id
	 * 
	 * @param name
	 * @param id
	 * @return
	 */
	public static String getUniqueName(String name, String id) {
		if (name == null) {
			return Utility.normalizePath(id);
		}
		return Utility.normalizePath(name + "__" + id);
	}

	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	/*
	 * RDF specific methods
	 */

	/**
	 * Get the JNL location
	 * 
	 * @param prop
	 * @return
	 */
	public static File getSysTapJnl(Properties prop) {
		final String PROP_NAME = "com.bigdata.journal.AbstractJournal.file";
		if (prop.getProperty(PROP_NAME) == null) {
			return null;
		}
		String baseFolder = Utility.getBaseFolder();
		String jnlLocation = baseFolder + DIR_SEPARATOR + prop.getProperty(PROP_NAME);
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

		return new File(
				Utility.normalizePath(jnlLocation.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId))));
	}

	/**
	 * Get the rdf file location
	 * 
	 * @param prop
	 * @return
	 */
	public static File getRdfFile(Properties prop) {
		if (prop.getProperty(Constants.RDF_FILE_NAME) == null) {
			return null;
		}
		String engineFolder = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.DATABASE,
				getUniqueName(prop));
		String rdfFileLoc = engineFolder + DIR_SEPARATOR + prop.getProperty(Constants.RDF_FILE_NAME);
		return new File(Utility.normalizePath(rdfFileLoc));
	}

	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	/*
	 * File specific methods
	 */

	/**
	 * Get the data file
	 * 
	 * @param prop
	 * @return
	 */
	public static File getDataFile(Properties prop) {
		if (prop.getProperty(AbstractDatabaseEngine.DATA_FILE) == null) {
			return null;
		}
		String baseFolder = Utility.getBaseFolder();
		String dataSuffix = prop.getProperty(AbstractDatabaseEngine.DATA_FILE);
		if (dataSuffix.startsWith("@BaseFolder@/")) {
			dataSuffix = dataSuffix.substring("@BaseFolder@/".length());
		}
		String dataFile = baseFolder + DIR_SEPARATOR + dataSuffix;
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

		return new File(
				Utility.normalizePath(dataFile.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId))));
	}

	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	/*
	 * Tinker specific methods
	 */

	/**
	 * Get the data file
	 * 
	 * @param prop
	 * @return
	 */
	public static File getTinkerFile(Properties prop) {
		if (prop.getProperty(Constants.TINKER_FILE) == null) {
			return null;
		}
		String baseFolder = Utility.getBaseFolder();
		String tinkerFile = null;
		String tinkerStr = prop.getProperty(Constants.TINKER_FILE);
		if (tinkerStr.contains("@BaseFolder@")) {
			tinkerFile = tinkerStr.replace("@BaseFolder@", baseFolder);
		} else {
			// could be external file outside of semoss base folder
			tinkerFile = tinkerStr;
		}
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

		return new File(
				Utility.normalizePath(tinkerFile.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId))));
	}

	/**
	 * Get the data file for an embedded neo4j graph
	 * 
	 * @param prop
	 * @return
	 */
	public static File getNeo4jFile(Properties prop) {
		if (prop.getProperty(Constants.NEO4J_FILE) == null) {
			return null;
		}
		String baseFolder = Utility.getBaseFolder();
		String neo4jFile = null;
		String neoConfPath = prop.getProperty(Constants.NEO4J_FILE);
		if (neoConfPath.contains("@BaseFolder@")) {
			neo4jFile = neoConfPath.replace("@BaseFolder@", baseFolder);
		} else {
			// could be external file outside of semoss base folder
			neo4jFile = neoConfPath;
		}
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

		return new File(
				Utility.normalizePath(neo4jFile.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId))));
	}

	/**
	 * Get the data file
	 * 
	 * @param prop
	 * @return
	 */
	public static File getJanusFile(Properties prop) {
		if (prop.getProperty(Constants.JANUS_CONF) == null) {
			return null;
		}
		String baseFolder = Utility.getBaseFolder();
		String janusFile = null;
		String janusConfPath = prop.getProperty(Constants.JANUS_CONF);
		if (janusConfPath.contains("@BaseFolder@")) {
			janusFile = janusConfPath.replace("@BaseFolder@", baseFolder);
		} else {
			// could be external file outside of semoss base folder
			janusFile = janusConfPath;
		}
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);

		return new File(
				Utility.normalizePath(janusFile.replace(ENGINE_REPLACEMENT, getUniqueName(engineName, engineId))));
	}

	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	/*
	 * project specific methods
	 */

	/**
	 * Generate the project folder and return the folder
	 * 
	 * @param projectId
	 * @param projectName
	 * @return
	 * @throws error if project folder exists
	 */
	public static File generateProjectFolder(String projectId, String projectName) throws Error {
		String baseFolder = Utility.getBaseFolder();
		if (!baseFolder.endsWith("\\") && !baseFolder.endsWith("/")) {
			baseFolder += DIR_SEPARATOR;
		}
		String projectFolder = baseFolder + Constants.PROJECT_FOLDER + DIR_SEPARATOR
				+ SmssUtilities.getUniqueName(projectName, projectId);
		File appFolder = new File(projectFolder);
		if (appFolder.exists()) {
			throw new IllegalArgumentException(
					"Project folder already contains a project with the same name. Please delete the existing project folder or provide a unique project name");
		}
		appFolder.mkdirs();
		return appFolder;
	}

	/**
	 * Get the project temporary smss location
	 * 
	 * @param projectId
	 * @param projectName
	 * @return
	 */
	private static String getProjectTempSmssLoc(String projectId, String projectName) {
		String baseFolder = Utility.getBaseFolder();
		if (!baseFolder.endsWith("\\") && !baseFolder.endsWith("/")) {
			baseFolder += DIR_SEPARATOR;
		}
		String tempSmssLoc = baseFolder + Constants.PROJECT_FOLDER + DIR_SEPARATOR
				+ SmssUtilities.getUniqueName(projectName, projectId) + ".temp";
		return tempSmssLoc;
	}

	/**
	 * Get the smss base url
	 */
	private static String getParamedSmssInsightDatabaseLocation(String rdbmsTypeStr) {
		String connectionUrl = Constants.PROJECT_FOLDER + DIR_SEPARATOR + SmssUtilities.PROJECT_REPLACEMENT
				+ DIR_SEPARATOR + "insights_database";
		// regardless of OS, connection url is always /
		connectionUrl = connectionUrl.replace('\\', '/');

		// append it on so it looks nicer
		if (rdbmsTypeStr.equalsIgnoreCase("SQLITE")) {
			connectionUrl += ".sqlite";
		}
		return connectionUrl;
	}

	/**
	 * Get the default connection url for the insights database NOTE : ONLY ALLOWING
	 * FOR H2 OR SQLITE STORAGE OPTIONS AT THE MOMENT NOTE : THIS IS THE ACTUAL FULL
	 * CONNECITON URL TODO: expand how we store this information to be able to keep
	 * in another database option / shared database
	 * 
	 * @param appName
	 * @return
	 */
	private static String getParamedNewInsightDatabaseConnectionUrl(RdbmsTypeEnum rdbmsType, String projectId,
			String projectName) {
		String baseFolder = "@" + Constants.BASE_FOLDER + "@" + DIR_SEPARATOR;
		String connectionUrl = null;
		if (rdbmsType == RdbmsTypeEnum.SQLITE) {
			// append .sqlite so it looks nicer - realize it is not required
			connectionUrl = "jdbc:sqlite:" + baseFolder + Constants.PROJECT_FOLDER + DIR_SEPARATOR
					+ SmssUtilities.getUniqueName(projectName, projectId) + DIR_SEPARATOR + "insights_database.sqlite";
		} else {
			connectionUrl = "jdbc:h2:nio:" + baseFolder + Constants.PROJECT_FOLDER + DIR_SEPARATOR
					+ SmssUtilities.getUniqueName(projectName, projectId) + DIR_SEPARATOR
					+ "insights_database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
		}
		// regardless of OS, connection url is always /
		connectionUrl = connectionUrl.replace('\\', '/');
		return connectionUrl;
	}

	/**
	 * Generate the SMSS for the project
	 * 
	 * @param projectId
	 * @param projectName
	 * @param hasPortal
	 * @param portalName
	 * @param gitProvider
	 * @param gitCloneUrl
	 * @param forceInsightDatabaseType
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryProjectSmss(String projectId, String projectName,
			IProject.PROJECT_TYPE projectEnumType, String gitProvider, String gitCloneUrl,
			RdbmsTypeEnum forceInsightDatabaseType) throws IOException {
		String projectTempSmssLoc = Utility.normalizePath(getProjectTempSmssLoc(projectId, projectName));

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File projectTempSmss = new File(projectTempSmssLoc);
		if (projectTempSmss.exists()) {
			projectTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		// also write the base properties
		File newFile = new File(projectTempSmssLoc);
		try (FileWriter writer = new FileWriter(newFile); BufferedWriter bufferedWriter = new BufferedWriter(writer);) {
			bufferedWriter.write("#Base Properties" + newLine);
			bufferedWriter.write(Constants.PROJECT + tab + projectId + newLine);
			bufferedWriter.write(Constants.PROJECT_ALIAS + tab + projectName + newLine);
			bufferedWriter.write(Constants.PROJECT_DISPLAY_NAME + tab + projectName + newLine);
			bufferedWriter.write(Constants.PROJECT_TYPE + tab + prerna.project.impl.Project.class.getName() + newLine);
			bufferedWriter.write(Constants.PROJECT_ENUM_TYPE + tab + projectEnumType.name() + newLine);

			// git details
			if (gitProvider != null && !(gitProvider = gitProvider.trim()).isEmpty()) {
				bufferedWriter.write(Constants.PROJECT_GIT_PROVIDER + tab + gitProvider + newLine);
			}
			if (gitCloneUrl != null && !(gitCloneUrl = gitCloneUrl.trim()).isEmpty()) {
				bufferedWriter.write(Constants.PROJECT_GIT_CLONE + tab + gitCloneUrl + newLine);
			}

			String rdbmsTypeStr = null;
			RdbmsTypeEnum rdbmsType = null;
			if (forceInsightDatabaseType != null) {
				rdbmsType = forceInsightDatabaseType;
				rdbmsTypeStr = rdbmsType.getLabel();
			} else {
				rdbmsTypeStr = Utility.getDIHelperProperty(Constants.DEFAULT_INSIGHTS_RDBMS);
				if (rdbmsTypeStr == null) {
					// default will be h2
					rdbmsTypeStr = "H2_DB";
				}
				rdbmsType = RdbmsTypeEnum.valueOf(rdbmsTypeStr);
			}

			bufferedWriter.write(
					Constants.RDBMS_INSIGHTS + tab + getParamedSmssInsightDatabaseLocation(rdbmsTypeStr) + newLine);
			bufferedWriter.write(Constants.RDBMS_INSIGHTS_TYPE + tab + rdbmsTypeStr + newLine);
			bufferedWriter.write(Constants.DRIVER + tab + rdbmsType.getDriver() + newLine);
			bufferedWriter.write(Constants.RDBMS_TYPE + tab + rdbmsType.getLabel() + newLine);
			if (rdbmsType == RdbmsTypeEnum.SQLITE) {
				// sqlite has no username/password
				bufferedWriter.write(Constants.USERNAME + tab + "" + newLine);
				bufferedWriter.write(Constants.PASSWORD + tab + "" + newLine);
			} else {
				bufferedWriter.write(Constants.USERNAME + tab + "sa" + newLine);
				bufferedWriter.write(Constants.PASSWORD + tab + "" + newLine);
			}
			String connectionUrl = getParamedNewInsightDatabaseConnectionUrl(rdbmsType, projectId, projectName);
			bufferedWriter.write(Constants.CONNECTION_URL + tab + connectionUrl + newLine);

			bufferedWriter.write(IEngine.PIPELINE + tab + "pipeline.json" + newLine);
		} catch (IOException e) {
			classLogger.error("Failed to write the temporary project smss file for project {}", projectId, e);
			throw new IOException("Could not generate project smss file");
		}

		return projectTempSmss;
	}

	/**
	 * Generate the SMSS for the project
	 * 
	 * @param appId
	 * @param appName
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryAssetSmss(String projectId, String projectName, boolean isAsset,
			RdbmsTypeEnum forceInsightDatabaseType) throws IOException {
		String baseFolder = Utility.getBaseFolder();
		if (!baseFolder.endsWith("\\") && !baseFolder.endsWith("/")) {
			baseFolder += DIR_SEPARATOR;
		}
		String projectTempSmssLoc = baseFolder + Constants.USER_FOLDER + DIR_SEPARATOR
				+ SmssUtilities.getUniqueName(projectName, projectId) + ".temp";

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File projectTempSmss = new File(projectTempSmssLoc);
		if (projectTempSmss.exists()) {
			projectTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		// also write the base properties
		File newFile = new File(projectTempSmssLoc);
		try (FileWriter writer = new FileWriter(newFile); BufferedWriter bufferedWriter = new BufferedWriter(writer);) {
			bufferedWriter.write("#Base Properties" + newLine);
			bufferedWriter.write(Constants.PROJECT + tab + projectId + newLine);
			bufferedWriter.write(Constants.PROJECT_ALIAS + tab + projectName + newLine);
			bufferedWriter.write(Constants.PROJECT_DISPLAY_NAME + tab + projectName + newLine);
			bufferedWriter.write(Constants.PROJECT_TYPE + tab + prerna.project.impl.Project.class.getName() + newLine);

			String rdbmsTypeStr = null;
			RdbmsTypeEnum rdbmsType = null;
			if (forceInsightDatabaseType != null) {
				rdbmsType = forceInsightDatabaseType;
				rdbmsTypeStr = rdbmsType.getLabel();
			} else {
				rdbmsTypeStr = Utility.getDIHelperProperty(Constants.DEFAULT_INSIGHTS_RDBMS);
				if (rdbmsTypeStr == null) {
					// default will be h2
					rdbmsTypeStr = "H2_DB";
				}
				rdbmsType = RdbmsTypeEnum.valueOf(rdbmsTypeStr);
			}

			// include if an asset or something else
			bufferedWriter.write(Constants.IS_ASSET_APP + tab + isAsset + newLine);
			// normal output
			bufferedWriter.write(
					Constants.RDBMS_INSIGHTS + tab + getParamedSmssInsightDatabaseLocation(rdbmsTypeStr) + newLine);
			bufferedWriter.write(Constants.RDBMS_INSIGHTS_TYPE + tab + rdbmsTypeStr + newLine);
			bufferedWriter.write(Constants.DRIVER + tab + rdbmsType.getDriver() + newLine);
			bufferedWriter.write(Constants.RDBMS_TYPE + tab + rdbmsType.getLabel() + newLine);
			if (rdbmsType == RdbmsTypeEnum.SQLITE) {
				// sqlite has no username/password
				bufferedWriter.write(Constants.USERNAME + tab + "" + newLine);
				bufferedWriter.write(Constants.PASSWORD + tab + "" + newLine);
			} else {
				bufferedWriter.write(Constants.USERNAME + tab + "sa" + newLine);
				bufferedWriter.write(Constants.PASSWORD + tab + "" + newLine);
			}
			String connectionUrl = getParamedNewInsightDatabaseConnectionUrl(rdbmsType, projectId, projectName);
			bufferedWriter.write(Constants.CONNECTION_URL + tab + connectionUrl + newLine);
		} catch (IOException e) {
			classLogger.error("Failed to write the temporary asset smss file for project {}", projectId, e);
			throw new IOException("Could not generate project smss file");
		}

		return projectTempSmss;
	}

	/**
	 * Validate the project name Does validation that: 1) The input is not
	 * null/empty 2) That the project folder doesn't exist in the file directory
	 * 
	 * @param user
	 * @param projectName
	 * @param projectId
	 * @return
	 * @throws IOException
	 */
	public static File validateProject(User user, String projectName, String projectId) throws IOException {
		if (projectName == null || projectName.isEmpty()) {
			throw new IllegalArgumentException("Need to provide a name for the project");
		}

		// need to make sure app folder doesn't already exist
		String projectLocation = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.PROJECT, projectId,
				projectName);
		File projectFolder = new File(projectLocation);
		if (projectFolder.exists()) {
			throw new IOException("Project folder already contains a project directory with the same name. "
					+ "Please delete the existing project folder or provide a unique project name");
		}

		return projectFolder;
	}

	/**
	 * Generate an empty insight database
	 * 
	 * @param projectId
	 * @param projectName
	 * @return
	 * @throws Exception
	 */
	public static RDBMSNativeEngine generateInsightsDatabase(String projectId, String projectName) throws Exception {
		String rdbmsTypeStr = Utility.getDIHelperProperty(Constants.DEFAULT_INSIGHTS_RDBMS);
		if (rdbmsTypeStr == null) {
			// default will be h2
			rdbmsTypeStr = "H2_DB";
		}
		RdbmsTypeEnum rdbmsType = RdbmsTypeEnum.valueOf(rdbmsTypeStr);

		Properties insightSmssProp = new Properties();
		/*
		 * This must be either H2 or SQLite
		 */
		String connectionUrl = getParamedNewInsightDatabaseConnectionUrl(rdbmsType, projectId, projectName);
		insightSmssProp.put(Constants.CONNECTION_URL, connectionUrl);
		if (rdbmsType == RdbmsTypeEnum.SQLITE) {
			// sqlite has no username/password
			insightSmssProp.put(Constants.USERNAME, "");
			insightSmssProp.put(Constants.PASSWORD, "");
		} else {
			insightSmssProp.put(Constants.USERNAME, "sa");
			insightSmssProp.put(Constants.PASSWORD, "");
		}
		insightSmssProp.put(Constants.DRIVER, rdbmsType.getDriver());
		insightSmssProp.put(Constants.RDBMS_TYPE, rdbmsType.getLabel());
		RDBMSNativeEngine insightEngine = new RDBMSNativeEngine();
		insightEngine.setBasic(true);
		insightEngine.open(insightSmssProp);

		runInsightCreateTableQueries(insightEngine);
		return insightEngine;
	}

	/**
	 * Run the create table queries for the insights database
	 * 
	 * @param insightEngine
	 */
	public static void runInsightCreateTableQueries(IRDBMSEngine insightEngine) {
		// CREATE TABLE QUESTION_ID (ID VARCHAR(50), QUESTION_NAME VARCHAR(255),
		// QUESTION_PERSPECTIVE VARCHAR(225), QUESTION_LAYOUT VARCHAR(225),
		// QUESTION_ORDER INT, QUESTION_DATA_MAKER VARCHAR(225), QUESTION_MAKEUP CLOB,
		// QUESTION_PROPERTIES CLOB, QUESTION_OWL CLOB, QUESTION_IS_DB_QUERY BOOLEAN,
		// DATA_TABLE_ALIGN VARCHAR(500), QUESTION_PKQL ARRAY)
		AbstractSqlQueryUtil queryUtil = insightEngine.getQueryUtil();
		String[] columns = null;
		String[] types = null;

		try {
			if (!queryUtil.tableExists(insightEngine.getConnection(), "QUESTION_ID", insightEngine.getDatabase(),
					insightEngine.getSchema())) {
				columns = new String[] { "ID", "QUESTION_NAME", "QUESTION_PERSPECTIVE", "QUESTION_LAYOUT",
						"QUESTION_ORDER", "QUESTION_DATA_MAKER", "QUESTION_MAKEUP", "DATA_TABLE_ALIGN",
						"HIDDEN_INSIGHT", "CACHEABLE", "QUESTION_PKQL" };
				types = new String[] { "VARCHAR(50)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "INT",
						"VARCHAR(255)", "CLOB", "VARCHAR(500)", "BOOLEAN", "BOOLEAN", "ARRAY" };
				// this is annoying
				// need to adjust if the engine allows array data types
				if (!queryUtil.allowArrayDatatype()) {
					types[types.length - 1] = "CLOB";
				}

				insightEngine.insertData(queryUtil.createTable("QUESTION_ID", columns, types));
			}

			// adding new insight metadata
			if (!queryUtil.tableExists(insightEngine.getConnection(), "INSIGHTMETA", insightEngine.getDatabase(),
					insightEngine.getSchema())) {
				columns = new String[] { "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER" };
				types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "CLOB", "INT" };
				insightEngine.insertData(queryUtil.createTable("INSIGHTMETA", columns, types));
			}

		} catch (Exception e) {
			classLogger.error("Failed to create the QUESTION_ID or INSIGHTMETA table in the insights database", e);
		}

		/*
		 * NOTE : THESE TABLES ARE LEGACY!!!!
		 */

		{
			/*
			 * Whenever we are finally done / have removed all our playsheet insights We can
			 * officially delete this portion
			 */

			// CREATE TABLE PARAMETER_ID (PARAMETER_ID VARCHAR(255), PARAMETER_LABEL
			// VARCHAR(255), PARAMETER_TYPE VARCHAR(225), PARAMETER_DEPENDENCY VARCHAR(225),
			// PARAMETER_QUERY VARCHAR(2000), PARAMETER_OPTIONS VARCHAR(2000),
			// PARAMETER_IS_DB_QUERY BOOLEAN, PARAMETER_MULTI_SELECT BOOLEAN,
			// PARAMETER_COMPONENT_FILTER_ID VARCHAR(255), PARAMETER_VIEW_TYPE VARCHAR(255),
			// QUESTION_ID_FK INT)

			try {
				if (!queryUtil.tableExists(insightEngine.getConnection(), "PARAMETER_ID", insightEngine.getDatabase(),
						insightEngine.getSchema())) {
					columns = new String[] { "PARAMETER_ID", "PARAMETER_LABEL", "PARAMETER_TYPE",
							"PARAMETER_DEPENDENCY", "PARAMETER_QUERY", "PARAMETER_OPTIONS", "PARAMETER_IS_DB_QUERY",
							"PARAMETER_MULTI_SELECT", "PARAMETER_COMPONENT_FILTER_ID", "PARAMETER_VIEW_TYPE",
							"QUESTION_ID_FK" };
					types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)",
							"VARCHAR(2000)", "VARCHAR(2000)", "BOOLEAN", "BOOLEAN", "VARCHAR(255)", "VARCHAR(255)",
							"INT" };
					insightEngine.insertData(queryUtil.createTable("PARAMETER_ID", columns, types));
				}
			} catch (Exception e) {
				classLogger.error("Failed to create the legacy PARAMETER_ID table in the insights database", e);
			}

			try {
				if (!queryUtil.tableExists(insightEngine.getConnection(), "UI", insightEngine.getDatabase(),
						insightEngine.getSchema())) {
					columns = new String[] { "QUESTION_ID_FK", "UI_DATA" };
					types = new String[] { "INT", "CLOB" };
					insightEngine.insertData(queryUtil.createTable("UI", columns, types));
				}
			} catch (Exception e) {
				classLogger.error("Failed to create the legacy UI table in the insights database", e);
			}
		}

		insightEngine.commit();
	}

	/**
	 * 
	 * @param currentSmssContent
	 * @return
	 */
	public static String concealSmssSensitiveInfo(String currentSmssContent) {
		StringBuilder concealedSmssContent = new StringBuilder();
		String[] currentSmssLines = currentSmssContent.split("\n");

		for (String curLine : currentSmssLines) {
			String curLineUpperMatch = curLine.toUpperCase();

			// loop through all the keys to find
			boolean found = false;
			for (String key : SmssUtilities.SENSITIVE_KEYWORDS) {
				if (curLineUpperMatch.startsWith(key + "\t") || curLineUpperMatch.startsWith(key + " ")
						|| curLineUpperMatch.startsWith(key + "=")) {
					concealedSmssContent.append(key).append("\t").append(Constants.SENSITIVE_INFO_MASK);
					found = true;
					break;
				}
			}
			if (!found) {
				concealedSmssContent.append(curLine);
			}
			concealedSmssContent.append("\n");
		}

		return concealedSmssContent.toString();
	}

	/**
	 * 
	 * @param newSmssContent
	 * @param currentSmssProperties
	 * @return
	 */
	public static String unconcealSmssSensitiveInfo(String newSmssContent, Properties currentSmssProperties) {
		return unconcealSmssSensitiveInfo(newSmssContent, currentSmssProperties, null);
	}

	/**
	 * 
	 * @param newSmssContent
	 * @param currentSmssProperties
	 * @param secretStoreValues
	 * @return
	 */
	public static String unconcealSmssSensitiveInfo(String newSmssContent, Properties currentSmssProperties,
			Map<String, Object> secretStoreValues) {
		Properties newProperties = Utility.loadPropertiesString(newSmssContent);
		if (newProperties == null) {
			throw new IllegalArgumentException("New SMSS content is not a valid properties file format");
		}
		CaseInsensitiveProperties allUpperProps = new CaseInsensitiveProperties(newProperties);

		boolean requireProcessing = false;
		for (String key : SmssUtilities.SENSITIVE_KEYWORDS) {
			if (allUpperProps.containsKey(key) && allUpperProps.get(key).equals(Constants.SENSITIVE_INFO_MASK)) {
				requireProcessing = true;
				break;
			}
		}

		if (!requireProcessing) {
			return newSmssContent;
		}

		// okay, we found a key that is all sensitive info
		// lets fix it

		CaseInsensitiveProperties allUpperCurrentSmss = new CaseInsensitiveProperties(currentSmssProperties);
		// add any secrets that might be there
		if (secretStoreValues != null) {
			allUpperCurrentSmss.putAll(secretStoreValues);
		}
		StringBuilder constructedSmssContent = new StringBuilder();
		String[] currentSmssLines = newSmssContent.split("\n");

		for (String curLine : currentSmssLines) {
			String curLineUpperMatch = curLine.toUpperCase();

			// loop through all the keys to find
			boolean found = false;
			for (String key : SmssUtilities.SENSITIVE_KEYWORDS) {
				if (curLineUpperMatch.startsWith(key + "\t") || curLineUpperMatch.startsWith(key + " ")
						|| curLineUpperMatch.startsWith(key + "=")) {
					// check if we are still the concealed value or not
					if (allUpperProps.get(key).equals(Constants.SENSITIVE_INFO_MASK)) {
						// write the key with the original value in the current smss file
						// value might be an empty string which could return a null
						Object value = "";
						if (allUpperCurrentSmss.get(key) != null) {
							value = allUpperCurrentSmss.get(key);
						}
						constructedSmssContent.append(key).append("\t").append(value);
					} else {
						// the value has been changed
						constructedSmssContent.append(curLine);
					}
					found = true;
					break;
				}
			}
			if (!found) {
				constructedSmssContent.append(curLine);
			}
			constructedSmssContent.append("\n");
		}
		return constructedSmssContent.toString();
	}

}
