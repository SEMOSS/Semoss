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
package prerna.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.datastax.DataStaxGraphEngine;
import prerna.engine.impl.neo4j.Neo4jEngine;
import prerna.engine.impl.owl.AbstractOWLEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.engine.impl.r.RNativeEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.tinker.JanusEngine;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.engine.impl.tinker.TinkerEngine.TINKER_DRIVER;
import prerna.io.connector.secrets.ISecrets;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.om.MosfetFile;
import prerna.poi.main.FormUtility;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;
import prerna.util.gson.GsonUtility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.H2QueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SQLiteQueryUtil;

public final class UploadUtilities {

	private static final Logger classLogger = LogManager.getLogger(UploadUtilities.class);

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public static final String INSIGHT_USAGE_STATS_INSIGHT_NAME = "View insight usage stats";
	public static final String INSIGHT_USAGE_STATS_LAYOUT = "Grid";

	public static final String EXPLORE_INSIGHT_INSIGHT_NAME = "Explore an instance of a selected node type";
	public static final String EXPLORE_INSIGHT_LAYOUT = "Graph";

	public static final String GRID_DELTA_INSIGHT_NAME = "Grid Delta";
	public static final String GRID_DELTA_LAYOUT = "Grid";

	public static final String AUDIT_MODIFICATION_VIEW_INSIGHT_NAME = "What are the modifications made to specific column(s)?";
	public static final String AUDIT_MODIFICATION_VIEW_LAYOUT = "Bar";

	public static final String AUDIT_TIMELINE_INSIGHT_NAME = "What are the modifications made to the specific column(s) over time?";
	public static final String AUDIT_TIMELINE_LAYOUT = "Line";

	public static final String INSERT_FORM_LAYOUT = "form-builder";
	public static final String UPDATE_FORM_LAYOUT = "form-builder";

	public static final String INSIGHT_ID_KEY = "id";
	public static final String RECIPE_ID_KEY = "recipe";
	public static final String INSIGHT_NAME_KEY = "insightName";
	public static final String SCHEMA_NAME_KEY = "schemaName";

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private UploadUtilities() {

	}

	/**
	 * Used to store the temp smss in DIHelper such that we do not load via the file
	 * watcher
	 * 
	 * @param temporaryNewDatabaseId
	 * @param temporarySmssFile
	 */
	public static void addEngineToDIHelperToIgnoreEngineWatchers(String temporaryNewDatabaseId,
			String temporarySmssFile) {
		DIHelper.getInstance().setEngineProperty(temporaryNewDatabaseId + "_" + Constants.STORE, temporarySmssFile);
	}

	/**
	 * Used to update DIHelper To be used when making new engine
	 * 
	 * @param newEngineName
	 * @param engine
	 * @param smssFile
	 */
	public static void addEngineToDIHelper(String newEngineId, String newEngineName, IEngine engine, File smssFile) {
		DIHelper.getInstance().setEngineProperty(newEngineId + "_" + Constants.STORE, smssFile.getAbsolutePath());
		DIHelper.getInstance().setEngineProperty(newEngineId, engine);
		String engineIds = (String) DIHelper.getInstance().getEngineProperty(Constants.ENGINES);
		engineIds = engineIds + ";" + newEngineId;
		DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engineIds);
	}

	/**
	 * Used to update DIHelper when making new engine and an error occurs, or
	 * deleting engine, or syncing engine from cloud
	 * 
	 * @param engineIdToremove
	 */
	public static void removeEngineExcludingSMSSFromDIHelper(String engineIdToremove) {
		DIHelper.getInstance().removeEngineProperty(engineIdToremove);
		String engineIds = (String) DIHelper.getInstance().getEngineProperty(Constants.ENGINES);
		engineIds = engineIds.replace(";" + engineIdToremove + ";", ";");
		engineIds = engineIds.replace(";" + engineIdToremove, "");
		engineIds = engineIds.replace(engineIdToremove + ";", "");
		DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engineIds);
	}

	/**
	 * Used to update DIHelper when making new engine and an error occurs or
	 * deleting engine
	 * 
	 * @param engineIdToRemove
	 */
	public static void removeEngineFromDIHelper(String engineIdToRemove) {
		removeEngineExcludingSMSSFromDIHelper(engineIdToRemove);
		DIHelper.getInstance().removeEngineProperty(engineIdToRemove + "_" + Constants.STORE);
	}

	/**
	 * Used to update DIHelper when making new project and an error occurs or
	 * deleting an engine
	 * 
	 * @param projectIdToRemove
	 */
	public static void removeProjectExcludingSMSSFromDIHelper(String projectIdToRemove) {
		DIHelper.getInstance().removeProjectProperty(projectIdToRemove);
		String projectIds = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
		projectIds = projectIds.replace(";" + projectIdToRemove + ";", ";");
		projectIds = projectIds.replace(";" + projectIdToRemove, "");
		projectIds = projectIds.replace(projectIdToRemove + ";", "");
		DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projectIds);
	}

	/**
	 * Used to update DIHelper when making new project and an error occurs or
	 * deleting an engine
	 * 
	 * @param projectIdToRemove
	 */
	public static void removeProjectFromDIHelper(String projectIdToRemove) {
		DIHelper.getInstance().removeProjectProperty(projectIdToRemove + "_" + Constants.STORE);
		DIHelper.getInstance().removeProjectProperty(projectIdToRemove);
		String projectIds = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
		projectIds = projectIds.replace(";" + projectIdToRemove + ";", ";");
		projectIds = projectIds.replace(";" + projectIdToRemove, "");
		projectIds = projectIds.replace(projectIdToRemove + ";", "");
		DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projectIds);
	}

	/**
	 * Delete all the corresponding files that are generated from the upload the
	 * failed
	 * 
	 * @param engine
	 * @param storageId
	 * @param tempSmss
	 * @param smssFile
	 * @param specificEngineFolder
	 */
	public static void cleanUpCreateNewError(IEngine engine, String engineId, File tempSmss, File smssFile,
			File specificEngineFolder) {
		try {
			// close the engine so we can delete it
			if (engine != null) {
				engine.close();
			}

			// delete the .temp file
			if (tempSmss != null && tempSmss.exists()) {
				FileUtils.forceDelete(tempSmss);
			}
			// delete the .smss file
			if (smssFile != null && smssFile.exists()) {
				FileUtils.forceDelete(smssFile);
			}
			if (specificEngineFolder != null && specificEngineFolder.exists()) {
				FileUtils.forceDelete(specificEngineFolder);
			}

			UploadUtilities.removeEngineFromDIHelper(engineId);

			// remove from secret store
			ISecrets secretStore = SecretsFactory.getSecretConnector();
			if (secretStore != null) {
				secretStore.deleteEngineSecrets(engine.getCatalogType(), engine.getEngineId(), engine.getEngineName());
			}
		} catch (Exception e) {
			classLogger.error("Error during engine creation cleanup for engine id {}: {}", engineId, e.getMessage(), e);
		}
	}

	/**
	 * Update local master
	 * 
	 * @param databaseId
	 * @throws Exception
	 */
	public static void updateMetadata(String databaseId, User user) throws Exception {
		Utility.synchronizeEngineMetadata(databaseId);
		SecurityEngineUtils.addEngine(databaseId, false, user);
	}

	/**
	 * Validate the engine name Does validation that: 1) The input is not null/empty
	 * 2) That the database folder doesn't exist in the file directory
	 * 
	 * @param engineName
	 * @throws IOException
	 */
	public static void validateEngine(IEngine.CATALOG_TYPE engineType, User user, String engineName, String engineId)
			throws IOException {
		if (engineName == null || engineName.isEmpty()) {
			throw new IllegalArgumentException("Need to provide a name for the database");
		}

		// need to make sure engine folder doesn't already exist
		String engineLocation = EngineUtility.getSpecificEngineBaseFolder(engineType, engineId, engineName);
		File engineFolder = new File(engineLocation);
		if (engineFolder.exists()) {
			throw new IOException("Engine folder already contains a directory with the same name. "
					+ "Please delete the existing engine folder or provide a unique database name");
		}
	}

	/**
	 * Generate the engine folder and return the folder
	 * 
	 * @param engineType
	 * @param engineId
	 * @param engineName
	 * @return
	 */
	public static File generateSpecificEngineFolder(IEngine.CATALOG_TYPE engineType, String engineId,
			String engineName) {
		String specificEngineLocation = EngineUtility.getSpecificEngineBaseFolder(engineType, engineId, engineName);
		File specificEngineF = new File(specificEngineLocation);
		specificEngineF.mkdirs();
		return specificEngineF;
	}

	/**
	 * Generate the engine assets folder and return the folder
	 * 
	 * @param engineType
	 * @param engineId
	 * @param engineName
	 * @return
	 */
	public static File generateSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE engineType, String engineId,
			String engineName) {
		String specificEngineLocation = EngineUtility.getSpecificEngineAssetsFolder(engineType, engineId, engineName);
		File specificEngineF = new File(specificEngineLocation);
		specificEngineF.mkdirs();
		return specificEngineF;
	}

	/**
	 * Generate an empty OWL file based on the database name
	 * 
	 * @param databaseName
	 * @return
	 */
	public static File generateOwlFile(IEngine.CATALOG_TYPE engineType, String engineId, String engineName) {
		String owlLocation = EngineUtility.getSpecificEngineAssetsFolder(engineType, engineId, engineName) + "/";
		if (engineName != null) {
			owlLocation += engineName;
		} else {
			owlLocation += engineId;
		}
		owlLocation += "_OWL.OWL";

		return generateEmptyRDFXMLFile(owlLocation);
	}

	/**
	 * 
	 * @param owlLocation
	 * @return
	 */
	public static File generateEmptyRDFXMLFile(String owlLocation) {
		File owlFile = new File(owlLocation);
		if (!owlFile.exists()) {
			try {
				// check if the parent folder is there
				if (!owlFile.getParentFile().exists()) {
					owlFile.getParentFile().mkdirs();
				}
				owlFile.createNewFile();
			} catch (IOException e) {
				classLogger.error("Failed to create empty OWL file at {}: {}", owlLocation, e.getMessage(), e);
			}
		}
		try (FileWriter writer = new FileWriter(owlFile); BufferedWriter bufferedWriter = new BufferedWriter(writer);) {
			bufferedWriter.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
			bufferedWriter.write("\n");
			bufferedWriter.write("<rdf:RDF");
			bufferedWriter.write("\n");
			bufferedWriter.write("\txmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"");
			bufferedWriter.write("\n");
			bufferedWriter.write("\txmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">");
			bufferedWriter.write("\n");
			bufferedWriter.write("</rdf:RDF>");
			bufferedWriter.flush();
		} catch (IOException e) {
			classLogger.error("Failed to write RDF/XML content to OWL file at {}: {}", owlLocation, e.getMessage(), e);
		}

		return owlFile;
	}

	/**
	 * 
	 * @param owlEngine
	 * @param nodeProps
	 * @param descriptions
	 * @param logicalNames
	 */
	public static void insertOwlMetadataToGraphicalEngine(WriteOWLEngine owlEngine, Map<String, List<String>> nodeProps,
			Map<String, String> descriptions, Map<String, List<String>> logicalNames) {
		// NOTE ::: We require the OWL to be loaded with the concepts and properties
		// to get the proper physical URLs

		Map<String, String> conceptHash = owlEngine.getConceptHash();
		Map<String, String> propHash = owlEngine.getPropHash();
		// take the node props
		// so we know what is a concept
		// and what is a property
		for (String table : nodeProps.keySet()) {
			// this is just to grab the concept
			String tablePhysicalUri = conceptHash.get(table);
			if (tablePhysicalUri == null) {
				System.err.println("Error with adding owl metadata on upload");
				continue;
			}

			// adding metadata to table physical uri
			if (descriptions != null && descriptions.containsKey(table)) {
				String desc = descriptions.get(table);
				owlEngine.addDescription(tablePhysicalUri, desc);
			}

			if (logicalNames != null && logicalNames.containsKey(table)) {
				owlEngine.addLogicalNames(tablePhysicalUri, logicalNames.get(table));
			}

			List<String> properties = nodeProps.get(table);
			if (!properties.isEmpty()) {
				for (int i = 0; i < properties.size(); i++) {
					String property = properties.get(i);
					String propertyPhysicaluri = propHash.get(table + "%" + property);
					if (propertyPhysicaluri == null) {
						System.err.println("Error with adding owl metadata on upload");
						continue;
					}

					// adding metadata to property physical uri
					if (descriptions != null && descriptions.containsKey(property)) {
						String desc = descriptions.get(property);
						owlEngine.addDescription(propertyPhysicaluri, desc);
					}

					if (logicalNames != null && logicalNames.containsKey(property)) {
						owlEngine.addLogicalNames(propertyPhysicaluri, logicalNames.get(property));
					}
				}
			}
		}
	}

	/**
	 * 
	 * @param owlEngine
	 * @param tableName
	 * @param headers
	 * @param descriptions
	 * @param logicalNames
	 */
	public static void insertFlatOwlMetadata(WriteOWLEngine owlEngine, String tableName, String[] headers,
			Map<String, String> descriptions, Map<String, List<String>> logicalNames) {
		// NOTE ::: We require the OWL to be loaded with the concepts and properties
		// to get the proper physical URLs

		Map<String, String> propHash = owlEngine.getPropHash();

		// we have already loaded everything into a single table
		// so we will grab all the properties for that table
		for (int i = 0; i < headers.length; i++) {
			String property = headers[i];
			String propertyPhysicaluri = propHash.get(tableName + "%" + property);
			if (propertyPhysicaluri == null) {
				System.err.println("Error with adding owl metadata on upload");
				continue;
			}

			// adding metadata to property physical uri
			if (descriptions != null && descriptions.containsKey(property)) {
				String desc = descriptions.get(property);
				owlEngine.addDescription(propertyPhysicaluri, desc);
			}

			if (logicalNames != null && logicalNames.containsKey(property)) {
				owlEngine.addLogicalNames(propertyPhysicaluri, logicalNames.get(property));
			}
		}
	}

	/*
	 * Below methods pertain to the smss file
	 */

	/**
	 * Create a temporary smss file for a rdbms engine
	 * 
	 * @param databaseId
	 * @param databaseName
	 * @param owlFile
	 * @param rdbmsType
	 * @param file
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	public static File createTemporaryFileBasedRdbmsSmss(String databaseId, String databaseName, File owlFile,
			RdbmsTypeEnum rdbmsType, String file) throws IOException {

		Map<String, Object> properties = new HashMap<>();
		properties.put(Constants.USERNAME, "sa");
		properties.put(Constants.PASSWORD, "");

		String connectionUrl = rdbmsType == RdbmsTypeEnum.H2_DB ? H2QueryUtil.BASE_H2_FILE_CONNECTION
				: SQLiteQueryUtil.BASE_SQLITE_FILE_CONNECTION;
		if (connectionUrl == null) {
			throw new IllegalArgumentException("Unsupported rdbms type " + rdbmsType.getLabel());
		}
		return createTemporaryExternalRdbmsSmss(databaseId, databaseName, owlFile, RDBMSNativeEngine.class.getName(),
				rdbmsType, connectionUrl, properties, Map.of());
	}

	/**
	 * Create a temporary smss file for a tinker database
	 * 
	 * @param databaseId
	 * @param databaseName
	 * @param owlFile
	 * @param tinkerDriverType
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryTinkerSmss(String databaseId, String databaseName, File owlFile,
			TINKER_DRIVER tinkerDriverType) throws IOException {

		ISecrets secretStore = SecretsFactory.getSecretConnector();

		String dbTempSmssLoc = getEngineTempSmssLoc(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File dbTempSmss = new File(dbTempSmssLoc);
		if (dbTempSmss.exists()) {
			dbTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		// also write the base properties
		try (FileWriter writer = new FileWriter(dbTempSmssLoc);
				BufferedWriter bufferedWriter = new BufferedWriter(writer)) {
			writeDefaultDatabaseSettings(bufferedWriter, databaseId, databaseName, owlFile,
					TinkerEngine.class.getName(), newLine, tab);

			// tinker-specific properties
			// neo4j does not have an extension
			// basefolder/db/engine/engine
			String tinkerFilePath = " @BaseFolder@" + DIR_SEPARATOR + "db" + DIR_SEPARATOR + "@ENGINE@" + DIR_SEPARATOR
					+ databaseName;
			if (tinkerFilePath.contains("\\")) {
				tinkerFilePath = tinkerFilePath.replace("\\", "/");
			}

			if (secretStore != null) {
				Map<String, Object> properties = new HashMap<>();
				properties.put(Constants.ENGINE, databaseId);
				properties.put(Constants.ENGINE_ALIAS, databaseName);
				properties.put(Constants.ENGINE_TYPE, TinkerEngine.class.getName());
				properties.put(Constants.OWL, owlFile.getName());
				properties.put(Constants.TINKER_DRIVER, tinkerDriverType);
				// if neo4j, point to the folder
				if (tinkerDriverType == TINKER_DRIVER.NEO4J) {
					properties.put(Constants.TINKER_FILE, tinkerFilePath);
				} else {
					// basefolder/db/engine/engine.driverTypeExtension
					properties.put(Constants.TINKER_FILE, tinkerFilePath + "." + tinkerDriverType);
				}

				secretStore.writeEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName, properties);
			} else {
				bufferedWriter.write(newLine);
				writeSmssProperty(bufferedWriter, Constants.TINKER_DRIVER, tinkerDriverType);
				// if neo4j, point to the folder
				if (tinkerDriverType == TINKER_DRIVER.NEO4J) {
					writeSmssProperty(bufferedWriter, Constants.TINKER_FILE, tinkerFilePath);
				} else {
					// basefolder/db/engine/engine.driverTypeExtension
					writeSmssProperty(bufferedWriter, Constants.TINKER_FILE, tinkerFilePath + "." + tinkerDriverType);
				}
			}
			writeSmssProperty(bufferedWriter, IEngine.PIPELINE, "pipeline.json");
		} catch (IOException ex) {
			classLogger.error("Failed to write Tinker database smss file for database {}: {}", databaseName,
					ex.getMessage(), ex);
			throw new IOException("Could not generate database smss file");
		}

		return dbTempSmss;
	}

	/**
	 * Create a temporary smss file for a rdf database
	 * 
	 * @param thisEngine
	 * @param databaseId
	 * @param databaseName
	 * @param owlFile
	 * @param baseUri
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryRdfSmss(IEngine thisEngine, String databaseId, String databaseName, File owlFile,
			String baseUri) throws IOException {

		ISecrets secretStore = SecretsFactory.getSecretConnector();

		String dbTempSmssLoc = getEngineTempSmssLoc(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File dbTempSmss = new File(dbTempSmssLoc);
		if (dbTempSmss.exists()) {
			dbTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		try (FileWriter writer = new FileWriter(dbTempSmssLoc);
				BufferedWriter bufferedWriter = new BufferedWriter(writer)) {

			String dbClassName = thisEngine.getClass().getName();
			writeDefaultDatabaseSettings(bufferedWriter, databaseId, databaseName, owlFile, dbClassName, newLine, tab);

			if (secretStore != null) {
				Map<String, Object> properties = new HashMap<>();
				properties.put(Constants.ENGINE, databaseId);
				properties.put(Constants.ENGINE_ALIAS, databaseName);
				properties.put(Constants.ENGINE_TYPE, dbClassName);
				properties.put(Constants.OWL, owlFile.getName());

				properties.put(Constants.RDF_FILE_BASE_URI, baseUri);
				if (dbClassName.endsWith("BigDataEngine")) {
					bufferedWriter.write(newLine);
					// get additional RDF default properties
					String defaultDBPropName = Constants.DATABASE_FOLDER + DIR_SEPARATOR + "Default" + DIR_SEPARATOR
							+ "Default.properties";
					String jnlName = Constants.DATABASE_FOLDER + DIR_SEPARATOR + SmssUtilities.ENGINE_REPLACEMENT
							+ DIR_SEPARATOR + databaseName + ".jnl";
					jnlName = jnlName.replace('\\', '/'); // Needed as prop file cannot contain single back slash
					String rdfDefaultProps = Utility.getBaseFolder() + DIR_SEPARATOR + defaultDBPropName;

					try (FileReader fileRead = new FileReader(rdfDefaultProps);
							BufferedReader bufferedReader = new BufferedReader(fileRead)) {
						String currentLine;
						while ((currentLine = bufferedReader.readLine()) != null) {
							if (currentLine.contains("@FileName@")) {
								currentLine = currentLine.replace("@FileName@", jnlName);
							}
							bufferedWriter.write(currentLine + newLine);
						}
					}
				} else {
					properties.put(Constants.RDF_FILE_NAME, databaseName + ".xml");
					properties.put(Constants.RDF_FILE_TYPE, "RDF/XML");
				}

				secretStore.writeEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName, properties);
			} else {
				bufferedWriter.write(newLine);
				writeSmssProperty(bufferedWriter, Constants.RDF_FILE_BASE_URI, baseUri);

				if (dbClassName.endsWith("BigDataEngine")) {
					// get additional RDF default properties
					String defaultDBPropName = Constants.DATABASE_FOLDER + DIR_SEPARATOR + "Default" + DIR_SEPARATOR
							+ "Default.properties";
					String jnlName = Constants.DATABASE_FOLDER + DIR_SEPARATOR + SmssUtilities.ENGINE_REPLACEMENT
							+ DIR_SEPARATOR + databaseName + ".jnl";
					jnlName = jnlName.replace('\\', '/'); // Needed as prop file cannot contain single back slash
					String rdfDefaultProps = Utility.getBaseFolder() + DIR_SEPARATOR + defaultDBPropName;

					try (FileReader fileRead = new FileReader(rdfDefaultProps);
							BufferedReader bufferedReader = new BufferedReader(fileRead)) {
						String currentLine;
						while ((currentLine = bufferedReader.readLine()) != null) {
							if (currentLine.contains("@FileName@")) {
								currentLine = currentLine.replace("@FileName@", jnlName);
							}
							bufferedWriter.write(currentLine + newLine);
						}
					}
				} else {
					writeSmssProperty(bufferedWriter, Constants.RDF_FILE_NAME, databaseName + ".xml");
					writeSmssProperty(bufferedWriter, Constants.RDF_FILE_TYPE, "RDF/XML");
				}
			}
			writeSmssProperty(bufferedWriter, IEngine.PIPELINE, "pipeline.json");
		} catch (IOException e) {
			classLogger.error("Failed to write RDF database smss file for database {}: {}", databaseName,
					e.getMessage(), e);
			throw new IOException("Could not generate temporary smss file for database");
		}

		return dbTempSmss;
	}

	/**
	 * Generate a janus database smss
	 * 
	 * @param databaseId
	 * @param databaseName
	 * @param owlFile
	 * @param janusConfPath
	 * @param typeMap
	 * @param nameMap
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryJanusGraphSmss(String databaseId, String databaseName, File owlFile,
			String janusConfPath, Map<String, String> typeMap, Map<String, String> nameMap, boolean useLabel)
			throws IOException {

		ISecrets secretStore = SecretsFactory.getSecretConnector();

		String dbTempSmssLoc = getEngineTempSmssLoc(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File dbTempSmss = new File(dbTempSmssLoc);
		if (dbTempSmss.exists()) {
			dbTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		// also write the base properties
		try (FileWriter writer = new FileWriter(dbTempSmssLoc);
				BufferedWriter bufferedWriter = new BufferedWriter(writer)) {
			writeDefaultDatabaseSettings(bufferedWriter, databaseId, databaseName, owlFile, JanusEngine.class.getName(),
					newLine, tab);

			// janus conf file location
			// we will want to parameterize this
			File f = new File(janusConfPath);
			String fileBasePath = f.getParent();
			janusConfPath = janusConfPath.replace(fileBasePath,
					"@BaseFolder@" + DIR_SEPARATOR + Constants.DATABASE_FOLDER + DIR_SEPARATOR + "@ENGINE@");

			if (janusConfPath.contains("\\")) {
				janusConfPath = janusConfPath.replace("\\", "/");
			}

			if (secretStore != null) {
				Map<String, Object> properties = new HashMap<>();
				properties.put(Constants.ENGINE, databaseId);
				properties.put(Constants.ENGINE_ALIAS, databaseName);
				properties.put(Constants.ENGINE_TYPE, JanusEngine.class.getName());
				properties.put(Constants.OWL, owlFile.getName());

				properties.put(Constants.JANUS_CONF, janusConfPath);
				if (useLabel) {
					properties.put(Constants.TINKER_USE_LABEL, useLabel);
				} else {
					properties.put(Constants.TYPE_MAP, GSON.toJson(typeMap));
				}
				properties.put(Constants.NAME_MAP, GSON.toJson(nameMap));

				secretStore.writeEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName, properties);
			} else {
				bufferedWriter.write(newLine);
				writeSmssProperty(bufferedWriter, Constants.JANUS_CONF, janusConfPath);
				// type map
				// if we use the label we do not need the type map
				if (useLabel) {
					writeSmssProperty(bufferedWriter, Constants.TINKER_USE_LABEL, useLabel);
				} else {
					writeSmssProperty(bufferedWriter, Constants.TYPE_MAP, GSON.toJson(typeMap));
				}
				// name map
				writeSmssProperty(bufferedWriter, Constants.NAME_MAP, GSON.toJson(nameMap));
			}
			writeSmssProperty(bufferedWriter, IEngine.PIPELINE, "pipeline.json");
		} catch (IOException ex) {
			classLogger.error("Failed to write JanusGraph database smss file for database {}: {}", databaseName,
					ex.getMessage(), ex);
			throw new IOException("Could not generate database smss file");
		}

		return dbTempSmss;
	}

	/**
	 * Generate a tinker smss
	 * 
	 * @param databaseId
	 * @param databaseName
	 * @param owlFile
	 * @param tinkerFilePath
	 * @param typeMap
	 * @param nameMap
	 * @param tinkerDriverType
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryExternalTinkerSmss(String databaseId, String databaseName, File owlFile,
			String tinkerFilePath, Map<String, String> typeMap, Map<String, String> nameMap,
			TINKER_DRIVER tinkerDriverType, boolean useLabel) throws IOException {

		ISecrets secretStore = SecretsFactory.getSecretConnector();

		String dbTempSmssLoc = getEngineTempSmssLoc(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File dbTempSmss = new File(dbTempSmssLoc);
		if (dbTempSmss.exists()) {
			dbTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		// also write the base properties
		try (FileWriter writer = new FileWriter(dbTempSmssLoc);
				BufferedWriter bufferedWriter = new BufferedWriter(writer)) {
			writeDefaultDatabaseSettings(bufferedWriter, databaseId, databaseName, owlFile,
					TinkerEngine.class.getName(), newLine, tab);

			// tinker file location
			// we will want to parameterize this
			// if it is not a Neo4j as we do not move this onto the server yet
			if (tinkerDriverType != TinkerEngine.TINKER_DRIVER.NEO4J) {
				File f = new File(tinkerFilePath);
				String fileBasePath = f.getParent();
				tinkerFilePath = tinkerFilePath.replace(fileBasePath,
						"@BaseFolder@" + DIR_SEPARATOR + Constants.DATABASE_FOLDER + DIR_SEPARATOR + "@ENGINE@");
			}
			if (tinkerFilePath.contains("\\")) {
				tinkerFilePath = tinkerFilePath.replace("\\", "/");
			}

			if (secretStore != null) {
				Map<String, Object> properties = new HashMap<>();
				properties.put(Constants.ENGINE, databaseId);
				properties.put(Constants.ENGINE_ALIAS, databaseName);
				properties.put(Constants.ENGINE_TYPE, TinkerEngine.class.getName());
				properties.put(Constants.OWL, owlFile.getName());

				properties.put(Constants.TINKER_FILE, tinkerFilePath);
				properties.put(Constants.TINKER_DRIVER, tinkerDriverType);
				if (useLabel) {
					properties.put(Constants.TINKER_USE_LABEL, useLabel);
				} else {
					properties.put(Constants.TYPE_MAP, GSON.toJson(typeMap));
				}
				properties.put(Constants.NAME_MAP, GSON.toJson(nameMap));

				secretStore.writeEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName, properties);
			} else {
				bufferedWriter.write(newLine);
				writeSmssProperty(bufferedWriter, Constants.TINKER_FILE, tinkerFilePath);
				// tinker driver
				writeSmssProperty(bufferedWriter, Constants.TINKER_DRIVER, tinkerDriverType);
				// type map
				// if we use the label we do not need the type map
				if (useLabel) {
					writeSmssProperty(bufferedWriter, Constants.TINKER_USE_LABEL, useLabel);
				} else {
					writeSmssProperty(bufferedWriter, Constants.TYPE_MAP, GSON.toJson(typeMap));
				}
				// name map
				writeSmssProperty(bufferedWriter, Constants.NAME_MAP, GSON.toJson(nameMap));
			}
			writeSmssProperty(bufferedWriter, IEngine.PIPELINE, "pipeline.json");
		} catch (IOException ex) {
			classLogger.error("Failed to write external Tinker database smss file for database {}: {}", databaseName,
					ex.getMessage(), ex);
			throw new IOException("Could not generate database smss file");
		}

		return dbTempSmss;
	}

	/**
	 * Generate a temporary datastax smss
	 * 
	 * @param databaseId
	 * @param databaseName
	 * @param owlFile
	 * @param host
	 * @param port
	 * @param username
	 * @param password
	 * @param graphname
	 * @param typeMap
	 * @param nameMap
	 * @param tinkerDriverType
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryDatastaxSmss(String databaseId, String databaseName, File owlFile, String host,
			String port, String username, String password, String graphName, Map<String, String> typeMap,
			Map<String, String> nameMap, boolean useLabel) throws IOException {

		ISecrets secretStore = SecretsFactory.getSecretConnector();

		String dbTempSmssLoc = getEngineTempSmssLoc(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File dbTempSmss = new File(dbTempSmssLoc);
		if (dbTempSmss.exists()) {
			dbTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		// also write the base properties
		try (FileWriter writer = new FileWriter(dbTempSmssLoc);
				BufferedWriter bufferedWriter = new BufferedWriter(writer)) {
			writeDefaultDatabaseSettings(bufferedWriter, databaseId, databaseName, owlFile,
					DataStaxGraphEngine.class.getName(), newLine, tab);

			if (secretStore != null) {
				Map<String, Object> properties = new HashMap<>();
				properties.put(Constants.ENGINE, databaseId);
				properties.put(Constants.ENGINE_ALIAS, databaseName);
				properties.put(Constants.ENGINE_TYPE, TinkerEngine.class.getName());
				properties.put(Constants.OWL, owlFile.getName());

				properties.put("HOST", host);
				properties.put(Constants.PORT, port);
				if (username != null) {
					properties.put(Constants.USERNAME, username);
				}
				if (password != null) {
					properties.put(Constants.PASSWORD, password);
				}
				properties.put("GRAPH_NAME", graphName);
				if (useLabel) {
					properties.put(Constants.TINKER_USE_LABEL, useLabel);
				} else {
					properties.put(Constants.TYPE_MAP, GSON.toJson(typeMap));
				}
				properties.put(Constants.NAME_MAP, GSON.toJson(nameMap));

				secretStore.writeEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName, properties);
			} else {
				bufferedWriter.write(newLine);
				// host + port
				writeSmssProperty(bufferedWriter, "HOST", host);
				writeSmssProperty(bufferedWriter, Constants.PORT, port);
				if (username != null) {
					writeSmssProperty(bufferedWriter, Constants.USERNAME, username);
				}
				if (password != null) {
					writeSmssProperty(bufferedWriter, Constants.PASSWORD, password);
				}
				writeSmssProperty(bufferedWriter, "GRAPH_NAME", graphName);

				// type map
				if (useLabel) {
					writeSmssProperty(bufferedWriter, Constants.TINKER_USE_LABEL, useLabel);
				} else {
					writeSmssProperty(bufferedWriter, Constants.TYPE_MAP, GSON.toJson(typeMap));
				}
				// name map
				writeSmssProperty(bufferedWriter, Constants.NAME_MAP, GSON.toJson(nameMap));
			}
			writeSmssProperty(bufferedWriter, IEngine.PIPELINE, "pipeline.json");
		} catch (IOException ex) {
			classLogger.error("Failed to write DataStax database smss file for database {}: {}", databaseName,
					ex.getMessage(), ex);
			throw new IOException("Could not generate database smss file");
		}

		return dbTempSmss;
	}

	/**
	 * Generate a neo4j smss
	 *
	 * @param databaseId
	 * @param databaseName
	 * @param owlFile
	 * @param connectionStringKey
	 * @param username
	 * @param password
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryExternalNeo4jSmss(String databaseId, String databaseName, File owlFile,
			String connectionStringKey, String username, String password, Map<String, String> typeMap,
			Map<String, String> nameMap, boolean useLabel) throws IOException {

		ISecrets secretStore = SecretsFactory.getSecretConnector();

		String dbTempNeo4jLoc = getEngineTempSmssLoc(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName);

		File dbTempSmss = new File(dbTempNeo4jLoc);
		if (dbTempSmss.exists()) {
			dbTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		// also write the base properties
		try (FileWriter writer = new FileWriter(dbTempNeo4jLoc);
				BufferedWriter bufferedWriter = new BufferedWriter(writer)) {
			writeDefaultDatabaseSettings(bufferedWriter, databaseId, databaseName, owlFile, Neo4jEngine.class.getName(),
					newLine, tab);

			if (secretStore != null) {
				Map<String, Object> properties = new HashMap<>();
				properties.put(Constants.ENGINE, databaseId);
				properties.put(Constants.ENGINE_ALIAS, databaseName);
				properties.put(Constants.ENGINE_TYPE, TinkerEngine.class.getName());
				properties.put(Constants.OWL, owlFile.getName());

				properties.put(Constants.CONNECTION_URL, connectionStringKey);
				properties.put(Constants.USERNAME, username);
				properties.put(Constants.PASSWORD, password);
				if (useLabel) {
					properties.put(Constants.TINKER_USE_LABEL, useLabel);
				} else {
					properties.put(Constants.TYPE_MAP, GSON.toJson(typeMap));
				}
				properties.put(Constants.NAME_MAP, GSON.toJson(nameMap));

				secretStore.writeEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName, properties);
			} else {
				bufferedWriter.write(newLine);
				// neo4j external properties
				writeSmssProperty(bufferedWriter, Constants.CONNECTION_URL, connectionStringKey);
				writeSmssProperty(bufferedWriter, Constants.USERNAME, username);
				writeSmssProperty(bufferedWriter, Constants.PASSWORD, password);
				// type map
				if (useLabel) {
					writeSmssProperty(bufferedWriter, Constants.TINKER_USE_LABEL, useLabel);
				} else {
					writeSmssProperty(bufferedWriter, Constants.TYPE_MAP, GSON.toJson(typeMap));
				}
				// name map
				writeSmssProperty(bufferedWriter, Constants.NAME_MAP, GSON.toJson(nameMap));
			}
			writeSmssProperty(bufferedWriter, IEngine.PIPELINE, "pipeline.json");
		} catch (IOException ex) {
			classLogger.error("Failed to write external Neo4j database smss file for database {}: {}", databaseName,
					ex.getMessage(), ex);
			throw new IOException("Could not generate database smss file");
		}

		return dbTempSmss;
	}

	/**
	 * Generate a neo4j smss
	 *
	 * @param databaseId
	 * @param databaseName
	 * @param owlFile
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryEmbeddedNeo4jSmss(String databaseId, String databaseName, File owlFile,
			String filePath, Map<String, String> typeMap, Map<String, String> nameMap, boolean useLabel)
			throws IOException {

		ISecrets secretStore = SecretsFactory.getSecretConnector();

		String dbTempNeo4jLoc = getEngineTempSmssLoc(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File dbTempSmss = new File(dbTempNeo4jLoc);
		if (dbTempSmss.exists()) {
			dbTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		// also write the base properties
		try (FileWriter writer = new FileWriter(dbTempNeo4jLoc);
				BufferedWriter bufferedWriter = new BufferedWriter(writer)) {
			writeDefaultDatabaseSettings(bufferedWriter, databaseId, databaseName, owlFile, Neo4jEngine.class.getName(),
					newLine, tab);

			if (secretStore != null) {
				Map<String, Object> properties = new HashMap<>();
				properties.put(Constants.ENGINE, databaseId);
				properties.put(Constants.ENGINE_ALIAS, databaseName);
				properties.put(Constants.ENGINE_TYPE, TinkerEngine.class.getName());
				properties.put(Constants.OWL, owlFile.getName());

				properties.put(Constants.NEO4J_FILE, filePath);
				if (useLabel) {
					properties.put(Constants.TINKER_USE_LABEL, useLabel);
				} else {
					properties.put(Constants.TYPE_MAP, GSON.toJson(typeMap));
				}
				properties.put(Constants.NAME_MAP, GSON.toJson(nameMap));

				secretStore.writeEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName, properties);
			} else {
				bufferedWriter.write(newLine);
				// neo4j external properties
				writeSmssProperty(bufferedWriter, Constants.NEO4J_FILE, filePath);
				if (useLabel) {
					writeSmssProperty(bufferedWriter, Constants.TINKER_USE_LABEL, useLabel);
				} else {
					writeSmssProperty(bufferedWriter, Constants.TYPE_MAP, GSON.toJson(typeMap));
				}
				// name map
				writeSmssProperty(bufferedWriter, Constants.NAME_MAP, GSON.toJson(nameMap));
			}
			writeSmssProperty(bufferedWriter, IEngine.PIPELINE, "pipeline.json");
		} catch (IOException ex) {
			classLogger.error("Failed to write embedded Neo4j database smss file for database {}: {}", databaseName,
					ex.getMessage(), ex);
			throw new IOException("Could not generate database smss file");
		}

		return dbTempSmss;
	}

	/**
	 * Create a temporary smss file for an external rdbms database
	 * 
	 * @param databaseId
	 * @param databaseName
	 * @param owlFile
	 * @param dbClassName
	 * @param dbType
	 * @param connectionUrl
	 * @param username
	 * @param password
	 * @param jdbcPropertiesMap
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	public static File createTemporaryExternalRdbmsSmss(String databaseId, String databaseName, File owlFile,
			String dbClassName, RdbmsTypeEnum dbType, String connectionUrl, Map<String, Object> connectionDetails,
			Map<String, Object> jdbcPropertiesMap) throws IOException {

		ISecrets secretStore = SecretsFactory.getSecretConnector();

		String dbTempSmssLoc = getEngineTempSmssLoc(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File dbTempSmss = new File(dbTempSmssLoc);
		if (dbTempSmss.exists()) {
			dbTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		try (FileWriter writer = new FileWriter(dbTempSmss);
				BufferedWriter bufferedWriter = new BufferedWriter(writer)) {
			writeDefaultDatabaseSettings(bufferedWriter, databaseId, databaseName, owlFile, dbClassName, newLine, tab);
			bufferedWriter.write(newLine);
			// write the rdbms type
			writeSmssProperty(bufferedWriter, Constants.RDBMS_TYPE, dbType.getLabel());

			// we write the url at the end
			String host = (String) connectionDetails.get(AbstractSqlQueryUtil.HOSTNAME);
			if (host != null && !host.isEmpty()) {
				File f = new File(host);
				if (f.exists()) {
					connectionUrl = AbstractSqlQueryUtil.parameterizeFileBasedConnectionUrl(connectionUrl,
							f.getParent());
				}
			}

			if (secretStore != null) {
				Map<String, Object> properties = new HashMap<>();
				properties.put(Constants.ENGINE, databaseId);
				properties.put(Constants.ENGINE_ALIAS, databaseName);
				properties.put(Constants.ENGINE_TYPE, TinkerEngine.class.getName());
				properties.put(Constants.OWL, owlFile.getName());
				properties.put(Constants.RDBMS_TYPE, dbType.getLabel());
				properties.put(Constants.DRIVER, dbType.getDriver());

				for (String key : connectionDetails.keySet()) {
					properties.put(key.toUpperCase(), connectionDetails.get(key));
				}
				for (String key : jdbcPropertiesMap.keySet()) {
					if (jdbcPropertiesMap.get(key) == null || jdbcPropertiesMap.get(key).toString().isEmpty()) {
						continue;
					}
					properties.put(key, jdbcPropertiesMap.get(key));
				}

				secretStore.writeEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName, properties);
			} else {
				writeSmssProperty(bufferedWriter, Constants.DRIVER, dbType.getDriver());
				// just write everything to the smss file
				// but ignore the connection url until the end
				for (String key : connectionDetails.keySet()) {
					if (key.equals(AbstractSqlQueryUtil.CONNECTION_URL) || connectionDetails.get(key) == null
							|| (!key.equals(AbstractSqlQueryUtil.PASSWORD)
									&& connectionDetails.get(key).toString().isEmpty())) {
						continue;
					}
					writeSmssProperty(bufferedWriter, key.toUpperCase(), connectionDetails.get(key));
				}

				// connection url
				writeSmssProperty(bufferedWriter, Constants.CONNECTION_URL, connectionUrl);
				bufferedWriter.write(newLine);

				// write the additonal jdbc properties at the end of the properties file
				for (String key : jdbcPropertiesMap.keySet()) {
					if (jdbcPropertiesMap.get(key) == null || jdbcPropertiesMap.get(key).toString().isEmpty()) {
						continue;
					}
					writeSmssProperty(bufferedWriter, key, jdbcPropertiesMap.get(key));
				}
			}
			writeSmssProperty(bufferedWriter, IEngine.PIPELINE, "pipeline.json");
		} catch (IOException e) {
			classLogger.error("Failed to write external RDBMS database smss file for database {}: {}", databaseName,
					e.getMessage(), e);
			throw new IOException("Could not generate temporary smss file for database");
		}

		return dbTempSmss;
	}

	/**
	 *
	 * @param databaseId
	 * @param databaseName
	 * @param owlFile
	 * @param fileName
	 * @param newHeaders
	 * @param dataTypesMap
	 * @param additionalDataTypeMap
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryRSmss(String databaseId, String databaseName, File owlFile, String fileName,
			Map<String, String> newHeaders, Map<String, String> dataTypesMap, Map<String, String> additionalDataTypeMap)
			throws IOException {

		ISecrets secretStore = SecretsFactory.getSecretConnector();

		String dbTempSmssLoc = getEngineTempSmssLoc(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName);

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File dbTempSmss = new File(dbTempSmssLoc);
		if (dbTempSmss.exists()) {
			dbTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";

		try (FileWriter writer = new FileWriter(dbTempSmss);
				BufferedWriter bufferedWriter = new BufferedWriter(writer)) {
			String engineClassName = RNativeEngine.class.getName();
			writeDefaultDatabaseSettings(bufferedWriter, databaseId, databaseName, owlFile, engineClassName, newLine,
					tab);
			String dataFile = "db" + DIR_SEPARATOR + SmssUtilities.ENGINE_REPLACEMENT + DIR_SEPARATOR + fileName;

			if (secretStore != null) {
				Map<String, Object> properties = new HashMap<>();
				properties.put(Constants.ENGINE, databaseId);
				properties.put(Constants.ENGINE_ALIAS, databaseName);
				properties.put(Constants.ENGINE_TYPE, TinkerEngine.class.getName());
				properties.put(Constants.OWL, owlFile.getName());

				properties.put(AbstractDatabaseEngine.DATA_FILE, dataFile.replace('\\', '/'));
				// stringify maps
				if (newHeaders != null && !newHeaders.isEmpty()) {
					properties.put(Constants.NEW_HEADERS, GSON.toJson(newHeaders));
				}
				if (dataTypesMap != null && !dataTypesMap.isEmpty()) {
					properties.put(Constants.SMSS_DATA_TYPES, GSON.toJson(dataTypesMap));
				}
				if (additionalDataTypeMap != null && !additionalDataTypeMap.isEmpty()) {
					properties.put(Constants.ADDITIONAL_DATA_TYPES, GSON.toJson(additionalDataTypeMap));
				}

				secretStore.writeEngineSecrets(IEngine.CATALOG_TYPE.DATABASE, databaseId, databaseName, properties);
			} else {
				bufferedWriter.write(newLine);
				writeSmssProperty(bufferedWriter, AbstractDatabaseEngine.DATA_FILE, dataFile.replace('\\', '/'));
				// stringify maps
				if (newHeaders != null && !newHeaders.isEmpty()) {
					writeSmssProperty(bufferedWriter, Constants.NEW_HEADERS, GSON.toJson(newHeaders));
				}
				if (dataTypesMap != null && !dataTypesMap.isEmpty()) {
					writeSmssProperty(bufferedWriter, Constants.SMSS_DATA_TYPES, GSON.toJson(dataTypesMap));
				}
				if (additionalDataTypeMap != null && !additionalDataTypeMap.isEmpty()) {
					writeSmssProperty(bufferedWriter, Constants.ADDITIONAL_DATA_TYPES,
							GSON.toJson(additionalDataTypeMap));
				}
			}
			writeSmssProperty(bufferedWriter, IEngine.PIPELINE, "pipeline.json");
		} catch (IOException e) {
			classLogger.error("Failed to write R native engine smss file for database {}: {}", databaseName,
					e.getMessage(), e);
			throw new IOException("Could not generate temporary smss file for database");
		}

		return dbTempSmss;
	}

	/**
	 * Create a temporary smss file for storage engine
	 * 
	 * @param engineId
	 * @param engineName
	 * @param className
	 * @param properties
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryStorageSmss(String engineId, String engineName, String className,
			Map<String, Object> properties) throws IOException {
		return createTemporaryEngineSmss(IEngine.CATALOG_TYPE.STORAGE, engineId, engineName, className, properties);
	}

	/**
	 * Create a temporary smss file for model engine
	 * 
	 * @param engineId
	 * @param engineName
	 * @param className
	 * @param properties
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryModelSmss(String engineId, String engineName, String className,
			Map<String, Object> properties) throws IOException {
		return createTemporaryEngineSmss(IEngine.CATALOG_TYPE.MODEL, engineId, engineName, className, properties);
	}

	/**
	 * Create a temporary smss file for a vector engine
	 * 
	 * @param engineId
	 * @param engineName
	 * @param className
	 * @param properties
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryVectorDatabaseSmss(String engineId, String engineName, String className,
			Map<String, Object> properties) throws IOException {
		return createTemporaryEngineSmss(IEngine.CATALOG_TYPE.VECTOR, engineId, engineName, className, properties);
	}

	/**
	 * Create a temporary smss file for function engine
	 * 
	 * @param engineId
	 * @param engineName
	 * @param className
	 * @param properties
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryFunctionSmss(String engineId, String engineName, String className,
			Map<String, Object> properties) throws IOException {
		return createTemporaryEngineSmss(IEngine.CATALOG_TYPE.FUNCTION, engineId, engineName, className, properties);
	}

	/**
	 * Create a temporary smss file for guardrail engine
	 * 
	 * @param engineId
	 * @param engineName
	 * @param className
	 * @param properties
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryGuardrailSmss(String engineId, String engineName, String className,
			Map<String, Object> properties) throws IOException {
		return createTemporaryEngineSmss(IEngine.CATALOG_TYPE.GUARDRAIL, engineId, engineName, className, properties);
	}

	/**
	 * Create a temporary smss file for venv engine
	 * 
	 * @param engineId
	 * @param engineName
	 * @param className
	 * @param properties
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryVenvSmss(String engineId, String engineName, String className,
			Map<String, Object> properties) throws IOException {
		return createTemporaryEngineSmss(IEngine.CATALOG_TYPE.VENV, engineId, engineName, className, properties);
	}

	/**
	 * 
	 * @param engineType
	 * @param engineId
	 * @param engineName
	 * @param className
	 * @param properties
	 * @return
	 * @throws IOException
	 */
	public static File createTemporaryEngineSmss(IEngine.CATALOG_TYPE engineType, String engineId, String engineName,
			String className, Map<String, Object> properties) throws IOException {
		String engineTempSmssLoc = getEngineTempSmssLoc(engineType, engineId, engineName);

		ISecrets secretStore = SecretsFactory.getSecretConnector();

		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		// i am okay with deleting the .temp if it exists
		// we dont leave this around
		// and they should be deleted after loading
		// so ideally this would never happen...
		File engineTempSmss = new File(engineTempSmssLoc);
		if (engineTempSmss.exists()) {
			engineTempSmss.delete();
		}

		final String newLine = "\n";
		final String tab = "\t";
		boolean pipelineFromUI = false;
		boolean mcpFromUI = false;

		try (FileWriter writer = new FileWriter(engineTempSmss);
				BufferedWriter bufferedWriter = new BufferedWriter(writer)) {
			writeDefaultEngineSettings(bufferedWriter, engineId, engineName, className, newLine, tab);
			bufferedWriter.write(newLine);

			if (properties != null) {
				if (secretStore != null) {
					properties.put(Constants.ENGINE, engineId);
					properties.put(Constants.ENGINE_ALIAS, engineName);
					properties.put(Constants.ENGINE_TYPE, className);
					secretStore.writeEngineSecrets(engineType, engineId, engineName, properties);
				} else {
					for (String key : properties.keySet()) {
						if (key != null && key.equalsIgnoreCase(IEngine.PIPELINE)) {
							pipelineFromUI = true;
						}
						if (key != null && key.equalsIgnoreCase(Constants.MCP_ENABLED)) {
							mcpFromUI = true;
						}
						writeSmssProperty(bufferedWriter, key.toUpperCase(), properties.get(key));
					}

					// if UI is not sending, we set as default
					if (!pipelineFromUI) {
						writeSmssProperty(bufferedWriter, IEngine.PIPELINE, "pipeline.json");
					}
					if (!mcpFromUI) {
						writeSmssProperty(bufferedWriter, Constants.MCP_ENABLED, false);
					}
				}
			}
		} catch (IOException e) {
			classLogger.error("Failed to write engine smss file for engine {}: {}", engineName, e.getMessage(), e);
			throw new IOException("Could not generate temporary smss file for model");
		}

		return engineTempSmss;
	}

	static String escapeSmssPropertyValue(Object value) {
		if (value == null) {
			return "";
		}

		String stringValue = value.toString();
		StringBuilder escapedValue = new StringBuilder(stringValue.length());
		for (int i = 0; i < stringValue.length(); i++) {
			char character = stringValue.charAt(i);
			switch (character) {
			case '\\':
				escapedValue.append("\\\\");
				break;
			case '\t':
				escapedValue.append("\\t");
				break;
			case '\r':
				escapedValue.append("\\r");
				break;
			case '\n':
				escapedValue.append("\\n");
				break;
			case '\f':
				escapedValue.append("\\f");
				break;
			default:
				if (character < 0x20 || character > 0x7e) {
					escapedValue.append(String.format("\\u%04x", (int) character));
				} else {
					escapedValue.append(character);
				}
			}
		}
		return escapedValue.toString();
	}

	private static void writeSmssProperty(BufferedWriter bufferedWriter, String key, Object value) throws IOException {
		bufferedWriter.write(key + "\t" + escapeSmssPropertyValue(value) + "\n");
	}

	/**
	 * Get the engine temporary smss location
	 * 
	 * @param engineType
	 * @param engineId
	 * @param engineName
	 * @return
	 */
	private static String getEngineTempSmssLoc(IEngine.CATALOG_TYPE engineType, String engineId, String engineName) {
		return EngineUtility.getLocalEngineBaseDirectory(engineType) + "/"
				+ SmssUtilities.getUniqueName(engineName, engineId) + ".temp";
	}

	/**
	 * 
	 * @param bufferedWriter
	 * @param engineId
	 * @param engineName
	 * @param className
	 * @param newLine
	 * @param tab
	 * @throws IOException
	 */
	private static void writeDefaultEngineSettings(BufferedWriter bufferedWriter, String engineId, String engineName,
			String className, final String newLine, final String tab) throws IOException {
		bufferedWriter.write("#Base Properties" + newLine);
		writeSmssProperty(bufferedWriter, Constants.ENGINE, engineId);
		writeSmssProperty(bufferedWriter, Constants.ENGINE_ALIAS, engineName);
		writeSmssProperty(bufferedWriter, Constants.ENGINE_DISPLAY_NAME, engineName);
		writeSmssProperty(bufferedWriter, Constants.ENGINE_TYPE, className);
	}

	/**
	 * Writes the shared properties across majority of databases. This includes: 1)
	 * database Name 2) database Type 3) OWL file location
	 * 
	 * @param bufferedWriter
	 * @param databaseName
	 * @param owlFile
	 * @param className
	 * @param newLine
	 * @param tab
	 * @throws IOException
	 */
	private static void writeDefaultDatabaseSettings(BufferedWriter bufferedWriter, String databaseId,
			String databaseName, File owlFile, String className, final String newLine, final String tab)
			throws IOException {
		writeDefaultEngineSettings(bufferedWriter, databaseId, databaseName, className, newLine, tab);
		// write owl
		writeSmssProperty(bufferedWriter, Constants.OWL, owlFile.getName());
	}

	/*
	 * Below methods pertain to the insights database
	 */

	/**
	 * Get a unique name for this insight
	 * 
	 * @param databaseName
	 * @param baseName
	 * @return
	 */
	public static String getInsightName(String databaseOrProjectName, String baseName) {
		return databaseOrProjectName + " - " + baseName;
	}

	/**
	 * Get a unique name for this insight
	 * 
	 * @param databaseName
	 * @param baseName
	 * @return
	 */
	public static String getInsightName(String databaseOrProjectName, String tableName, String baseName) {
		return databaseOrProjectName + " - " + baseName;
	}

	/**
	 * Add explore an instance to the insights database
	 * 
	 * @param databaseId
	 * @param insightEngine
	 * @return String containing the new insight id
	 */
	public static Map<String, Object> addExploreInstanceInsight(String projectId, String projectName, String databaseId,
			String databaseName, IRDBMSEngine insightEngine) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		String exploreLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR
				+ "ExploreInstanceDefaultWidget.json";
		File exploreF = new File(exploreLoc);
		if (exploreF.exists()) {
			String newPixel = "META | AddPanel(0); META | Panel ( 0 ) | SetPanelView ( \"param\" , \"<encode> {\"json\":";
			try {
				newPixel += new String(Files.readAllBytes(exploreF.toPath())).replaceAll("\n|\r|\t", "")
						.replaceAll("\\s\\s+", "").replace("<<ENGINE>>", databaseId);
				newPixel += "} </encode>\" ) ;";
				List<String> pixelRecipeToSave = new ArrayList<>();
				pixelRecipeToSave.add(newPixel);
				String insightName = getInsightName(databaseName, EXPLORE_INSIGHT_INSIGHT_NAME);
				boolean global = true;
				boolean cacheable = Utility.getApplicationCacheInsight();
				int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
				boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
				String cacheCron = Utility.getApplicationCacheCron();
				ZonedDateTime cachedOn = null;
				String description = null;
				List<String> tags = null;
				String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(projectId, insightName);

				String insightId = admin.addInsight(insightName, EXPLORE_INSIGHT_LAYOUT, pixelRecipeToSave, global,
						cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
				// write recipe to file
				MosfetSyncHelper.makeMosfitFile(projectId, projectName, insightId, insightName, EXPLORE_INSIGHT_LAYOUT,
						pixelRecipeToSave, global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt,
						description, tags, schemaName);
				// add the git here
				String gitFolder = AssetUtility.getProjectVersionFolder(projectName, projectId);
				List<String> files = new ArrayList<>();
				files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
				GitRepoUtils.addSpecificFiles(gitFolder, files);
				GitRepoUtils.commitAddedFiles(gitFolder,
						GitUtils.getDateMessage("Saved " + insightName + " insight on"));

				Map<String, Object> retMap = new HashMap<>();
				retMap.put(INSIGHT_ID_KEY, insightId);
				retMap.put(RECIPE_ID_KEY, pixelRecipeToSave);
				retMap.put(INSIGHT_NAME_KEY, insightName);
				retMap.put(SCHEMA_NAME_KEY, schemaName);
				return retMap;
			} catch (Exception e) {
				classLogger.error("Failed to add explore instance insight for database {}: {}", databaseName,
						e.getMessage(), e);
			}
		}
		return null;
	}

	public static Map<String, Object> addInsightUsageStats(String projectId, String projectName,
			IRDBMSEngine insightEngine) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		List<String> pixelRecipeToSave = new ArrayList<>();
		pixelRecipeToSave.add("AddPanel(panel = [ 0 ] , sheet = [ \"0\" ] );");
		pixelRecipeToSave.add("Panel ( 0 ) | AddPanelConfig ( config = [ { \"type\" : \"golden\" } ] );");
		pixelRecipeToSave.add(
				"Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;");
		pixelRecipeToSave.add(
				"useageFrame = InsightUsageStatistics ( project = [ \"" + projectId + "\" ] , panel = [ \"0\" ] ) ;");
		pixelRecipeToSave.add(
				"Frame(useageFrame) | QueryAll() | AutoTaskOptions(panel = [ \"0\" ] , layout = [ \"GRID\" ] ) | Collect(-1);");
		pixelRecipeToSave.add(
				"SetInsightConfig({\"panels\":{\"0\":{\"config\":{\"type\":\"golden\",\"backgroundColor\":\"\",\"opacity\":100}}},\"sheets\":{\"0\":{\"golden\":{\"content\":[{\"type\":\"row\",\"content\":[{\"type\":\"stack\",\"activeItemIndex\":0,\"width\":100,\"content\":[{\"type\":\"component\",\"componentName\":\"panel\",\"componentState\":{\"panelId\":\"0\"}}]}]}]}}},\"sheet\":\"0\"});");
		try {
			boolean global = true;
			boolean cacheable = Utility.getApplicationCacheInsight();
			int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
			boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
			String cacheCron = Utility.getApplicationCacheCron();
			ZonedDateTime cachedOn = null;
			String description = null;
			List<String> tags = null;
			String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(projectId,
					INSIGHT_USAGE_STATS_INSIGHT_NAME);

			String insightId = admin.addInsight(INSIGHT_USAGE_STATS_INSIGHT_NAME, INSIGHT_USAGE_STATS_LAYOUT,
					pixelRecipeToSave, global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
			// write recipe to file
			MosfetSyncHelper.makeMosfitFile(projectId, projectName, insightId, INSIGHT_USAGE_STATS_INSIGHT_NAME,
					INSIGHT_USAGE_STATS_LAYOUT, pixelRecipeToSave, global, cacheable, cacheMinutes, cacheCron, cachedOn,
					cacheEncrypt, description, tags, schemaName);
			// add the git here
			String gitFolder = AssetUtility.getProjectVersionFolder(projectName, projectId);
			List<String> files = new ArrayList<>();
			files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
			GitRepoUtils.addSpecificFiles(gitFolder, files);
			GitRepoUtils.commitAddedFiles(gitFolder,
					GitUtils.getDateMessage("Saved " + INSIGHT_USAGE_STATS_INSIGHT_NAME + " insight on"));

			Map<String, Object> retMap = new HashMap<>();
			retMap.put(INSIGHT_ID_KEY, insightId);
			retMap.put(RECIPE_ID_KEY, pixelRecipeToSave);
			retMap.put(INSIGHT_NAME_KEY, INSIGHT_USAGE_STATS_INSIGHT_NAME);
			retMap.put(SCHEMA_NAME_KEY, schemaName);
			return retMap;
		} catch (Exception e) {
			classLogger.error("Failed to add insight usage stats insight for project {}: {}", projectName,
					e.getMessage(), e);
		}
		return null;
	}

	/**
	 * Add grid delta to the insights database
	 * 
	 * @param databaseId
	 * @param insightEngine
	 * @return String containing the new insight id
	 */
	public static Map<String, Object> addGridDeltaInsight(String projectId, String projectName, String databaseId,
			String databaseName, IRDBMSEngine insightEngine) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		List<String> pixelRecipeToSave = new ArrayList<>();
		pixelRecipeToSave
				.add("META | AddPanel(0); META | Panel(0) | SetPanelView(\"grid-delta\",\"<encode>{\"database\":\""
						+ databaseId + "\"}</encode>\");");
		String insightName = getInsightName(databaseName, GRID_DELTA_INSIGHT_NAME);
		// write recipe to file
		try {
			boolean global = true;
			boolean cacheable = Utility.getApplicationCacheInsight();
			int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
			boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
			String cacheCron = Utility.getApplicationCacheCron();
			ZonedDateTime cachedOn = null;
			String description = null;
			List<String> tags = null;
			String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(projectId, insightName);

			String insightId = admin.addInsight(insightName, GRID_DELTA_LAYOUT, pixelRecipeToSave, global, cacheable,
					cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
			MosfetSyncHelper.makeMosfitFile(projectId, projectName, insightId, insightName, GRID_DELTA_LAYOUT,
					pixelRecipeToSave, global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, description,
					tags, schemaName);
			// add the insight to git
			String gitFolder = AssetUtility.getProjectVersionFolder(projectName, projectId);
			List<String> files = new ArrayList<>();
			files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
			GitRepoUtils.addSpecificFiles(gitFolder, files);
			GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved " + insightName + " insight on"));

			Map<String, Object> retMap = new HashMap<>();
			retMap.put(INSIGHT_ID_KEY, insightId);
			retMap.put(RECIPE_ID_KEY, pixelRecipeToSave);
			retMap.put(INSIGHT_NAME_KEY, insightName);
			retMap.put(SCHEMA_NAME_KEY, schemaName);
			return retMap;
		} catch (Exception e) {
			classLogger.error("Failed to add grid delta insight for database {}: {}", databaseName, e.getMessage(), e);
		}
		return null;
	}

	/**
	 * Add the insight to check the modifications made to a column from audit db
	 * 
	 * @param databaseId
	 * @param insightEngine
	 */
	public static Map<String, Object> addAuditModificationView(String projectId, String projectName, String databaseId,
			String databaseName, IRDBMSEngine insightEngine) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		String jsonLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR
				+ "AuditModificationView.json";
		File jsonFile = new File(jsonLoc);
		if (jsonFile.exists()) {
			String newPixel = "META | AddPanel(0); META | Panel ( 0 ) | SetPanelView ( \"param\" , \"<encode> {\"json\":";
			try {
				newPixel += new String(Files.readAllBytes(jsonFile.toPath())).replaceAll("\n|\r|\t", "")
						.replace("<<ENGINE>>", databaseId)
						.replace("<<INSIGHT_NAME>>", AUDIT_MODIFICATION_VIEW_INSIGHT_NAME);
				newPixel += "} </encode>\" ) ;";
				List<String> pixelRecipeToSave = new ArrayList<>();
				pixelRecipeToSave.add(newPixel);
				String insightName = getInsightName(databaseName, AUDIT_MODIFICATION_VIEW_INSIGHT_NAME);
				boolean global = true;
				boolean cacheable = Utility.getApplicationCacheInsight();
				int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
				boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
				String cacheCron = Utility.getApplicationCacheCron();
				ZonedDateTime cachedOn = null;
				String description = null;
				List<String> tags = null;
				String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(projectId, insightName);

				String insightId = admin.addInsight(insightName, AUDIT_MODIFICATION_VIEW_LAYOUT, pixelRecipeToSave,
						global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
				// write recipe to file
				MosfetSyncHelper.makeMosfitFile(projectId, projectName, insightId, insightName,
						AUDIT_MODIFICATION_VIEW_LAYOUT, pixelRecipeToSave, global, cacheable, cacheMinutes, cacheCron,
						cachedOn, cacheEncrypt, description, tags, schemaName);
				// add the insight to git
				String gitFolder = AssetUtility.getProjectVersionFolder(projectName, projectId);
				List<String> files = new ArrayList<>();
				files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
				GitRepoUtils.addSpecificFiles(gitFolder, files);
				GitRepoUtils.commitAddedFiles(gitFolder,
						GitUtils.getDateMessage("Saved " + insightName + " insight on"));

				Map<String, Object> retMap = new HashMap<>();
				retMap.put(INSIGHT_ID_KEY, insightId);
				retMap.put(RECIPE_ID_KEY, pixelRecipeToSave);
				retMap.put(INSIGHT_NAME_KEY, insightName);
				retMap.put(SCHEMA_NAME_KEY, schemaName);
				return retMap;
			} catch (Exception e) {
				classLogger.error("Failed to add audit modification view insight for database {}: {}", databaseName,
						e.getMessage(), e);
			}
		}
		return null;
	}

	/**
	 * Add the insight to check the modifications made to a column over time from
	 * audit database
	 * 
	 * @param databaseId
	 * @param insightEngine
	 */
	public static Map<String, Object> addAuditTimelineView(String projectId, String projectName, String databaseId,
			String databaseName, IRDBMSEngine insightEngine) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		String jsonLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR
				+ "AuditTimelineView.json";
		File jsonFile = new File(jsonLoc);
		if (jsonFile.exists()) {
			String newPixel = "META | AddPanel(0); META | Panel ( 0 ) | SetPanelView ( \"param\" , \"<encode> {\"json\":";
			try {
				newPixel += new String(Files.readAllBytes(jsonFile.toPath())).replaceAll("\n|\r|\t", "")
						.replace("<<ENGINE>>", databaseId).replace("<<INSIGHT_NAME>>", AUDIT_TIMELINE_INSIGHT_NAME);
				newPixel += "} </encode>\" ) ;";
				List<String> pixelRecipeToSave = new ArrayList<>();
				pixelRecipeToSave.add(newPixel);
				String insightName = getInsightName(databaseName, AUDIT_TIMELINE_INSIGHT_NAME);
				boolean global = true;
				boolean cacheable = Utility.getApplicationCacheInsight();
				int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
				boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
				String cacheCron = Utility.getApplicationCacheCron();
				ZonedDateTime cachedOn = null;
				String description = null;
				List<String> tags = null;
				String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(projectId, insightName);

				String insightId = admin.addInsight(insightName, AUDIT_TIMELINE_LAYOUT, pixelRecipeToSave, global,
						cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
				// write recipe to file
				MosfetSyncHelper.makeMosfitFile(projectId, projectName, insightId, insightName, AUDIT_TIMELINE_LAYOUT,
						pixelRecipeToSave, global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt,
						description, tags, schemaName);
				// add the insight to git
				String gitFolder = AssetUtility.getProjectVersionFolder(projectName, projectId);
				List<String> files = new ArrayList<>();
				files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
				GitRepoUtils.addSpecificFiles(gitFolder, files);
				GitRepoUtils.commitAddedFiles(gitFolder,
						GitUtils.getDateMessage("Saved " + insightName + " insight on"));

				Map<String, Object> retMap = new HashMap<>();
				retMap.put(INSIGHT_ID_KEY, insightId);
				retMap.put(RECIPE_ID_KEY, pixelRecipeToSave);
				retMap.put(INSIGHT_NAME_KEY, insightName);
				retMap.put(SCHEMA_NAME_KEY, schemaName);
				return retMap;
			} catch (Exception e) {
				classLogger.error("Failed to add audit timeline view insight for database {}: {}", databaseName,
						e.getMessage(), e);
			}
		}
		return null;
	}

	/**
	 * Add insert form for csv
	 *
	 * @param projectId
	 * @param projectName
	 * @param databaseId
	 * @param databaseName
	 * @param insightEngine
	 * @param headers
	 * @return
	 */
	public static Map<String, Object> addInsertFormInsight(String projectId, String projectName, String databaseId,
			String databaseName, IRDBMSEngine insightEngine, String[] headers) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		Map<String, Map<String, SemossDataType>> metamodel = getExistingMetamodel(
				Utility.getDatabase(databaseId).getOWLEngineFactory().getReadOWL());
		// assuming single sheet
		String sheetName = metamodel.keySet().iterator().next();
		String insightName = getInsightFormSheetName(sheetName);
		Gson gson = GsonUtility.getDefaultGson();
		String newPixel = "META | AddPanel(0); META | Panel(0) | SetPanelView(\"" + INSERT_FORM_LAYOUT
				+ "\", \"<encode>{\"json\":" + gson.toJson(createInsertForm(databaseId, metamodel, headers))
				+ "}</encode>\");";
		List<String> pixelRecipeToSave = new ArrayList<>();
		pixelRecipeToSave.add(newPixel);
		try {
			boolean global = true;
			boolean cacheable = Utility.getApplicationCacheInsight();
			int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
			boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
			String cacheCron = Utility.getApplicationCacheCron();
			ZonedDateTime cachedOn = null;
			String description = null;
			List<String> tags = null;
			String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(projectId, insightName);

			String insightId = admin.addInsight(insightName, INSERT_FORM_LAYOUT, pixelRecipeToSave, global, cacheable,
					cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
			insightEngine.commit();
			// write recipe to file
			MosfetSyncHelper.makeMosfitFile(databaseId, databaseName, insightId, insightName, INSERT_FORM_LAYOUT,
					pixelRecipeToSave, global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, description,
					tags, schemaName);
			// add the insight to git
			String gitFolder = AssetUtility.getProjectVersionFolder(databaseName, databaseId);
			List<String> files = new ArrayList<>();
			files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
			GitRepoUtils.addSpecificFiles(gitFolder, files);
			GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved " + insightName + " insight on"));

			Map<String, Object> retMap = new HashMap<>();
			retMap.put(INSIGHT_ID_KEY, insightId);
			retMap.put(RECIPE_ID_KEY, pixelRecipeToSave);
			retMap.put(INSIGHT_NAME_KEY, insightName);
			retMap.put(SCHEMA_NAME_KEY, schemaName);
			return retMap;
		} catch (Exception e) {
			classLogger.error("Failed to add insert form insight for database {}: {}", databaseName, e.getMessage(), e);
		}

		return null;
	}

	/**
	 * Add insert form for csv
	 *
	 * @param projectId
	 * @param projectName
	 * @param databaseId
	 * @param databaseName
	 * @param insightEngine
	 * @return
	 */
	public static Map<String, Object> addInsertFormInsight(String projectId, String projectName, String databaseId,
			String databaseName, IRDBMSEngine insightEngine) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		Map<String, Map<String, SemossDataType>> metamodel = getExistingMetamodel(
				Utility.getDatabase(databaseId).getOWLEngineFactory().getReadOWL());
		// assuming single sheet
		String sheetName = metamodel.keySet().iterator().next();
		String insightName = getInsightFormSheetName(sheetName);
		String[] headers = new TreeSet<>(metamodel.get(sheetName).keySet()).toArray(new String[] {});
		Gson gson = GsonUtility.getDefaultGson();
		String newPixel = "META | AddPanel(0); META | Panel(0) | SetPanelView(\"" + INSERT_FORM_LAYOUT
				+ "\", \"<encode>{\"json\":" + gson.toJson(createInsertForm(databaseId, metamodel, headers))
				+ "}</encode>\");";
		List<String> pixelRecipeToSave = new ArrayList<>();
		pixelRecipeToSave.add(newPixel);
		try {
			boolean global = true;
			boolean cacheable = Utility.getApplicationCacheInsight();
			int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
			boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
			String cacheCron = Utility.getApplicationCacheCron();
			ZonedDateTime cachedOn = null;
			String description = null;
			List<String> tags = null;
			String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(projectId, insightName);

			String insightId = admin.addInsight(insightName, INSERT_FORM_LAYOUT, pixelRecipeToSave, global, cacheable,
					cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
			insightEngine.commit();
			// write recipe to file
			MosfetSyncHelper.makeMosfitFile(databaseId, databaseName, insightId, insightName, INSERT_FORM_LAYOUT,
					pixelRecipeToSave, global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, description,
					tags, schemaName);
			// add the insight to git
			String gitFolder = AssetUtility.getProjectVersionFolder(databaseName, databaseId);
			List<String> files = new ArrayList<>();
			files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
			GitRepoUtils.addSpecificFiles(gitFolder, files);
			GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved " + insightName + " insight on"));

			Map<String, Object> retMap = new HashMap<>();
			retMap.put(INSIGHT_ID_KEY, insightId);
			retMap.put(RECIPE_ID_KEY, pixelRecipeToSave);
			retMap.put(INSIGHT_NAME_KEY, insightName);
			retMap.put(SCHEMA_NAME_KEY, schemaName);
			return retMap;
		} catch (Exception e) {
			classLogger.error("Failed to add insert form insight (auto-headers) for database {}: {}", databaseName,
					e.getMessage(), e);
		}

		return null;
	}

	/**
	 * Add insert form for excel
	 *
	 * @param insightDatabase
	 * @param projectId
	 * @param projectName
	 * @param databaseId
	 * @param databaseName
	 * @param sheetName
	 * @param propMap
	 * @param headers
	 * @return
	 */
	public static Map<String, Object> addInsertFormInsight(IRDBMSEngine insightDatabase, String projectId,
			String projectName, String databaseId, String databaseName, String sheetName,
			Map<String, SemossDataType> propMap, String[] headers) {
		InsightAdministrator admin = new InsightAdministrator(insightDatabase);
		Map<String, Map<String, SemossDataType>> metamodel = new HashMap<>();
		metamodel.put(sheetName, propMap);
		// assuming single sheet
		String insightName = getInsightFormSheetName(sheetName);
		Gson gson = GsonUtility.getDefaultGson();
		String newPixel = "META | AddPanel(0); META | Panel(0) | SetPanelView(\"" + INSERT_FORM_LAYOUT
				+ "\", \"<encode>{\"json\":" + gson.toJson(createInsertForm(databaseId, metamodel, headers))
				+ "}</encode>\");";
		List<String> pixelRecipeToSave = new ArrayList<>();
		pixelRecipeToSave.add(newPixel);
		try {
			boolean global = true;
			boolean cacheable = Utility.getApplicationCacheInsight();
			int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
			boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
			String cacheCron = Utility.getApplicationCacheCron();
			ZonedDateTime cachedOn = null;
			String description = null;
			List<String> tags = null;
			String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(projectId, insightName);

			String insightId = admin.addInsight(insightName, INSERT_FORM_LAYOUT, pixelRecipeToSave, global, cacheable,
					cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
			insightDatabase.commit();
			// write recipe to file
			MosfetSyncHelper.makeMosfitFile(databaseId, databaseName, insightId, insightName, INSERT_FORM_LAYOUT,
					pixelRecipeToSave, global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, description,
					tags, schemaName);
			// add the insight to git
			String gitFolder = AssetUtility.getProjectVersionFolder(databaseName, databaseId);
			List<String> files = new ArrayList<>();
			files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
			GitRepoUtils.addSpecificFiles(gitFolder, files);
			GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved " + insightName + " insight on"));

			Map<String, Object> retMap = new HashMap<>();
			retMap.put(INSIGHT_ID_KEY, insightId);
			retMap.put(RECIPE_ID_KEY, pixelRecipeToSave);
			retMap.put(INSIGHT_NAME_KEY, insightName);
			retMap.put(SCHEMA_NAME_KEY, schemaName);
			return retMap;
		} catch (Exception e) {
			classLogger.error("Failed to add Excel insert form insight for sheet {} in database {}: {}", sheetName,
					databaseName, e.getMessage(), e);
		}

		return null;
	}

	/**
	 * Create Excel form insight using data validation map
	 *
	 * @param insightEngine
	 * @param projectId
	 * @param projectName
	 * @param databaseId
	 * @param databaseName
	 * @param sheetName
	 * @param widgetJson    - data validation map
	 * @return
	 */
	public static Map<String, Object> addInsertFormInsight(IRDBMSEngine insightEngine, String projectId,
			String projectName, String databaseId, String databaseName, String sheetName,
			Map<String, Object> widgetJson) {
		InsightAdministrator admin = new InsightAdministrator(insightEngine);
		String insightName = getInsightFormSheetName(sheetName);
		Gson gson = GsonUtility.getDefaultGson();
		String newPixel = "META | AddPanel(0); META | Panel(0) | SetPanelView(\"" + INSERT_FORM_LAYOUT
				+ "\", \"<encode>{\"json\":" + gson.toJson(widgetJson) + "}</encode>\");";
		List<String> pixelRecipeToSave = new ArrayList<>();
		pixelRecipeToSave.add(newPixel);
		try {
			boolean global = true;
			boolean cacheable = Utility.getApplicationCacheInsight();
			int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
			boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
			String cacheCron = Utility.getApplicationCacheCron();
			ZonedDateTime cachedOn = null;
			String description = null;
			List<String> tags = null;
			String schemaName = SecurityInsightUtils.makeInsightSchemaNameUnique(projectId, insightName);

			String insightId = admin.addInsight(insightName, INSERT_FORM_LAYOUT, pixelRecipeToSave, global, cacheable,
					cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
			insightEngine.commit();
			// write recipe to file
			MosfetSyncHelper.makeMosfitFile(databaseId, databaseName, insightId, insightName, INSERT_FORM_LAYOUT,
					pixelRecipeToSave, global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, description,
					tags, schemaName);
			// add the insight to git
			String gitFolder = AssetUtility.getProjectVersionFolder(databaseName, databaseId);
			List<String> files = new ArrayList<>();
			files.add(insightId + "/" + MosfetFile.RECIPE_FILE);
			GitRepoUtils.addSpecificFiles(gitFolder, files);
			GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Saved " + insightName + " insight on"));

			Map<String, Object> retMap = new HashMap<>();
			retMap.put(INSIGHT_ID_KEY, insightId);
			retMap.put(RECIPE_ID_KEY, pixelRecipeToSave);
			retMap.put(INSIGHT_NAME_KEY, insightName);
			retMap.put(SCHEMA_NAME_KEY, schemaName);
			return retMap;
		} catch (Exception e) {
			classLogger.error("Failed to add Excel form insight (widget JSON) for sheet {} in database {}: {}",
					sheetName, databaseName, e.getMessage(), e);
		}

		return null;
	}

	/**
	 * The name of the form insight
	 *
	 * @param sheetName
	 * @return
	 */
	public static String getInsightFormSheetName(String sheetName) {
		// sheetNames are inserted as tables all caps
		return "Insert Into " + sheetName.toUpperCase() + " Form";
	}

	/**
	 * Map of concept to propMap with database type
	 * 
	 * @param owl
	 * @return
	 */
	public static Map<String, Map<String, SemossDataType>> getExistingMetamodel(AbstractOWLEngine helper) {
		List<String> conceptsList = helper.getPhysicalConcepts();
		Map<String, Map<String, SemossDataType>> existingMetaModel = new HashMap<>();

		try {
			for (String conceptPhysicalUri : conceptsList) {
				// so grab the conceptual name
				String conceptName = helper.getPixelSelectorFromPhysicalUri(conceptPhysicalUri);
				// and grab its properties
				List<String> properties = helper.getPropertyUris4PhysicalUri(conceptPhysicalUri);

				Map<String, SemossDataType> propMap = new HashMap<>();
				for (String prop : properties) {
					// grab the conceptual name
					String propertyPixelName = helper.getPixelSelectorFromPhysicalUri(prop);
					String owlType = helper.getDataTypes(prop);
					SemossDataType type = null;
					if (owlType != null) {
						owlType = owlType.replace("TYPE:", "");
						type = SemossDataType.convertStringToDataType(owlType);
					} else {
						// something is weird that you have no type
						// lets assume you are a string
						type = SemossDataType.STRING;
					}
					// property conceptual uris are always /Column/Table
					String propertyConceptualName = propertyPixelName.split("__")[1];
					propMap.put(propertyConceptualName, type);
				}
				existingMetaModel.put(conceptName, propMap);
			}
		} catch (Exception e) {
			classLogger.warn("OWL is not formatted properly...");
			classLogger.error("Failed to read OWL metamodel structure: {}", e.getMessage(), e);
		}
		return existingMetaModel;
	}

	/**
	 * 
	 * @param databaseId
	 * @param existingMetamodel
	 * @param headers
	 * @return
	 */
	public static Map<String, Object> createInsertForm(String databaseId,
			Map<String, Map<String, SemossDataType>> existingMetamodel, String[] headers) {
		Map<String, Object> formMap = new HashMap<>();
		formMap.put("js", new ArrayList<>());
		formMap.put("css", new ArrayList<>());
		// assuming this is a flat table so there is only one concept
		String conceptualName = existingMetamodel.keySet().iterator().next();
		Map<String, SemossDataType> propMap = existingMetamodel.get(conceptualName);
		List<String> propertyList = new ArrayList<String>();
		// order params by header order
		for (String header : headers) {
			if (propMap.containsKey(header)) {
				propertyList.add(header);
			}
		}

		// create values and into strings for query
		StringBuilder intoString = new StringBuilder();
		StringBuilder valuesString = new StringBuilder();
		for (int i = 0; i < propertyList.size(); i++) {
			String property = propertyList.get(i);
			intoString.append(conceptualName + "__" + property);
			valuesString.append("(<" + property + ">)");
			if (i < propertyList.size() - 1) {
				intoString.append(",");
				valuesString.append(",");
			}
		}
		// create insert pixel map
		Map<String, Object> pixelMap = new HashMap<>();
		Map<String, Object> insertMap = new HashMap<>();
		insertMap.put("name", "Insert");
		insertMap.put("pixel", "Database(database=[\"" + databaseId + "\"]) | Insert (into=[" + intoString
				+ "], values=[" + valuesString + "]);");
		pixelMap.put("Insert", insertMap);

		formMap.put("pixel", pixelMap);

		StringBuilder htmlSb = new StringBuilder();
		Map<String, Object> dataMap = new HashMap<>();
		for (int i = 0; i < propertyList.size(); i++) {
			String property = propertyList.get(i);
			htmlSb.append(FormUtility.getTextComponent(property));
			SemossDataType propType = propMap.get(property);
			// build html based on input component
			if (propType == SemossDataType.DATE) {
				htmlSb.append(FormUtility.getDatePickerComponent(property));
			} else if (Utility.isNumericType(propType.toString())) {
				htmlSb.append(FormUtility.getNumberPickerComponent(property));
			} else if (propType == SemossDataType.STRING) {
				htmlSb.append(FormUtility.getTypeAheadComponent(property));
			}

			// build data property map for data binding
			Map<String, Object> propertyMap = new HashMap<>();
			propertyMap.put("defaultValue", "");
			propertyMap.put("options", new ArrayList<>());
			propertyMap.put("name", property);
			propertyMap.put("dependsOn", new ArrayList<>());
			propertyMap.put("required", true);
			propertyMap.put("autoPopulate", false);
			Map<String, Object> configMap = new HashMap<>();
			configMap.put("table", conceptualName);
			Map<String, Object> appMap = new HashMap<>();
			appMap.put("value", databaseId);
			configMap.put("app", appMap);
			propertyMap.put("config", configMap);
			propertyMap.put("pixel", "");

			// adding pixel data binding for non-numeric values
			if (propType == SemossDataType.STRING) {
				String pixel = "Database( database=[\"" + databaseId + "\"] )|" + "Select(" + conceptualName + "__"
						+ property + ").as([" + property + "])| Collect(-1);";
				propertyMap.put("pixel", pixel);
			} else if (Utility.isNumericType(propType.toString())) {
				propertyMap.put("defaultValue", "0");
			}
			dataMap.put(property, propertyMap);
		}
		htmlSb.append(FormUtility.getSubmitComponent("Insert"));
		formMap.put("html", htmlSb.toString());
		formMap.put("data", dataMap);
		return formMap;
	}

	public static Map<String, Object> createUpdateMap(String appId, String concept,
			Map<String, SemossDataType> propMap) {
		Map<String, Object> updateMap = new HashMap<>();
		updateMap.put("database", appId);
		updateMap.put("table", concept);
		// config map
		Map<String, Object> configMap = new HashMap<>();
		for (String property : propMap.keySet()) {

			Map<String, Object> configPropMap = new HashMap<>();

			SemossDataType type = propMap.get(property);
			boolean readOnly = false;
			if (property.equals(concept)) {
				// assume this is the auto generated column
				// users should not modify this
				readOnly = true;
			}
			configPropMap.put("read-only", readOnly);
			if (type == SemossDataType.DOUBLE) {
				ArrayList<String> validationList = new ArrayList<>();
				String regex = "^\\d+(\\.\\d*)?$";
				validationList.add(regex);
				configPropMap.put("validation", validationList);
			} else if (type == SemossDataType.INT) {
				ArrayList<String> validationList = new ArrayList<>();
				String regex = "^\\d*$";
				validationList.add(regex);
				configPropMap.put("validation", validationList);
			} else if (type == SemossDataType.STRING) {
//				configPropMap.put("selection-type", "database");
			} else if (type == SemossDataType.DATE) {
				// yyyy-mm-dd
				ArrayList<String> validationList = new ArrayList<>();
				String regex = "^\\d{4}-\\d{2}-\\d{2}$";
				validationList.add(regex);
				configPropMap.put("validation", validationList);
			}
			configMap.put(property, configPropMap);
		}
		updateMap.put("config", configMap);
		return updateMap;
	}

	/**
	 * Parse the file
	 * 
	 * @param filePath
	 * @param delimiter
	 * @param dataTypesMap
	 * @param newHeaders
	 * @return
	 */
	public static CSVFileHelper getHelper(final String filePath, final String delimiter,
			Map<String, String> dataTypesMap, Map<String, String> newHeaders) {
		CSVFileHelper csvHelper = new CSVFileHelper();
		csvHelper.setDelimiter(delimiter.charAt(0));
		csvHelper.parse(filePath);

		// if the user has cleaned any headers
		if (newHeaders != null && !newHeaders.isEmpty()) {
			csvHelper.modifyCleanedHeaders(newHeaders);
		}

		// specify the columns to use
		// default will include all
		if (dataTypesMap != null && !dataTypesMap.isEmpty()) {
			Set<String> headersToUse = new TreeSet<String>(dataTypesMap.keySet());
			csvHelper.parseColumns(headersToUse.toArray(new String[] {}));
		}
		return csvHelper;
	}

	/**
	 * Figure out the types and how to use them Will return an object[] Index 0 of
	 * the return is an array of the headers Index 1 of the return is an array of
	 * the types Index 2 of the return is an array of the additional type
	 * information The 3 arrays all match based on index
	 * 
	 * @param helper
	 * @param dataTypesMap
	 * @param additionalDataTypeMap
	 * @return
	 */
	public static Object[] getHeadersAndTypes(CSVFileHelper helper, Map<String, String> dataTypesMap,
			Map<String, String> additionalDataTypeMap) {
		String[] headers = helper.getHeaders();
		int numHeaders = headers.length;
		// we want types
		// and we want additional types
		SemossDataType[] types = new SemossDataType[numHeaders];
		String[] additionalTypes = new String[numHeaders];

		// get the types
		if (dataTypesMap == null || dataTypesMap.isEmpty()) {
			Map[] retMap = FileHelperUtil.generateDataTypeMapsFromPrediction(headers, helper.predictTypes());
			dataTypesMap = retMap[0];
			additionalDataTypeMap = retMap[1];
		}

		for (int i = 0; i < numHeaders; i++) {
			types[i] = SemossDataType.convertStringToDataType(dataTypesMap.get(headers[i]));
		}

		// get additional type information
		if (additionalDataTypeMap != null && !additionalDataTypeMap.isEmpty()) {
			for (int i = 0; i < numHeaders; i++) {
				additionalTypes[i] = additionalDataTypeMap.get(headers[i]);
			}
		}

		return new Object[] { headers, types, additionalTypes };
	}

	/**
	 * Save metamodel structure to json in database folder
	 * 
	 * @param databaseId
	 * @param databaseName
	 * @param csvFileName
	 * @param metamodel
	 * @return
	 */
	public static boolean createPropFile(String databaseId, String databaseName, String csvFilePath,
			Map<String, Object> metamodel) {
		String csvFileName = new File(csvFilePath).getName().replace(".csv", "");
		Date currDate = Calendar.getInstance().getTime();
		SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHHmmssZ");
		String dateName = sdf.format(currDate);
		String dbFolderPath = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.DATABASE, databaseId,
				databaseName);
		String metaModelFilePath = dbFolderPath + DIR_SEPARATOR + databaseName + "_" + csvFileName + "_" + dateName
				+ "_PROP.json";
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String json = gson.toJson(metamodel);
		// create file
		File f = new File(Utility.normalizePath(metaModelFilePath));
		try {
			f.getParentFile().mkdirs();
			// write json to file
			FileUtils.writeStringToFile(f, json, StandardCharsets.UTF_8);
		} catch (IOException e1) {
			classLogger.error("Failed to write metamodel prop file at {}: {}", metaModelFilePath, e1.getMessage(), e1);
			return false;
		}
		return true;
	}

	/**
	 * Return map for uploading a new engine
	 * 
	 * @param databaseId
	 * @return
	 */
	public static Map<String, Object> getEngineReturnData(User user, String engineId) {
		List<Map<String, Object>> baseInfo = SecurityEngineUtils.getUserEngineList(user, engineId, null);
		Map<String, Object> retMap = baseInfo.get(0);
		return retMap;
	}

	/**
	 * Return map for uploading a new project
	 * 
	 * @param projectId
	 * @return
	 */
	public static Map<String, Object> getProjectReturnData(User user, String projectId) {
		List<Map<String, Object>> baseInfo = SecurityProjectUtils.getUserProjectList(user, projectId);
		Map<String, Object> retMap = baseInfo.get(0);
		return retMap;
	}
}
