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
package prerna.project.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteProjectRunner;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.project.api.IProject;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.InsightsRDBMSUtils;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public final class ProjectHelper {

	private static final Logger classLogger = LogManager.getLogger(ProjectHelper.class);

	/**
	 * PROJECTMETA tag value marking a SKILL-type project. Written by
	 * {@link #createSkillProject} for user skills and by ProjectWatcher for the
	 * built-in platform skill projects. (Legacy rows may carry the old value
	 * "Skill_Project"; PROJECT.TYPE is the authoritative marker.)
	 */
	public static final String SKILL_PROJECT_TAG = "SKILL";

	// regex pattern for UUIDs
	private static final String UUID_PATTERN_STRING = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$";

	// regex used to split tokens
	private static final String TOKEN_SPLIT_REGEX = "[^a-zA-Z0-9-]+";

	// list of file extensions to search UUIDs from
	private static final String[] DEPENDENCIES_FILE_EXTENSIONS = { ".js", ".jsx", ".java", ".env", ".py", ".ts", ".tsx",
			".json" };

	private ProjectHelper() {

	}

	/**
	 * Create a project with a new random id. See
	 * {@link #generateNewProject(String, String, IProject.PROJECT_TYPE, boolean, String, String, User, Logger)}.
	 *
	 * @param projectName the project name
	 * @param projectType the project type
	 * @param global      whether the project is public (PROJECT.GLOBAL)
	 * @param gitProvider the git provider written to the smss, or null
	 * @param gitCloneUrl the git clone URL written to the smss, or null
	 * @param user        the signed in user, who becomes the owner
	 * @param logger      the logger for progress messages
	 * @return the opened project
	 */
	public static IProject generateNewProject(String projectName, IProject.PROJECT_TYPE projectType, boolean global,
			String gitProvider, String gitCloneUrl, User user, Logger logger) {
		String projectId = UUID.randomUUID().toString();
		return generateNewProject(projectId, projectName, projectType, global, gitProvider, gitCloneUrl, user, logger);
	}

	/**
	 * Create a project: check that the user may create it, write and open its smss,
	 * push it to the cluster when clustered, and register it in the security
	 * database with the user as owner under each of their logins. When a step after
	 * the checks fails, the partly created project is removed before the error is
	 * thrown.
	 *
	 * @param projectId   the project id
	 * @param projectName the project name
	 * @param projectType the project type
	 * @param global      whether the project is public (PROJECT.GLOBAL)
	 * @param gitProvider the git provider written to the smss, or null
	 * @param gitCloneUrl the git clone URL written to the smss, or null
	 * @param user        the signed in user, who becomes the owner
	 * @param logger      the logger for progress messages
	 * @return the opened project
	 * @throws IllegalArgumentException when the project name is null or empty
	 * @throws SemossPixelException     when there is no signed in user, the user is
	 *                                  anonymous, not a publisher or not an admin
	 *                                  where those are required, a project folder
	 *                                  with the name already exists, or creating
	 *                                  the project fails
	 */
	public static IProject generateNewProject(String projectId, String projectName, IProject.PROJECT_TYPE projectType,
			boolean global, String gitProvider, String gitCloneUrl, User user, Logger logger) {
		if (projectName == null || projectName.isEmpty()) {
			throw new IllegalArgumentException("Need to provide a name for the project");
		}

		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to create a project",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			AbstractReactor.throwAnonymousUserError();
		}

		// throw error is user doesn't have rights to publish new apps
		if (AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(user)) {
			AbstractReactor.throwUserNotPublisherError();
		}

		if (AbstractSecurityUtils.adminOnlyProjectAdd(projectType) && !SecurityAdminUtils.userIsAdmin(user)) {
			AbstractReactor.throwFunctionalityOnlyExposedForAdminsError();
		}

		File newProjectFolder;
		try {
			newProjectFolder = SmssUtilities.validateProject(user, projectName, projectId);
			newProjectFolder.mkdirs();
		} catch (IOException e) {
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}

		// Create the project class
		IProject project = new Project();

		File tempSmss = null;
		File smssFile = null;
		boolean error = false;
		boolean pushedToCluster = false;
		boolean addedToSecurity = false;
		try {
			logger.info("Creating project workspace");
			// Add database into DIHelper so that the web watcher doesn't try to load as
			// well
			tempSmss = SmssUtilities.createTemporaryProjectSmss(projectId, projectName, projectType, gitProvider,
					gitCloneUrl, null);
			DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, tempSmss.getAbsolutePath());

			// Only at end do we add to DIHelper
			DIHelper.getInstance().setProjectProperty(projectId, project);
			String projects = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
			projects = projects + ";" + projectId;
			DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projects);

			// Rename .temp to .smss
			smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(tempSmss, smssFile);
			tempSmss.delete();

			// Update engine smss file location
			project.open(smssFile.getAbsolutePath());
			logger.info("Finished creating project");
			DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, smssFile.getAbsolutePath());

			if (ClusterUtil.IS_CLUSTER) {
				logger.info("Syncing project for cloud backup");
				ClusterUtil.pushProject(projectId);
				pushedToCluster = true;
			}

			SecurityProjectUtils.addProject(projectId, global, user);
			addedToSecurity = true;
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider ap : logins) {
					SecurityProjectUtils.addProjectOwner(user, projectId, user.getAccessToken(ap).getId());
				}
			}

			return project;
		} catch (Exception e) {
			error = true;
			classLogger.error("Failed to create project '{}' with id {}", projectName, projectId, e);
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("An error occurred creating the new project"));
		} finally {
			if (error) {
				cleanUpFailedProject(project, projectId, smssFile, newProjectFolder, pushedToCluster, addedToSecurity);
			}

			// always delete temp smss
			if (tempSmss != null && tempSmss.exists() && tempSmss.isFile()) {
				tempSmss.delete();
			}
		}
	}

	/**
	 * Undo a project creation that failed partway: close the project so its
	 * insights database releases its files, remove it from DIHelper, delete its
	 * smss and project folder, and delete its security rows and cluster copy when
	 * it got that far. Each step logs its own failure so the rest still run.
	 *
	 * @param project         the project being created
	 * @param projectId       the project id
	 * @param smssFile        the project smss, or null when it was not written
	 * @param projectFolder   the project folder
	 * @param pushedToCluster whether the project was pushed to the cluster
	 * @param addedToSecurity whether the project was added to the security database
	 */
	private static void cleanUpFailedProject(IProject project, String projectId, File smssFile, File projectFolder,
			boolean pushedToCluster, boolean addedToSecurity) {
		try {
			project.close();
		} catch (Exception e) {
			classLogger.error("Failed to close project {} while cleaning up after it could not be created: {}",
					projectId, e.getMessage(), e);
		}

		try {
			UploadUtilities.removeProjectFromDIHelper(projectId);
		} catch (Exception e) {
			classLogger.error("Failed to remove project {} from DIHelper after it could not be created: {}", projectId,
					e.getMessage(), e);
		}

		if (smssFile != null) {
			FileUtils.deleteQuietly(smssFile);
		}
		try {
			FileUtils.deleteDirectory(projectFolder);
		} catch (IOException e) {
			classLogger.error("Failed to delete folder {} while cleaning up after project {} could not be created: {}",
					projectFolder, projectId, e.getMessage(), e);
		}

		if (addedToSecurity) {
			try {
				SecurityProjectUtils.deleteProject(projectId);
			} catch (Exception e) {
				classLogger.error("Failed to delete the security rows for project {} after it could not be created: {}",
						projectId, e.getMessage(), e);
			}
		}

		if (pushedToCluster) {
			Thread.ofVirtual().start(new DeleteProjectRunner(projectId));
		}
	}

	/**
	 * Open a project's insights database from the project smss properties. A
	 * missing database file is created with the project's RDBMS_INSIGHTS_TYPE, and
	 * an existing database gets any missing insight tables and columns.
	 *
	 * @param mainEngineProp the project smss properties, which must set
	 *                       RDBMS_INSIGHTS; RDBMS_INSIGHTS_TYPE defaults to H2_DB
	 * @param logger         the logger for progress messages
	 * @return the opened insights database
	 * @throws Exception if RDBMS_INSIGHTS_TYPE is not a database type or the
	 *                   database cannot be opened
	 */
	public static IRDBMSEngine loadInsightsEngine(Properties mainEngineProp, Logger logger) throws Exception {
		String projectId = mainEngineProp.getProperty(Constants.PROJECT);

		String rdbmsInsightsTypeStr = mainEngineProp.getProperty(Constants.RDBMS_INSIGHTS_TYPE, "H2_DB");
		RdbmsTypeEnum rdbmsInsightsType = RdbmsTypeEnum.getEnumFromStringOrThrow(rdbmsInsightsTypeStr);
		String insightDatabaseLoc = SmssUtilities.getInsightsRdbmsFile(mainEngineProp).getAbsolutePath();
		return loadInsightsDatabase(projectId, rdbmsInsightsType, insightDatabaseLoc, logger);
	}

	/**
	 * Open the insights database file. When the file does not exist, a new database
	 * of the given type is created in the file's folder with all the insight
	 * tables. An existing database without the QUESTION_ID table gets all the
	 * insight tables. Otherwise a missing INSIGHTMETA table and any missing cache
	 * and schema columns on QUESTION_ID are added, and a failure there is logged
	 * rather than thrown.
	 *
	 * @param projectId          the project id
	 * @param rdbmsInsightsType  the database type: SQLITE, or H2 for any other type
	 * @param insightDatabaseLoc the database file path
	 * @param logger             the logger for progress messages
	 * @return the opened insights database
	 * @throws Exception if the database cannot be created or opened
	 */
	private static IRDBMSEngine loadInsightsDatabase(String projectId, RdbmsTypeEnum rdbmsInsightsType,
			String insightDatabaseLoc, Logger logger) throws Exception {
		File insightDatabaseFile = new File(insightDatabaseLoc);
		if (!insightDatabaseFile.exists()) {
			return InsightsRDBMSUtils.generateInsightsDatabase(projectId, rdbmsInsightsType,
					insightDatabaseFile.getParent());
		}
		IRDBMSEngine insightsRdbms = new RDBMSNativeEngine();
		Properties insightSmssProp = new Properties();
		insightSmssProp.put(Constants.DRIVER, rdbmsInsightsType.getDriver());
		insightSmssProp.put(Constants.RDBMS_TYPE, rdbmsInsightsType.getLabel());
		String connURL = null;
		logger.info("Insight rdbms database location is {}", Utility.cleanLogString(insightDatabaseLoc));

		if (rdbmsInsightsType == RdbmsTypeEnum.SQLITE) {
			connURL = rdbmsInsightsType.getUrlPrefix() + ":" + insightDatabaseLoc;
			insightSmssProp.put(Constants.USERNAME, "");
			insightSmssProp.put(Constants.PASSWORD, "");
		} else {
			connURL = rdbmsInsightsType.getUrlPrefix() + ":nio:" + insightDatabaseLoc.replace(".mv.db", "");
			insightSmssProp.put(Constants.USERNAME, "sa");
			insightSmssProp.put(Constants.PASSWORD, "");
		}
		logger.info("Insight rdbms database url is {}", Utility.cleanLogString(connURL));
		insightSmssProp.put(Constants.CONNECTION_URL, connURL);
		insightsRdbms.setBasic(true);
		insightsRdbms.open(insightSmssProp);
		insightsRdbms.setEngineId(projectId + Constants.RDBMS_INSIGHTS_ENGINE_SUFFIX);

		AbstractSqlQueryUtil queryUtil = insightsRdbms.getQueryUtil();
		String tableExistsQuery = queryUtil.tableExistsQuery("QUESTION_ID", insightsRdbms.getDatabase(),
				insightsRdbms.getSchema());
		boolean tableExists = false;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(insightsRdbms, tableExistsQuery)) {
			tableExists = wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Failed to check whether the QUESTION_ID table exists in the insights database "
					+ "for project '{}', treating it as absent", projectId, e);
		}

		if (!tableExists) {
			// well, you already created the file
			// need to run the queries to make this
			InsightsRDBMSUtils.runInsightCreateTableQueries(insightsRdbms);
		} else {

			// adding new insight metadata
			try {
				if (!metadataTableExists(insightsRdbms.getConnection(), "INSIGHTMETA", insightsRdbms.getSchema())) {
					String[] columns = new String[] { "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER" };
					String[] types = new String[] { "VARCHAR(255)", "VARCHAR(255)", queryUtil.getClobDataTypeName(),
							"INT" };
					try {
						insightsRdbms.insertData(queryUtil.createTable("INSIGHTMETA", columns, types));
					} catch (SQLException e) {
						classLogger.error(
								"Failed to create the INSIGHTMETA table in the insights database " + "for project '{}'",
								projectId, e);
					}
				}
			} catch (SQLException e) {
				classLogger.error("Failed to check whether the INSIGHTMETA table exists in the insights database "
						+ "for project '{}'", projectId, e);
			}

			{
				List<String> allCols;
				try {
					allCols = metadataTableColumns(insightsRdbms.getConnection(), InsightAdministrator.TABLE_NAME,
							insightsRdbms.getSchema());
					// this should return in all upper case
					// ... but sometimes it is not -_- i.e. postgres always lowercases
					// TEMPORARY CHECK! - added 01/29/2022
					if (!allCols.contains(InsightAdministrator.CACHE_MINUTES_COL.toUpperCase())
							&& !allCols.contains(InsightAdministrator.CACHE_MINUTES_COL.toLowerCase())) {
						if (queryUtil.allowIfExistsModifyColumnSyntax()) {
							insightsRdbms.insertData(queryUtil.alterTableAddColumnIfNotExists(
									InsightAdministrator.TABLE_NAME, InsightAdministrator.CACHE_MINUTES_COL, "INT"));
						} else {
							insightsRdbms.insertData(queryUtil.alterTableAddColumn(InsightAdministrator.TABLE_NAME,
									InsightAdministrator.CACHE_MINUTES_COL, "INT"));
						}
					}
					// TEMPORARY CHECK! - added 02/07/2022
					if (!allCols.contains(InsightAdministrator.CACHE_ENCRYPT_COL.toUpperCase())
							&& !allCols.contains(InsightAdministrator.CACHE_ENCRYPT_COL.toLowerCase())) {
						if (queryUtil.allowIfExistsModifyColumnSyntax()) {
							insightsRdbms.insertData(queryUtil.alterTableAddColumnIfNotExists(
									InsightAdministrator.TABLE_NAME, InsightAdministrator.CACHE_ENCRYPT_COL,
									queryUtil.getBooleanDataTypeName()));
						} else {
							insightsRdbms.insertData(queryUtil.alterTableAddColumn(InsightAdministrator.TABLE_NAME,
									InsightAdministrator.CACHE_ENCRYPT_COL, queryUtil.getBooleanDataTypeName()));
						}
					}
					// TEMPORARY CHECK! - added 02/14/2022
					if (!allCols.contains(InsightAdministrator.CACHE_CRON_COL.toUpperCase())
							&& !allCols.contains(InsightAdministrator.CACHE_CRON_COL.toLowerCase())) {
						if (queryUtil.allowIfExistsModifyColumnSyntax()) {
							insightsRdbms.insertData(
									queryUtil.alterTableAddColumnIfNotExists(InsightAdministrator.TABLE_NAME,
											InsightAdministrator.CACHE_CRON_COL, "VARCHAR(25)"));
						} else {
							insightsRdbms.insertData(queryUtil.alterTableAddColumn(InsightAdministrator.TABLE_NAME,
									InsightAdministrator.CACHE_CRON_COL, "VARCHAR(25)"));
						}
					}
					// TEMPORARY CHECK! - added 02/14/2022
					if (!allCols.contains(InsightAdministrator.CACHED_ON_COL.toUpperCase())
							&& !allCols.contains(InsightAdministrator.CACHED_ON_COL.toLowerCase())) {
						if (queryUtil.allowIfExistsModifyColumnSyntax()) {
							insightsRdbms.insertData(
									queryUtil.alterTableAddColumnIfNotExists(InsightAdministrator.TABLE_NAME,
											InsightAdministrator.CACHED_ON_COL, queryUtil.getDateWithTimeDataType()));
						} else {
							insightsRdbms.insertData(queryUtil.alterTableAddColumn(InsightAdministrator.TABLE_NAME,
									InsightAdministrator.CACHED_ON_COL, queryUtil.getDateWithTimeDataType()));
						}
					}
					// TEMPORARY CHECK! - added 02/02/2023
					if (!allCols.contains(InsightAdministrator.SCHEMA_NAME_COL.toUpperCase())
							&& !allCols.contains(InsightAdministrator.SCHEMA_NAME_COL.toLowerCase())) {
						if (queryUtil.allowIfExistsModifyColumnSyntax()) {
							insightsRdbms.insertData(
									queryUtil.alterTableAddColumnIfNotExists(InsightAdministrator.TABLE_NAME,
											InsightAdministrator.SCHEMA_NAME_COL, "VARCHAR(255)"));
						} else {
							insightsRdbms.insertData(queryUtil.alterTableAddColumn(InsightAdministrator.TABLE_NAME,
									InsightAdministrator.SCHEMA_NAME_COL, "VARCHAR(255)"));
						}
					}
				} catch (SQLException e) {
					classLogger.error("Failed to add missing columns to the {} table in the insights database "
							+ "for project '{}'", InsightAdministrator.TABLE_NAME, projectId, e);
				}
			}
		}
		return insightsRdbms;
	}

	/**
	 * Check through the JDBC metadata whether a table exists in a schema, ignoring
	 * case.
	 *
	 * @param connection the database connection, left open
	 * @param tableName  the table name
	 * @param schema     the schema, or null for any schema
	 * @return true when the table exists
	 * @throws SQLException if the metadata cannot be read
	 */
	private static boolean metadataTableExists(Connection connection, String tableName, String schema)
			throws SQLException {
		try (ResultSet tables = connection.getMetaData().getTables(null, schema, null, new String[] { "TABLE" })) {
			while (tables.next()) {
				if (tableName.equalsIgnoreCase(tables.getString("TABLE_NAME"))) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * List a table's column names from the JDBC metadata, in upper case. The table
	 * name is matched ignoring case.
	 *
	 * @param connection the database connection, left open
	 * @param tableName  the table name
	 * @param schema     the schema, or null for any schema
	 * @return the upper case column names, empty when the table does not exist
	 * @throws SQLException if the metadata cannot be read
	 */
	private static List<String> metadataTableColumns(Connection connection, String tableName, String schema)
			throws SQLException {
		List<String> columns = new ArrayList<>();
		try (ResultSet columnRows = connection.getMetaData().getColumns(null, schema, null, null)) {
			while (columnRows.next()) {
				if (tableName.equalsIgnoreCase(columnRows.getString("TABLE_NAME"))) {
					columns.add(columnRows.getString("COLUMN_NAME").toUpperCase());
				}
			}
		}
		return columns;
	}

	/**
	 * Find the engines a project's files refer to. Every UUID in the project's code
	 * and json files is looked up in the engine table. The result has a "success"
	 * map from engine id to its engineType, engineName and files, and a "failed"
	 * map from each UUID that is not an engine to its files. Each files entry is a
	 * list of filename and instances (the number of times the UUID appears).
	 *
	 * @param projectFolder the project folder to search
	 * @return the "success" and "failed" maps
	 */
	public static Map<String, Object> extractEngineIdsFromProjectFolder(File projectFolder) {
		// extract engineIds from project including project_dependencies.json
		Map<String, Map<String, Object>> uuidToFiles = ProjectHelper.countUuidsInProjectFolder(projectFolder);
		// process engineIds and set project dependencies
		Map<String, Object> engineInfo = ProjectHelper.validateProjectDependencies(uuidToFiles.keySet());
		Map<String, Map<String, String>> successMap = (Map<String, Map<String, String>>) engineInfo.get("success");
		Set<String> failedSet = (Set<String>) engineInfo.get("failed");

		// final success list of engineIds
		Map<String, Map<String, Object>> successResult = new HashMap<>();
		for (Map.Entry<String, Map<String, String>> entry : successMap.entrySet()) {
			String engineId = entry.getKey();
			Map<String, String> engineMeta = entry.getValue();

			Map<String, Object> value = new HashMap<>();
			value.put("engineType", engineMeta.get("engineType"));
			value.put("engineName", engineMeta.get("engineName"));
			value.put("files", uuidToFiles.get(engineId).get("files"));
			successResult.put(engineId, value);
		}

		// final failed list of engineIds
		Map<String, Map<String, Object>> failureResult = new HashMap<>();
		for (String engineId : failedSet) {
			Map<String, Object> value = new HashMap<>();
			value.put("files",
					uuidToFiles.containsKey(engineId) ? uuidToFiles.get(engineId).get("files") : new ArrayList<>());
			failureResult.put(engineId, value);
		}

		Map<String, Object> engineIdMap = new HashMap<>();
		engineIdMap.put("success", successResult);
		engineIdMap.put("failed", failureResult);
		return engineIdMap;
	}

	/**
	 * Count the version 4 UUIDs in every file under a folder whose extension is in
	 * DEPENDENCIES_FILE_EXTENSIONS. A file that cannot be read is logged and
	 * skipped.
	 *
	 * @param finalProjectFolderF the folder to search, including subfolders
	 * @return each UUID mapped to a "files" list of filename and instances, empty
	 *         when the folder is null or not a directory
	 */
	private static Map<String, Map<String, Object>> countUuidsInProjectFolder(File finalProjectFolderF) {
		if (finalProjectFolderF == null || !finalProjectFolderF.isDirectory()) {
			return new HashMap<>();
		}

		Map<String, Map<String, Object>> uuidDetailsMap = new HashMap<>();
		Pattern UUID_PATTERN = Pattern.compile(UUID_PATTERN_STRING);
		String folderPath = finalProjectFolderF.getAbsolutePath();

		CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPLACE)
				.onUnmappableCharacter(CodingErrorAction.REPLACE);

		try (Stream<Path> stream = Files.walk(Paths.get(folderPath))) {
			stream.filter(Files::isRegularFile).filter(path -> {
				String fileName = path.getFileName().toString().toLowerCase();
				for (String extension : DEPENDENCIES_FILE_EXTENSIONS) {
					if (fileName.endsWith(extension)) {
						return true;
					}
				}
				return false;
			}).forEach(path -> {
				// get the file name
				String fileName = path.getFileName().toString();
				try (BufferedReader reader = new BufferedReader(
						new InputStreamReader(Files.newInputStream(path), decoder));
						Stream<String> lines = reader.lines()) {

					// to keep the count of no of occurrence of a particular uuid in a particular
					// file
					Map<String, Integer> localCountMap = new HashMap<>();

					lines.forEach(line -> {
						String[] tokens = line.split(TOKEN_SPLIT_REGEX);
						for (String token : tokens) {
							Matcher matcher = UUID_PATTERN.matcher(token.trim());
							if (matcher.matches()) {
								String uuid = token.trim();
								localCountMap.put(uuid, localCountMap.getOrDefault(uuid, 0) + 1);
							}
						}
					});

					for (Map.Entry<String, Integer> entry : localCountMap.entrySet()) {
						String uuid = entry.getKey();
						int count = entry.getValue();

						// get or create the UUID (files) entry
						Map<String, Object> uuidEntry = uuidDetailsMap.computeIfAbsent(uuid, k -> {
							Map<String, Object> newEntry = new HashMap<>();
							newEntry.put("files", new ArrayList<Map<String, Object>>());
							return newEntry;
						});

						// get the files list
						List<Map<String, Object>> filesList = (List<Map<String, Object>>) uuidEntry.get("files");

						// add the current file info to uuidEntry
						Map<String, Object> fileEntry = new HashMap<>();
						fileEntry.put("filename", fileName);
						fileEntry.put("instances", count);
						filesList.add(fileEntry);
					}

				} catch (IOException e) {
					classLogger.error("Failed to read {} while extracting engine ids, skipping it", path, e);
				}
			});

		} catch (IOException e) {
			classLogger.error("Failed to walk project folder {} while extracting engine ids", folderPath, e);
		}

		return uuidDetailsMap;
	}

	/**
	 * Split ids into those in the engine table and those that are not.
	 *
	 * @param engineIds the ids to check
	 * @return a "success" map from engine id to its engineType and engineName, and
	 *         a "failed" set of the ids that are not engines
	 */
	private static Map<String, Object> validateProjectDependencies(Collection<String> engineIds) {
		Map<String, Map<String, String>> success = new HashMap<>();
		Set<String> failed = new HashSet<>();

		for (String engineId : engineIds) {
			if (SecurityEngineUtils.containsEngineId(engineId)) {
				IEngine.CATALOG_TYPE engineType = SecurityEngineUtils.getEngineType(engineId);
				String engineName = SecurityEngineUtils.getEngineAliasForId(engineId);

				Map<String, String> engineInfo = new HashMap<>();
				engineInfo.put("engineType", engineType.toString());
				engineInfo.put("engineName", engineName);

				success.put(engineId, engineInfo);
			} else {
				failed.add(engineId);
			}
		}

		// build final result map to return
		Map<String, Object> result = new HashMap<>();
		result.put("success", success);
		result.put("failed", failed);
		return result;
	}

	/**
	 * Create a project and tag it as a workspace project in PROJECTMETA. See
	 * {@link #generateNewProject(String, String, IProject.PROJECT_TYPE, boolean, String, String, User, Logger)}.
	 *
	 * @param projectId   the project id
	 * @param projectName the project name
	 * @param projectType the project type
	 * @param global      whether the project is public (PROJECT.GLOBAL)
	 * @param gitProvider the git provider written to the smss, or null
	 * @param gitCloneUrl the git clone URL written to the smss, or null
	 * @param user        the signed in user, who becomes the owner
	 * @param logger      the logger for progress messages
	 * @return the opened project
	 */
	public static IProject createWorkspaceProject(String projectId, String projectName,
			IProject.PROJECT_TYPE projectType, boolean global, String gitProvider, String gitCloneUrl, User user,
			Logger logger) {
		IProject project = generateNewProject(projectId, projectName, projectType, global, gitProvider, gitCloneUrl,
				user, logger);
		Map<String, Object> metadata = new HashMap<>();
		metadata.put("tag", ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG);
		SecurityProjectUtils.updateProjectMetadata(projectId, metadata);
		return project;
	}

	/**
	 * Create a SKILL project and tag it with {@link #SKILL_PROJECT_TAG} in
	 * PROJECTMETA. See
	 * {@link #generateNewProject(String, String, IProject.PROJECT_TYPE, boolean, String, String, User, Logger)}.
	 *
	 * @param projectId   the project id
	 * @param projectName the project name
	 * @param global      whether the project is public (PROJECT.GLOBAL)
	 * @param gitProvider the git provider written to the smss, or null
	 * @param gitCloneUrl the git clone URL written to the smss, or null
	 * @param user        the signed in user, who becomes the owner
	 * @param logger      the logger for progress messages
	 * @return the opened project
	 */
	public static IProject createSkillProject(String projectId, String projectName, boolean global, String gitProvider,
			String gitCloneUrl, User user, Logger logger) {
		IProject project = generateNewProject(projectId, projectName, IProject.PROJECT_TYPE.SKILL, global, gitProvider,
				gitCloneUrl, user, logger);
		Map<String, Object> metadata = new HashMap<>();
		metadata.put("tag", SKILL_PROJECT_TAG);
		SecurityProjectUtils.updateProjectMetadata(projectId, metadata);
		return project;
	}

}
