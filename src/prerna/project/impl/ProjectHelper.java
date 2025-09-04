package prerna.project.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.apache.commons.io.FilenameUtils;
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
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
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
import prerna.util.EngineUtility;
import prerna.util.InsightsRDBMSUtils;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public final class ProjectHelper {

	private static final Logger classLogger = LogManager.getLogger(ProjectHelper.class);
	private static final String DIR_SEPARATOR = "/";

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
	 * 
	 * @param projectName
	 * @param hasPortal
	 * @param portalName
	 * @param gitProvider
	 * @param gitCloneUrl
	 * @param user
	 * @param logger
	 * @return
	 */
	public static IProject generateNewProject(String projectName, IProject.PROJECT_TYPE projectType, boolean global,
			boolean hasPortal, String portalName, String gitProvider, String gitCloneUrl, User user, Logger logger) {
		String projectId = UUID.randomUUID().toString();
		return generateNewProject(projectId, projectName, projectType, global, hasPortal, portalName, gitProvider,
				gitCloneUrl, user, logger);
	}

	/**
	 * 
	 * @param projectId
	 * @param projectName
	 * @param hasPortal
	 * @param portalName
	 * @param gitProvider
	 * @param gitCloneUrl
	 * @param user
	 * @param logger
	 * @return
	 */
	public static IProject generateNewProject(String projectId, String projectName, IProject.PROJECT_TYPE projectType,
			boolean global, boolean hasPortal, String portalName, String gitProvider, String gitCloneUrl, User user,
			Logger logger) {
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

		if (AbstractSecurityUtils.adminOnlyProjectAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			AbstractReactor.throwFunctionalityOnlyExposedForAdminsError();
		}

		try {
			File newProjectFolder = SmssUtilities.validateProject(user, projectName, projectId);
			newProjectFolder.mkdirs();
		} catch (IOException e) {
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}

		// Create the project class
		IProject project = new Project();

		File tempSmss = null;
		File smssFile = null;
		boolean error = false;
		try {
			logger.info("Creating project workspace");
			// Add database into DIHelper so that the web watcher doesn't try to load as
			// well
			tempSmss = SmssUtilities.createTemporaryProjectSmss(projectId, projectName, projectType, hasPortal,
					portalName, gitProvider, gitCloneUrl, null);
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
			}

			EngineUtility.createPipelineJsonInSpecificEngineFolder(IEngine.CATALOG_TYPE.PROJECT, projectId,
					projectName);

			SecurityProjectUtils.addProject(projectId, global, user);
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider ap : logins) {
					SecurityProjectUtils.addProjectOwner(user, projectId, user.getAccessToken(ap).getId());
				}
			}

			return project;
		} catch (Exception e) {
			error = true;
			throw new SemossPixelException(
					NounMetadata.getErrorNounMessage("An error occurred creating the new project"));
		} finally {
			// if we had an error
			if (error) {
				if (smssFile != null && smssFile.exists() && smssFile.isFile()) {
					smssFile.delete();
				}
				if (smssFile != null) {
					File projectFolder = new File(FilenameUtils.getBaseName(smssFile.getAbsolutePath()));
					// delete the engine folder and all its contents
					if (projectFolder != null && projectFolder.exists() && projectFolder.isDirectory()) {
						File[] files = projectFolder.listFiles();
						if (files != null) { // some JVMs return null for empty dirs
							for (File f : files) {
								try {
									FileUtils.forceDelete(f);
								} catch (IOException e) {
									classLogger.error(Constants.STACKTRACE, e);
								}
							}
						}
						try {
							FileUtils.forceDelete(projectFolder);
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}

			// always delete temp smss
			if (tempSmss != null && tempSmss.exists() && tempSmss.isFile()) {
				tempSmss.delete();
			}
		}
	}

	/**
	 * Load the insights rdbms engine using the main engine properties
	 * 
	 * @param mainEngineProp
	 * @return
	 * @throws Exception
	 */
	public static RDBMSNativeEngine loadInsightsEngine(Properties mainEngineProp, Logger logger) throws Exception {
		String projectId = mainEngineProp.getProperty(Constants.PROJECT);
		String projectName = mainEngineProp.getProperty(Constants.PROJECT_ALIAS);

		String rdbmsInsightsTypeStr = mainEngineProp.getProperty(Constants.RDBMS_INSIGHTS_TYPE, "H2_DB");
		RdbmsTypeEnum rdbmsInsightsType = RdbmsTypeEnum.valueOf(rdbmsInsightsTypeStr);
		String insightDatabaseLoc = SmssUtilities.getInsightsRdbmsFile(mainEngineProp).getAbsolutePath();
		return loadInsightsDatabase(projectId, projectName, rdbmsInsightsType, insightDatabaseLoc, logger);
	}

	/**
	 * Load the insights rdbms engine
	 * 
	 * @param engineId
	 * @param engineName
	 * @param rdbmsInsightsType
	 * @param insightDatabaseLoc
	 * @param logger
	 * @return
	 * @throws Exception
	 */
	private static RDBMSNativeEngine loadInsightsDatabase(String projectId, String projectName,
			RdbmsTypeEnum rdbmsInsightsType, String insightDatabaseLoc, Logger logger) throws Exception {
		if (insightDatabaseLoc == null || !new File(insightDatabaseLoc).exists()) {
			// make a new database
			RDBMSNativeEngine insightsRdbms = InsightsRDBMSUtils.generateInsightsDatabase(projectId, projectName);
			// UploadUtilities.addExploreInstanceInsight(projectId, projectName,
			// insightsRdbms);
			// UploadUtilities.addInsightUsageStats(projectId, projectName, insightsRdbms);
			return insightsRdbms;
		}
		RDBMSNativeEngine insightsRdbms = new RDBMSNativeEngine();
		Properties insightSmssProp = new Properties();
		insightSmssProp.put(Constants.DRIVER, rdbmsInsightsType.getDriver());
		insightSmssProp.put(Constants.RDBMS_TYPE, rdbmsInsightsType.getLabel());
		String connURL = null;
		logger.info("Insight rdbms database location is " + Utility.cleanLogString(insightDatabaseLoc));

		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		// decrypt the password
		String propFile = baseFolder + DIR_SEPARATOR + Constants.PROJECT_FOLDER + DIR_SEPARATOR
				+ SmssUtilities.getUniqueName(projectName, projectId) + ".smss";
		String pass = null;
		if (new File(Utility.normalizePath(propFile)).exists()) {
			pass = insightsRdbms.decryptPass(Utility.normalizePath(propFile), true);
		}
		if (pass == null) {
			pass = "";
		}

		if (rdbmsInsightsType == RdbmsTypeEnum.SQLITE) {
			connURL = rdbmsInsightsType.getUrlPrefix() + ":" + insightDatabaseLoc;
			insightSmssProp.put(Constants.USERNAME, "");
			insightSmssProp.put(Constants.PASSWORD, pass);
		} else {
			connURL = rdbmsInsightsType.getUrlPrefix() + ":nio:" + insightDatabaseLoc.replace(".mv.db", "");
			insightSmssProp.put(Constants.USERNAME, "sa");
			insightSmssProp.put(Constants.PASSWORD, pass);
		}
		logger.info("Insight rdbms database url is " + Utility.cleanLogString(connURL));
		insightSmssProp.put(Constants.CONNECTION_URL, connURL);
		insightsRdbms.setBasic(true);
		insightsRdbms.open(insightSmssProp);
		insightsRdbms.setEngineId(projectId + Constants.RDBMS_INSIGHTS_ENGINE_SUFFIX);

		AbstractSqlQueryUtil queryUtil = insightsRdbms.getQueryUtil();
		String tableExistsQuery = queryUtil.tableExistsQuery("QUESTION_ID", insightsRdbms.getDatabase(),
				insightsRdbms.getSchema());
		boolean tableExists = false;
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(insightsRdbms, tableExistsQuery);
			tableExists = wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		if (!tableExists) {
			// well, you already created the file
			// need to run the queries to make this
			InsightsRDBMSUtils.runInsightCreateTableQueries(insightsRdbms);
		} else {

			// adding new insight metadata
			try {
				if (!queryUtil.tableExists(insightsRdbms.getConnection(), "INSIGHTMETA", insightsRdbms.getDatabase(),
						insightsRdbms.getSchema())) {
					String[] columns = new String[] { "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER" };
					String[] types = new String[] { "VARCHAR(255)", "VARCHAR(255)", queryUtil.getClobDataTypeName(),
							"INT" };
					try {
						insightsRdbms.insertData(queryUtil.createTable("INSIGHTMETA", columns, types));
					} catch (SQLException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}

			{
				List<String> allCols;
				try {
					allCols = queryUtil.getTableColumns(insightsRdbms.getConnection(), InsightAdministrator.TABLE_NAME,
							insightsRdbms.getDatabase(), insightsRdbms.getSchema());
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
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return insightsRdbms;
	}

	/**
	 * 
	 * @param projectId
	 * @param projectFolder
	 * @return
	 */
	public static Map<String, Object> extractEngineIdsFromProjectFolder(String projectId, File projectFolder) {
		// extract engineIds from project including project_dependencies.json
		Map<String, Map<String, Object>> uuidToFiles = ProjectHelper.extractEngineIdsFromProjectFolder(projectFolder);
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
	 * Extracts and returns engineIds from the files with the given extensions in
	 * the project folder
	 * 
	 * @param finalProjectFolderF
	 * @return
	 */
	private static Map<String, Map<String, Object>> extractEngineIdsFromProjectFolder(File finalProjectFolderF) {
		if (finalProjectFolderF == null || !finalProjectFolderF.isDirectory()) {
			return new HashMap<>();
		}

		Map<String, Map<String, Object>> uuidDetailsMap = new HashMap<>();
		Pattern UUID_PATTERN = Pattern.compile(UUID_PATTERN_STRING);
		String folderPath = finalProjectFolderF.getAbsolutePath();

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
				try (Stream<String> lines = Files.lines(path)) {

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
					classLogger.error("Error reading file: " + path);
				}
			});

		} catch (IOException e) {
			classLogger.error("Error reading file: {}", folderPath, e);
		}

		return uuidDetailsMap;
	}

	/**
	 * Check if the extracted engineIds are present in the engine table or not
	 * 
	 * @param engineIds
	 * @return
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

}
