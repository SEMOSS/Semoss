package prerna.project.impl;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.project.api.IProject;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.ProjectUtils;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class ProjectHelper {

	private static final Logger classLogger = LogManager.getLogger(ProjectHelper.class);
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

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
	public static IProject generateNewProject(String projectName, 
			boolean hasPortal, String portalName,
			String gitProvider, String gitCloneUrl, User user, Logger logger) {
		String projectId = UUID.randomUUID().toString();
		return generateNewProject(projectId, projectName, hasPortal, portalName, gitProvider, gitCloneUrl, user, logger);
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
	public static IProject generateNewProject(String projectId, String projectName, 
			boolean hasPortal, String portalName, 
			String gitProvider, String gitCloneUrl, User user, Logger logger) {
		if(projectName == null || projectName.isEmpty()) {
			throw new IllegalArgumentException("Need to provide a name for the project");
		}

		if(user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to create a project", PixelDataType.CONST_STRING, 
					PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
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
			// Add database into DIHelper so that the web watcher doesn't try to load as well
			tempSmss = SmssUtilities.createTemporaryProjectSmss(projectId, projectName, hasPortal, portalName, gitProvider, gitCloneUrl, null);
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

			SecurityProjectUtils.addProject(projectId, user);
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider ap : logins) {
					SecurityProjectUtils.addProjectOwner(user, projectId, user.getAccessToken(ap).getId());
				}
			}

			return project;
		} catch(Exception e) {
			error = true;
			throw new SemossPixelException(NounMetadata.getErrorNounMessage("An error occurred creating the new project"));
		} finally {
			// if we had an error
			if(error) {
				if(smssFile != null && smssFile.exists() && smssFile.isFile()) {
					smssFile.delete();
				}
				if(smssFile != null) {
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
			if(tempSmss != null && tempSmss.exists() && tempSmss.isFile()) {
				tempSmss.delete();
			}
		}
	}

	/**
	 * Load the insights rdbms engine using the main engine properties
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
	 * @param engineId
	 * @param engineName
	 * @param rdbmsInsightsType
	 * @param insightDatabaseLoc
	 * @param logger
	 * @return
	 * @throws Exception 
	 */
	private static RDBMSNativeEngine loadInsightsDatabase(String projectId, String projectName, RdbmsTypeEnum rdbmsInsightsType, String insightDatabaseLoc, Logger logger) throws Exception {
		if(insightDatabaseLoc == null || !new File(insightDatabaseLoc).exists()) {
			// make a new database
			RDBMSNativeEngine insightsRdbms = (RDBMSNativeEngine) ProjectUtils.generateInsightsDatabase(projectId, projectName);
			//			UploadUtilities.addExploreInstanceInsight(projectId, projectName, insightsRdbms);
			//			UploadUtilities.addInsightUsageStats(projectId, projectName, insightsRdbms);
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
		String propFile = baseFolder + DIR_SEPARATOR + Constants.PROJECT_FOLDER + DIR_SEPARATOR + SmssUtilities.getUniqueName(projectName, projectId) + ".smss";
		String pass = null;
		if(new File(Utility.normalizePath(propFile)).exists()) {
			pass = insightsRdbms.decryptPass(Utility.normalizePath(propFile), true);
		}
		if(pass == null) {
			pass = "";
		}

		if(rdbmsInsightsType == RdbmsTypeEnum.SQLITE) {
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
		String tableExistsQuery = queryUtil.tableExistsQuery("QUESTION_ID", insightsRdbms.getDatabase(), insightsRdbms.getSchema());
		boolean tableExists = false;
		IRawSelectWrapper wrapper = null;
		try {
			wrapper  = WrapperManager.getInstance().getRawWrapper(insightsRdbms, tableExistsQuery);
			tableExists = wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		if(!tableExists) {
			// well, you already created the file
			// need to run the queries to make this
			ProjectUtils.runInsightCreateTableQueries(insightsRdbms);
		} else {

			// adding new insight metadata
			try {
				if(!queryUtil.tableExists(insightsRdbms.getConnection(), "INSIGHTMETA", insightsRdbms.getDatabase(), insightsRdbms.getSchema())) {
					String[] columns = new String[] { "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"};
					String[] types = new String[] { "VARCHAR(255)", "VARCHAR(255)", queryUtil.getClobDataTypeName(), "INT"};
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
					allCols = queryUtil.getTableColumns(insightsRdbms.getConnection(), InsightAdministrator.TABLE_NAME, insightsRdbms.getDatabase(), insightsRdbms.getSchema());
					// this should return in all upper case
					// ... but sometimes it is not -_- i.e. postgres always lowercases
					// TEMPORARY CHECK! - added 01/29/2022
					if(!allCols.contains(InsightAdministrator.CACHE_MINUTES_COL.toUpperCase()) && !allCols.contains(InsightAdministrator.CACHE_MINUTES_COL.toLowerCase())) {
						if(queryUtil.allowIfExistsModifyColumnSyntax()) {
							insightsRdbms.insertData(queryUtil.alterTableAddColumnIfNotExists(InsightAdministrator.TABLE_NAME, 
									InsightAdministrator.CACHE_MINUTES_COL, "INT"));
						} else {
							insightsRdbms.insertData(queryUtil.alterTableAddColumn(InsightAdministrator.TABLE_NAME, 
									InsightAdministrator.CACHE_MINUTES_COL, "INT"));
						}
					}
					// TEMPORARY CHECK! - added 02/07/2022
					if(!allCols.contains(InsightAdministrator.CACHE_ENCRYPT_COL.toUpperCase()) && !allCols.contains(InsightAdministrator.CACHE_ENCRYPT_COL.toLowerCase())) {
						if(queryUtil.allowIfExistsModifyColumnSyntax()) {
							insightsRdbms.insertData(queryUtil.alterTableAddColumnIfNotExists(InsightAdministrator.TABLE_NAME, 
									InsightAdministrator.CACHE_ENCRYPT_COL, queryUtil.getBooleanDataTypeName()));
						} else {
							insightsRdbms.insertData(queryUtil.alterTableAddColumn(InsightAdministrator.TABLE_NAME, 
									InsightAdministrator.CACHE_ENCRYPT_COL, queryUtil.getBooleanDataTypeName()));
						}
					}
					// TEMPORARY CHECK! - added 02/14/2022
					if(!allCols.contains(InsightAdministrator.CACHE_CRON_COL.toUpperCase()) && !allCols.contains(InsightAdministrator.CACHE_CRON_COL.toLowerCase())) {
						if(queryUtil.allowIfExistsModifyColumnSyntax()) {
							insightsRdbms.insertData(queryUtil.alterTableAddColumnIfNotExists(InsightAdministrator.TABLE_NAME, 
									InsightAdministrator.CACHE_CRON_COL, "VARCHAR(25)"));
						} else {
							insightsRdbms.insertData(queryUtil.alterTableAddColumn(InsightAdministrator.TABLE_NAME, 
									InsightAdministrator.CACHE_CRON_COL, "VARCHAR(25)"));
						}
					}
					// TEMPORARY CHECK! - added 02/14/2022
					if(!allCols.contains(InsightAdministrator.CACHED_ON_COL.toUpperCase()) && !allCols.contains(InsightAdministrator.CACHED_ON_COL.toLowerCase())) {
						if(queryUtil.allowIfExistsModifyColumnSyntax()) {
							insightsRdbms.insertData(queryUtil.alterTableAddColumnIfNotExists(InsightAdministrator.TABLE_NAME, 
									InsightAdministrator.CACHED_ON_COL, queryUtil.getDateWithTimeDataType()));
						} else {
							insightsRdbms.insertData(queryUtil.alterTableAddColumn(InsightAdministrator.TABLE_NAME, 
									InsightAdministrator.CACHED_ON_COL, queryUtil.getDateWithTimeDataType()));
						}
					}
					// TEMPORARY CHECK! - added 02/02/2023
					if(!allCols.contains(InsightAdministrator.SCHEMA_NAME_COL.toUpperCase()) && !allCols.contains(InsightAdministrator.SCHEMA_NAME_COL.toLowerCase())) {
						if(queryUtil.allowIfExistsModifyColumnSyntax()) {
							insightsRdbms.insertData(queryUtil.alterTableAddColumnIfNotExists(InsightAdministrator.TABLE_NAME, 
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

			//			// okay, might need to do some updates
			//			String q = "SELECT TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='QUESTION_ID' and COLUMN_NAME='ID'";
			//			IRawSelectWrapper wrap = WrapperManager.getInstance().getRawWrapper(insightsRdbms, q);
			//			while(wrap.hasNext()) {
			//				String val = wrap.next().getValues()[0] + "";
			//				if(!val.equals("VARCHAR")) {
			//					String update = "ALTER TABLE QUESTION_ID ALTER COLUMN ID VARCHAR(50);";
			//					try {
			//						insightsRdbms.insertData(update);
			//					} catch (SQLException e) {
			//						classLogger.error(Constants.STACKTRACE, e);
			//					}
			//					insightsRdbms.commit();
			//				}
			//			}
			//			wrap.cleanUp();
			//			
			//			// previous alter column ... might be time to delete this ? 11/8/2018 
			//			String update = "ALTER TABLE QUESTION_ID ADD COLUMN IF NOT EXISTS HIDDEN_INSIGHT BOOLEAN DEFAULT FALSE";								
			//			try {
			//				insightsRdbms.insertData(update);
			//			} catch (SQLException e) {
			//				classLogger.error(Constants.STACKTRACE, e);
			//			}
			//			insightsRdbms.commit();
			//
			//			// THIS IS FOR LEGACY !!!!
			//			// TODO: EVENTUALLY WE WILL DELETE THIS
			//			// TODO: EVENTUALLY WE WILL DELETE THIS
			//			// TODO: EVENTUALLY WE WILL DELETE THIS
			//			// TODO: EVENTUALLY WE WILL DELETE THIS
			//			// TODO: EVENTUALLY WE WILL DELETE THIS
			//			InsightsDatabaseUpdater3CacheableColumn.update(engineId, insightsRdbms);
		}
		return insightsRdbms;
	}

}
