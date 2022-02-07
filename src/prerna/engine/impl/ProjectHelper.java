package prerna.engine.impl;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.CloudClient;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.project.api.IProject;
import prerna.project.impl.Project;
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

	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private ProjectHelper() {
		
	}
	
	public static IProject generateNewProject(String projectName, User user, Logger logger) {
		String projectId = UUID.randomUUID().toString();
		return generateNewProject(projectId, projectName, user, logger);
	}
	
	public static IProject generateNewProject(String projectId, String projectName, User user, Logger logger) {
		if(projectName == null || projectName.isEmpty()) {
			throw new IllegalArgumentException("Need to provide a name for the project");
		}
		
		boolean security = AbstractSecurityUtils.securityEnabled();
		if(security) {
			if(user == null) {
				NounMetadata noun = new NounMetadata("User must be signed into an account in order to create a project", PixelDataType.CONST_STRING, 
						PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
			
			// throw error if user is anonymous
			if(AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				AbstractReactor.throwAnonymousUserError();
			}
			
			// throw error is user doesn't have rights to publish new apps
			if(AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(user)) {
				AbstractReactor.throwUserNotPublisherError();
			}
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
			tempSmss = SmssUtilities.createTemporaryProjectSmss(projectId, projectName, null);
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
			project.openProject(smssFile.getAbsolutePath());
			logger.info("Finished creating project");
			DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, smssFile.getAbsolutePath());

			if (ClusterUtil.IS_CLUSTER) {
				logger.info("Syncing project for cloud backup");
				CloudClient.getClient().pushProject(projectId);
			}
			
			SecurityUpdateUtils.addProject(projectId);
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider ap : logins) {
					SecurityUpdateUtils.addProjectOwner(projectId, user.getAccessToken(ap).getId());
				}
			}
			
			return project;
		} catch(Exception e) {
			error = true;
			throw new SemossPixelException(NounMetadata.getErrorNounMessage("An error occured creating the new project"));
		} finally {
			// if we had an error
			if(error) {
				if(smssFile != null && smssFile.exists() && smssFile.isFile()) {
					smssFile.delete();
				}
				
				File projectFolder = new File(FilenameUtils.getBaseName(smssFile.getAbsolutePath()));
				// delete the engine folder and all its contents
				if (projectFolder != null && projectFolder.exists() && projectFolder.isDirectory()) {
					File[] files = projectFolder.listFiles();
					if (files != null) { // some JVMs return null for empty dirs
						for (File f : files) {
							try {
								FileUtils.forceDelete(f);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
					try {
						FileUtils.forceDelete(projectFolder);
					} catch (IOException e) {
						e.printStackTrace();
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
	 */
	public static RDBMSNativeEngine loadInsightsEngine(Properties mainEngineProp, Logger logger) {
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
	 */
	private static RDBMSNativeEngine loadInsightsDatabase(String projectId, String projectName, RdbmsTypeEnum rdbmsInsightsType, String insightDatabaseLoc, Logger logger) {
		if(insightDatabaseLoc == null || !new File(insightDatabaseLoc).exists()) {
			// make a new database
			RDBMSNativeEngine insightsRdbms = (RDBMSNativeEngine) ProjectUtils.generateInsightsDatabase(projectId, projectName);
//			UploadUtilities.addExploreInstanceInsight(projectId, projectName, insightsRdbms);
//			UploadUtilities.addInsightUsageStats(projectId, projectName, insightsRdbms);
			return insightsRdbms;
		}
		RDBMSNativeEngine insightsRdbms = new RDBMSNativeEngine();
		Properties prop = new Properties();
		prop.put(Constants.DRIVER, rdbmsInsightsType.getDriver());
		prop.put(Constants.RDBMS_TYPE, rdbmsInsightsType.getLabel());
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
			prop.put(Constants.USERNAME, "");
			prop.put(Constants.PASSWORD, pass);
		} else {
			connURL = rdbmsInsightsType.getUrlPrefix() + ":nio:" + insightDatabaseLoc.replace(".mv.db", "");
			prop.put(Constants.USERNAME, "sa");
			prop.put(Constants.PASSWORD, pass);
		}
		logger.info("Insight rdbms database url is " + Utility.cleanLogString(connURL));
		prop.put(Constants.CONNECTION_URL, connURL);

		insightsRdbms.setProp(prop);
		insightsRdbms.openDB(null);
		insightsRdbms.setEngineId(projectId + "_INSIGHTS_RDBMS");
		
		AbstractSqlQueryUtil queryUtil = insightsRdbms.getQueryUtil();
		String tableExistsQuery = queryUtil.tableExistsQuery("QUESTION_ID", insightsRdbms.getSchema());
		boolean tableExists = false;
		IRawSelectWrapper wrapper = null;
		try {
			wrapper  = WrapperManager.getInstance().getRawWrapper(insightsRdbms, tableExistsQuery);
			tableExists = wrapper.hasNext();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		if(!tableExists) {
			// well, you already created the file
			// need to run the queries to make this
			ProjectUtils.runInsightCreateTableQueries(insightsRdbms);
		} else {
			
			// adding new insight metadata
			try {
				if(!queryUtil.tableExists(insightsRdbms.getConnection(), "INSIGHTMETA", insightsRdbms.getSchema())) {
					String[] columns = new String[] { "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"};
					String[] types = new String[] { "VARCHAR(255)", "VARCHAR(255)", queryUtil.getClobDataTypeName(), "INT"};
					try {
						insightsRdbms.insertData(queryUtil.createTable("INSIGHTMETA", columns, types));
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			{
				List<String> allCols;
				try {
					allCols = queryUtil.getTableColumns(insightsRdbms.getConnection(), "QUESTION_ID", insightsRdbms.getSchema());
					// this should return in all upper case
					// ... but sometimes it is not -_- i.e. postgres always lowercases
					// TEMPORARY CHECK! - added 01/29/2022
					if(!allCols.contains("CACHE_MINUTES") && !allCols.contains("cache_minutes")) {
						if(queryUtil.allowIfExistsModifyColumnSyntax()) {
							insightsRdbms.insertData(queryUtil.alterTableAddColumnIfNotExists("QUESTION_ID", "CACHE_MINUTES", "INT"));
						} else {
							insightsRdbms.insertData(queryUtil.alterTableAddColumn("QUESTION_ID", "CACHE_MINUTES", "INT"));
						}
					}
					// TEMPORARY CHECK! - added 02/07/2022
					if(!allCols.contains("CACHE_ENCRYPT") && !allCols.contains("cache_encrypt")) {
						if(queryUtil.allowIfExistsModifyColumnSyntax()) {
							insightsRdbms.insertData(queryUtil.alterTableAddColumnIfNotExists("QUESTION_ID", "CACHE_ENCRYPT", queryUtil.getBooleanDataTypeName()));
						} else {
							insightsRdbms.insertData(queryUtil.alterTableAddColumn("QUESTION_ID", "CACHE_ENCRYPT", queryUtil.getBooleanDataTypeName()));
						}
					}
				} catch (SQLException e) {
					e.printStackTrace();
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
//						e.printStackTrace();
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
//				e.printStackTrace();
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
		
		insightsRdbms.setBasic(true);
		return insightsRdbms;
	}
	
}
