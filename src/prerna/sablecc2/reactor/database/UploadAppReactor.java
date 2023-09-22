package prerna.sablecc2.reactor.database;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

/**
 * Deprecating this file as the functionality has been divided into two new classes,
 * UploadDatabaseReactor and UploadProjectReactor
 *
 */

@Deprecated
public class UploadAppReactor extends AbstractInsightReactor {
	
	private static final String CLASS_NAME = UploadAppReactor.class.getName();

	public UploadAppReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = this.getLogger(CLASS_NAME);
		int step = 1;
		String zipFilePath = UploadInputUtility.getFilePath(this.store, this.insight);
		// check security
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create or update an app",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		// throw error is user doesn't have rights to publish new apps
		if (AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(user)) {
			throwUserNotPublisherError();
		}
		
		if (AbstractSecurityUtils.adminOnlyEngineAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		// creating a temp folder to unzip db folder and smss
		String temporaryAppId = UUID.randomUUID().toString();
		String dbFolderPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "db";
		String tempDbFolderPath = dbFolderPath + DIR_SEPARATOR + temporaryAppId;
		File tempDbFolder = new File(tempDbFolderPath);

		// gotta keep track of the smssFile and files unzipped
		Map<String, List<String>> filesAdded = new HashMap<>();
		List<String> fileList = new Vector<>();
		String smssFileLoc = null;
		File smssFile = null;
		// unzip files to temp db folder
		boolean error = false;
		try {
			logger.info(step + ") Unzipping app");
			filesAdded = ZipUtils.unzip(zipFilePath, tempDbFolderPath);
			logger.info(step + ") Done");
			step++;
			
			// look for smss file
			fileList = filesAdded.get("FILE");
			logger.info(step + ") Searching for smss");
			for (String filePath : fileList) {
				if (filePath.endsWith(Constants.SEMOSS_EXTENSION)) {
					smssFileLoc = tempDbFolderPath + DIR_SEPARATOR + filePath;
					smssFile = new File(Utility.normalizePath(smssFileLoc));
					// check if the file exists
					if (!smssFile.exists()) {
						// invalid file need to delete the files unzipped
						smssFileLoc = null;
					}
					break;
				}
			}
			logger.info(step + ") Done");
			step++;


			// delete the files if we were unable to find the smss file
			if (smssFileLoc == null) {
				throw new SemossPixelException("Unable to find " + Constants.SEMOSS_EXTENSION + " file", false);
			}
		} catch (SemossPixelException e) {
			error = true;
			throw e;
		} catch (Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error occurred while unzipping the files", false);
		} finally {
			if(error) {
				cleanUpFolders(null, null, null, null, tempDbFolder, logger);
			}
		}

		String engines = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		String appId = null;
		String appName = null;
		File tempSmss = null;
		File tempEngFolder = null;
		File finalSmss = null;
		File finalEngFolder = null;
		
		try {
			logger.info(step + ") Reading smss");
			Properties prop = Utility.loadProperties(smssFileLoc);
			appId = prop.getProperty(Constants.ENGINE);
			appName = prop.getProperty(Constants.ENGINE_ALIAS);
			logger.info(step + ") Done");
			step++;

			// zip file has the smss and db folder on the same level
			// need to move these files around
			String oldDbFolderPath = tempDbFolder + DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId);
			tempEngFolder = new File(Utility.normalizePath(oldDbFolderPath));
			finalEngFolder = new File(Utility.normalizePath(dbFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId)));
			finalSmss = new File(Utility.normalizePath(dbFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId) + Constants.SEMOSS_EXTENSION));

			// need to ignore file watcher
			if (!(engines.startsWith(appId) || engines.contains(";" + appId + ";") || engines.endsWith(";" + appId))) {
				String newEngines = engines + ";" + appId;
				DIHelper.getInstance().setLocalProperty(Constants.ENGINES, newEngines);
			} else {
				SemossPixelException exception = new SemossPixelException(
						NounMetadata.getErrorNounMessage("App ID already exists"));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
			// move database folder
			logger.info(step + ") Moving app folder");
			FileUtils.copyDirectory(tempEngFolder, finalEngFolder);
			logger.info(step + ") Done");
			step++;

			// move smss file
			logger.info(step + ") Moving smss file");
			tempSmss = new File(Utility.normalizePath(tempDbFolder + DIR_SEPARATOR 
					+ SmssUtilities.getUniqueName(appName, appId) + Constants.SEMOSS_EXTENSION));
			FileUtils.copyFile(tempSmss, finalSmss);
			logger.info(step + ") Done");
			step++;
		} catch (Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage(), false);
		} finally {
			if(error) {
				DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engines);
				cleanUpFolders(tempSmss, finalSmss, tempEngFolder, finalEngFolder, tempDbFolder, logger);
			} else {
				// just delete the temp db folder
				cleanUpFolders(null, null, null, null, tempDbFolder, logger);
			}
		}

		try {
			DIHelper.getInstance().setEngineProperty(appId + "_" + Constants.STORE, finalSmss.getAbsolutePath());
			logger.info(step + ") Grabbing app structure");
			Utility.synchronizeEngineMetadata(appId);
			logger.info(step + ") Done");
			SecurityEngineUtils.addEngine(appId, false, user);
		} catch(Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error occurred trying to synchronize the metadata and insights for the zip file", false);
		} finally {
			if(error) {
				// delete all the resources
				cleanUpFolders(tempSmss, finalSmss, tempEngFolder, finalEngFolder, tempDbFolder, logger);
				// remove from DIHelper
				DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engines);
				// delete from local master
				DeleteFromMasterDB lmDeleter = new DeleteFromMasterDB();
				lmDeleter.deleteEngineRDBMS(appId);
				// delete from security
				SecurityEngineUtils.deleteEngine(appId);
			}
		}
		
		// even if no security, just add user as engine owner
		if (user != null) {
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityEngineUtils.addEngineOwner(appId, user.getAccessToken(ap).getId());
			}
		}

		ClusterUtil.pushEngine(appId);

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), appId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);	
	}
	
	/**
	 * Utility method to delete resources that have to be cleaned up
	 * @param tempSmss
	 * @param finalSmss
	 * @param tempEngDir
	 * @param finalEngDir
	 * @param tempDbDir
	 * @param logger
	 */
	private void cleanUpFolders(File tempSmss, File finalSmss, File tempEngDir, File finalEngDir, File tempDbDir, Logger logger) {
		if(tempSmss != null && tempSmss.exists()) {
			try {
				FileUtils.forceDelete(tempSmss);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		if(finalSmss != null && finalSmss.exists()) {
			try {
				FileUtils.forceDelete(finalSmss);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		if(tempEngDir != null && tempEngDir.exists()) {
			try {
				FileUtils.deleteDirectory(tempEngDir);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		if(finalEngDir != null && finalEngDir.exists()) {
			try {
				FileUtils.deleteDirectory(finalEngDir);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		if(tempDbDir != null && tempDbDir.exists()) {
			try {
				FileUtils.deleteDirectory(tempDbDir);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
	}

}
