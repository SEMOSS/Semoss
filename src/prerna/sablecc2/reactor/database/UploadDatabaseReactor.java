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


public class UploadDatabaseReactor extends AbstractInsightReactor {
	
	private static final String CLASS_NAME = UploadDatabaseReactor.class.getName();

	public UploadDatabaseReactor() {
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
		boolean security = AbstractSecurityUtils.securityEnabled();
		if (security) {
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
			
			if (AbstractSecurityUtils.adminOnlyDbAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
				throwFunctionalityOnlyExposedForAdminsError();
			}
		}

		// creating a temp folder to unzip db folder and smss
		String temporaryAppId = UUID.randomUUID().toString();
		String dbFolderPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + Constants.DATABASE_FOLDER;
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

		//String engines = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		String engines = (String) DIHelper.getInstance().getEngineProperty(Constants.ENGINES);
		String databaseId = null;
		String databseName = null;
		File tempSmss = null;
		File tempEngFolder = null;
		File finalSmss = null;
		File finalEngFolder = null;
		
		try {
			logger.info(step + ") Reading smss");
			Properties prop = Utility.loadProperties(smssFileLoc);
			databaseId = prop.getProperty(Constants.ENGINE);
			databseName = prop.getProperty(Constants.ENGINE_ALIAS);
			logger.info(step + ") Done");
			step++;

			// zip file has the smss and db folder on the same level
			// need to move these files around
			String oldDbFolderPath = tempDbFolder + DIR_SEPARATOR + SmssUtilities.getUniqueName(databseName, databaseId);
			tempEngFolder = new File(Utility.normalizePath(oldDbFolderPath));
			finalEngFolder = new File(Utility.normalizePath(dbFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(databseName, databaseId)));
			finalSmss = new File(Utility.normalizePath(dbFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(databseName, databaseId) + Constants.SEMOSS_EXTENSION));

			// need to ignore file watcher
			if (!(engines.startsWith(databaseId) || engines.contains(";" + databaseId + ";") || engines.endsWith(";" + databaseId))) {
				String newEngines = engines + ";" + databaseId;
				DIHelper.getInstance().setEngineProperty(Constants.ENGINES, newEngines);
			} else {
				SemossPixelException exception = new SemossPixelException(
						NounMetadata.getErrorNounMessage("Database id already exists"));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
			// move database folder
			logger.info(step + ") Moving database folder");
			FileUtils.copyDirectory(tempEngFolder, finalEngFolder);
			logger.info(step + ") Done");
			step++;

			// move smss file
			logger.info(step + ") Moving smss file");
			tempSmss = new File(Utility.normalizePath(tempDbFolder + DIR_SEPARATOR 
					+ SmssUtilities.getUniqueName(databseName, databaseId) + Constants.SEMOSS_EXTENSION));
			FileUtils.copyFile(tempSmss, finalSmss);
			logger.info(step + ") Done");
			step++;
		} catch (Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage(), false);
		} finally {
			if(error) {
				DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engines);
				cleanUpFolders(tempSmss, finalSmss, tempEngFolder, finalEngFolder, tempDbFolder, logger);
			} else {
				// just delete the temp db folder
				cleanUpFolders(null, null, null, null, tempDbFolder, logger);
			}
		}

		try {
			DIHelper.getInstance().setEngineProperty(databaseId + "_" + Constants.STORE, finalSmss.getAbsolutePath());
			logger.info(step + ") Grabbing database structure");
			Utility.synchronizeEngineMetadata(databaseId);
			SecurityEngineUtils.addEngine(databaseId, !AbstractSecurityUtils.securityEnabled(), user);
			logger.info(step + ") Done");
		} catch(Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error occurred trying to synchronize the metadata and insights for the zip file", false);
		} finally {
			if(error) {
				// delete all the resources
				cleanUpFolders(tempSmss, finalSmss, tempEngFolder, finalEngFolder, tempDbFolder, logger);
				// remove from DIHelper
				DIHelper.getInstance().setEngineProperty(Constants.ENGINES, engines);
				// delete from local master
				DeleteFromMasterDB lmDeleter = new DeleteFromMasterDB();
				lmDeleter.deleteEngineRDBMS(databaseId);
				// delete from security
				SecurityEngineUtils.deleteDatabase(databaseId);
			}
		}
		
		// even if no security, just add user as engine owner
		if (user != null) {
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider ap : logins) {
				SecurityEngineUtils.addDatabaseOwner(databaseId, user.getAccessToken(ap).getId());
			}
		}

		ClusterUtil.reactorPushDatabase(databaseId);

		Map<String, Object> retMap = UploadUtilities.getDatabaseReturnData(this.insight.getUser(), databaseId);
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
