package prerna.reactor.engine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.reflect.TypeToken;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.DeleteFromMasterDB;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import prerna.util.gson.GsonUtility;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

public class UploadEngineReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(UploadEngineReactor.class);

	private static final String CLASS_NAME = UploadEngineReactor.class.getName();

	public UploadEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.GLOBAL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = this.getLogger(CLASS_NAME);
		int step = 1;
		String zipFilePath = UploadInputUtility.getFilePath(this.store, this.insight);
		// do we want this project to be accessible to everyone
		boolean global = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.GLOBAL.getKey())+"");
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

		// creating a temp folder to unzip the engine folder and smss
		String randomIdAsDir = UUID.randomUUID().toString();
		String randomTempUnzipFolderPath = this.insight.getInsightFolder() + DIR_SEPARATOR + randomIdAsDir;
		File randomTempUnzipF = new File(randomTempUnzipFolderPath);

		// gotta keep track of the smssFile and files unzipped
		Map<String, List<String>> filesAdded = new HashMap<>();
		List<String> fileList = new ArrayList<>();
		String smssFileLoc = null;
		File smssFile = null;
		// unzip files to temp db folder
		boolean error = false;
		try {
			logger.info(step + ") Unzipping engine");
			filesAdded = ZipUtils.unzip(zipFilePath, randomTempUnzipFolderPath);
			logger.info(step + ") Done");
			step++;
			
			// look for smss file
			fileList = filesAdded.get("FILE");
			logger.info(step + ") Searching for smss");
			for (String filePath : fileList) {
				if (filePath.endsWith(Constants.SEMOSS_EXTENSION)) {
					smssFileLoc = randomTempUnzipFolderPath + DIR_SEPARATOR + filePath;
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
				cleanUpFolders(randomTempUnzipF);
			}
		}
		
		// need to know which type of Engine we are using
		Properties prop = Utility.loadProperties(smssFileLoc);
		logger.info(step + ") Reading smss");
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
		Object[] typeAndSubtypeAndCost = SecurityEngineUtils.getEngineTypeAndSubTypeAndCost(prop);
		IEngine.CATALOG_TYPE engineType = (IEngine.CATALOG_TYPE) typeAndSubtypeAndCost[0];
		logger.info(step + ") Done");
		step++;

		// now we have the path we want to move the unzipped folder and smss to
		String engineFolderPath = EngineUtility.getLocalEngineBaseDirectory(engineType);
		logger.info("Determined the engine type = " + engineType);

		String engines = (String) DIHelper.getInstance().getEngineProperty(Constants.ENGINES);
		
		File finalEngineSmss = null;
		File finalEngineFolder = null;
		try {
			// zip file has the smss and db folder on the same level
			// need to move these files around
			File tempUnzippedEngineF = new File(Utility.normalizePath(randomTempUnzipF + DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId)));
			finalEngineFolder = new File(Utility.normalizePath(engineFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId)));
			finalEngineSmss = new File(Utility.normalizePath(engineFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId) + Constants.SEMOSS_EXTENSION));

			// need to ignore file watcher
			if (!(engines.startsWith(engineId) || engines.contains(";" + engineId + ";") || engines.endsWith(";" + engineId))) {
				String newEngines = engines + ";" + engineId;
				DIHelper.getInstance().setEngineProperty(Constants.ENGINES, newEngines);
			} else {
				SemossPixelException exception = new SemossPixelException(
						NounMetadata.getErrorNounMessage("Engine id already exists"));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
			// move engine folder
			logger.info(step + ") Moving engine folder");
			FileUtils.copyDirectory(tempUnzippedEngineF, finalEngineFolder);
			logger.info(step + ") Done");
			step++;

			// move smss file
			logger.info(step + ") Moving smss file");
			File tempUnzippedSmssF = new File(Utility.normalizePath(randomTempUnzipF + DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId) + Constants.SEMOSS_EXTENSION));
			FileUtils.copyFile(tempUnzippedSmssF, finalEngineSmss);
			logger.info(step + ") Done");
			step++;
		} catch (Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage(), false);
		} finally {
			if(error) {
				// remove from DIHelper
				UploadUtilities.removeEngineFromDIHelper(engineId);
				cleanUpFolders(randomTempUnzipF, finalEngineSmss, finalEngineFolder);
			} else {
				// just delete the temp db folder
				cleanUpFolders(randomTempUnzipF);
			}
		}

		try {
			DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.STORE, finalEngineSmss.getAbsolutePath());
			if(IEngine.CATALOG_TYPE.DATABASE == engineType) {
				logger.info(step + ") Synchronizing database structure");
				Utility.synchronizeEngineMetadata(engineId);
				logger.info(step + ") Done");
				step++;
			}
			logger.info(step + ") Synchronizing the engine metadata");
			SecurityEngineUtils.addEngine(engineId, global, user);
			logger.info(step + ") Done");
			step++;
			
			// see if we have any dependencies or metadata to load
			{
				File metadataFile = new File(finalEngineFolder.getAbsolutePath() + "/" + engineName + IEngine.METADATA_FILE_SUFFIX);
				if(metadataFile.exists() && metadataFile.isFile()) {
					Map<String, Object> metadata = (Map<String, Object>) GsonUtility.readJsonFileToObject(metadataFile, new TypeToken<Map<String, Object>>() {}.getType());
					SecurityEngineUtils.updateEngineMetadata(engineId, metadata);
					// delete this file since values can update and file is dynamically generated on export
					metadataFile.delete();
				}
			}
		} catch(Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error occurred trying to synchronize the metadata for the zip file", false);
		} finally {
			if(error) {
				// delete all the resources
				cleanUpFolders(randomTempUnzipF, finalEngineSmss, finalEngineFolder);
				// remove from DIHelper
				UploadUtilities.removeEngineFromDIHelper(engineId);
				if(IEngine.CATALOG_TYPE.DATABASE == engineType) {
					// delete from local master
					DeleteFromMasterDB lmDeleter = new DeleteFromMasterDB();
					lmDeleter.deleteEngineRDBMS(engineId);
				}
				// delete from security
				SecurityEngineUtils.deleteEngine(engineId);
			}
		}
		
		// add user as engine owner
		List<AuthProvider> logins = user.getLogins();
		for (AuthProvider ap : logins) {
			SecurityEngineUtils.addEngineOwner(engineId, user.getAccessToken(ap).getId());
		}

		ClusterUtil.pushEngine(engineId);

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), engineId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);	
	}
	
	/**
	 * 
	 * @param fileToDelete
	 */
	private void cleanUpFolders(File... fileToDelete) {
		for(File f : fileToDelete) {
			if(f != null && f.exists()) {
				try {
					FileUtils.forceDelete(f);
				} catch (IOException e) {
					classLogger.warn("Error on clean up attempting to delete " + f.getAbsolutePath());
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

}
