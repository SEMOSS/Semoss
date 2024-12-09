package prerna.reactor.vector.upload;

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
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.StorageTypeEnum;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.vector.OpenSearchRestVectorDatabaseEngine;
import prerna.masterdatabase.DeleteFromMasterDB;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.UploadInputUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;
import prerna.util.ZipUtils;
import prerna.util.gson.GsonUtility;

public class UploadVectorDatabaseEngineReactor extends AbstractReactor{

	private static final Logger classLogger = LogManager.getLogger(UploadVectorDatabaseEngineReactor.class);
	
	public UploadVectorDatabaseEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.GLOBAL.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to upload a vector engine", PixelDataType.CONST_STRING,
					PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		if (AbstractSecurityUtils.anonymousUsersEnabled()) {
			if (this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
		}

		// throw error is user doesn't have rights to publish new databases
		if (AbstractSecurityUtils.adminSetPublisher()
				&& !SecurityQueryUtils.userIsPublisher(this.insight.getUser())) {
			throwUserNotPublisherError();
		}

		if (AbstractSecurityUtils.adminOnlyEngineAdd() && !SecurityAdminUtils.userIsAdmin(user)) {
			throwFunctionalityOnlyExposedForAdminsError();
		}

		int step = 1;
		String zipFilePath = UploadInputUtility.getFilePath(this.store, this.insight);
		// do we want this project to be accessible to everyone
		boolean global = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.GLOBAL.getKey())+"");
		
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
			classLogger.info(step + ") Unzipping engine");
			filesAdded = ZipUtils.unzip(zipFilePath, randomTempUnzipFolderPath);
			classLogger.info(step + ") Done");
			step++;

			// look for smss file
			fileList = filesAdded.get("FILE");
			classLogger.info(step + ") Searching for smss");
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
			classLogger.info(step + ") Done");
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
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error occurred while unzipping the files", false);
		} finally {
			if (error) {
				cleanUpFolders(randomTempUnzipF);
			}
		}

		// need to know which type of Engine we are using
		Properties prop = Utility.loadProperties(smssFileLoc);
		classLogger.info(step + ") Reading smss");
		String engineId = prop.getProperty(Constants.ENGINE);
		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
		Object[] typeAndSubtypeAndCost = SecurityEngineUtils.getEngineTypeAndSubTypeAndCost(prop);
		IEngine.CATALOG_TYPE engineType = (IEngine.CATALOG_TYPE) typeAndSubtypeAndCost[0];
		VectorDatabaseTypeEnum engineSubType = VectorDatabaseTypeEnum.getEnumFromName((String)typeAndSubtypeAndCost[1]);
		
		if (engineType != IEngine.CATALOG_TYPE.VECTOR) {
			throw new IllegalArgumentException("Expecting a VECTOR engine but was " + engineType);
		}
		
		if(engineSubType == VectorDatabaseTypeEnum.OPEN_SEARCH) {
			if(prop.getProperty(Constants.USERNAME) == null) { throw new IllegalArgumentException(Constants.USERNAME + " is not provided."); }
			if(prop.getProperty(Constants.PASSWORD) == null) { throw new IllegalArgumentException(Constants.PASSWORD + " is not provided."); }
			if(prop.getProperty(Constants.HOSTNAME) == null) { throw new IllegalArgumentException(Constants.HOSTNAME + " is not provided."); }
			if(prop.getProperty(OpenSearchRestVectorDatabaseEngine.INDEX_NAME) == null) { throw new IllegalArgumentException(OpenSearchRestVectorDatabaseEngine.INDEX_NAME + " is not provided."); }
		}
		if(engineSubType == VectorDatabaseTypeEnum.WEAVIATE) {
			if(prop.getProperty(Constants.API_KEY) == null) { throw new IllegalArgumentException(Constants.API_KEY + " is not provided."); }
			if(prop.getProperty(Constants.HOSTNAME) == null) { throw new IllegalArgumentException(Constants.HOSTNAME + " is not provided."); }
		}
		
		if (prop.getProperty(Constants.INDEX_CLASSES) == null) {
			throw new IllegalArgumentException(Constants.INDEX_CLASSES + " is not provided.");
		}
		
		classLogger.info(step + ") Done");
		step++;
		
		// extract the embedding engine from the smss
		classLogger.info(step + ") Retrieving Embedder Engine");
		String embedderEngineId = prop.getProperty(Constants.EMBEDDER_ENGINE_ID);
		if (embedderEngineId == null) {
			throw new IllegalArgumentException("EMBEDDER_ENGINE_ID must be defined for FAISS database");
		}	
		IModelEngine embeddingModel = Utility.getModel(embedderEngineId);
		if(embeddingModel == null) {
			throw new IllegalArgumentException("EMBEDDER_ENGINE_ID " + embedderEngineId + " could not be found");
		}
		classLogger.info(step + ") Done");
		step++;
		

		// now we have the path we want to move the unzipped folder and smss to
		String engineFolderPath = EngineUtility.getLocalEngineBaseDirectory(engineType);
		classLogger.info("Determined the engine type = " + engineType);

		String engines = (String) DIHelper.getInstance().getEngineProperty(Constants.ENGINES);

		File finalEngineSmss = null;
		File finalEngineFolder = null;
		try {
			// zip file has the smss and db folder on the same level
			// need to move these files around
			File tempUnzippedEngineF = new File(Utility.normalizePath(
					randomTempUnzipF + DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId)));
			finalEngineFolder = new File(Utility.normalizePath(
					engineFolderPath + DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId)));
			finalEngineSmss = new File(Utility.normalizePath(engineFolderPath + DIR_SEPARATOR
					+ SmssUtilities.getUniqueName(engineName, engineId) + Constants.SEMOSS_EXTENSION));

			// need to ignore file watcher
			if (!(engines.startsWith(engineId) || engines.contains(";" + engineId + ";")
					|| engines.endsWith(";" + engineId))) {
				String newEngines = engines + ";" + engineId;
				DIHelper.getInstance().setEngineProperty(Constants.ENGINES, newEngines);
			} else {
				SemossPixelException exception = new SemossPixelException(
						NounMetadata.getErrorNounMessage("Engine id already exists"));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
			// move engine folder
			classLogger.info(step + ") Moving engine folder");
			FileUtils.copyDirectory(tempUnzippedEngineF, finalEngineFolder);
			classLogger.info(step + ") Done");
			step++;

			// move smss file
			classLogger.info(step + ") Moving smss file");
			File tempUnzippedSmssF = new File(Utility.normalizePath(randomTempUnzipF + DIR_SEPARATOR
					+ SmssUtilities.getUniqueName(engineName, engineId) + Constants.SEMOSS_EXTENSION));
			FileUtils.copyFile(tempUnzippedSmssF, finalEngineSmss);
			classLogger.info(step + ") Done");
			step++;
		} catch (Exception e) {
			error = true;
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage(), false);
		} finally {
			if (error) {
				// remove from DIHelper
				UploadUtilities.removeEngineFromDIHelper(engineId);
				cleanUpFolders(randomTempUnzipF, finalEngineSmss, finalEngineFolder);
			} else {
				// just delete the temp db folder
				cleanUpFolders(randomTempUnzipF);
			}
		}

		try {
			DIHelper.getInstance().setEngineProperty(engineId + "_" + Constants.STORE, 
					finalEngineSmss.getAbsolutePath());
			classLogger.info(step + ") Synchronizing the engine metadata");
			SecurityEngineUtils.addEngine(engineId, global, user);
			classLogger.info(step + ") Done");
			step++;

			// see if we have any dependencies or metadata to load
			{
				File metadataFile = new File(Utility.normalizePath(
						finalEngineFolder.getAbsolutePath() + "/" + engineName + IEngine.METADATA_FILE_SUFFIX));
				if (metadataFile.exists() && metadataFile.isFile()) {
					Map<String, Object> metadata = (Map<String, Object>) GsonUtility.readJsonFileToObject(metadataFile,
							new TypeToken<Map<String, Object>>() {
							}.getType());
					SecurityEngineUtils.updateEngineMetadata(engineId, metadata);
					// delete this file since values can update and file is dynamically generated on
					// export
					metadataFile.delete();
				}
			}
		} catch (Exception e) {
			error = true;
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error occurred trying to synchronize the metadata for the zip file", false);
		} finally {
			if (error) {
				// delete all the resources
				cleanUpFolders(randomTempUnzipF, finalEngineSmss, finalEngineFolder);
				// remove from DIHelper
				UploadUtilities.removeEngineFromDIHelper(engineId);
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
