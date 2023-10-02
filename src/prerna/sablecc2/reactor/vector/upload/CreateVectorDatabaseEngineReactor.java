package prerna.sablecc2.reactor.vector.upload;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.upload.UploadUtilities;

public class CreateVectorDatabaseEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateVectorDatabaseEngineReactor.class);
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	public CreateVectorDatabaseEngineReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONNECTION_DETAILS.getKey(), "filePaths"};
		this.keyRequired = new int[] {1, 1, 0};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create a model engine", PixelDataType.CONST_STRING,
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

		organizeKeys();
		
		String vectorDbName = getVectorDatabaseName();
		Map<String, String> vectorDbDetails = getVectorDatabaseDetails();
		if (!vectorDbDetails.containsKey("vectorDbDetails")) {
			vectorDbDetails.put("INDEX_CLASSES", "default");
		}
		List<String> filePaths = getFiles();
		
		String vectorDbTypeStr = vectorDbDetails.get(IVectorDatabaseEngine.VECTOR_TYPE);
		
		if(vectorDbTypeStr == null || (vectorDbTypeStr=vectorDbTypeStr.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the model type");
		}
		VectorDatabaseTypeEnum vectorDbType = null;
		try {
			vectorDbType = VectorDatabaseTypeEnum.getEnumFromName(vectorDbTypeStr);
		} catch(Exception e) {
			throw new IllegalArgumentException("Invalid model type " + vectorDbTypeStr);
		}
		
		String vectorDbId = UUID.randomUUID().toString();
		File tempSmss = null;
		File smssFile = null;
		File specificEngineFolder = null;
		IVectorDatabaseEngine vectorDb = null;
		try {
			// validate engine
			UploadUtilities.validateEngine(IEngine.CATALOG_TYPE.VECTOR, user, vectorDbName, vectorDbId);
			specificEngineFolder = UploadUtilities.generateSpecificEngineFolder(IEngine.CATALOG_TYPE.VECTOR, vectorDbName, vectorDbId);
			
			String vectorDbClass = vectorDbType.getVectorDatabaseClass();
			vectorDb = (IVectorDatabaseEngine) Class.forName(vectorDbClass).newInstance();
			tempSmss = UploadUtilities.createTemporaryVectorDatabaseSmss(vectorDbId, vectorDbName, vectorDbClass, vectorDbDetails);
			
			// store in DIHelper so that when we move temp smss to smss it doesn't try to reload again
			DIHelper.getInstance().setEngineProperty(vectorDbId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
			vectorDb.open(tempSmss.getAbsolutePath());
			
			smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(tempSmss, smssFile);
			tempSmss.delete();
			vectorDb.setSmssFilePath(smssFile.getAbsolutePath());
			UploadUtilities.updateDIHelper(vectorDbId, vectorDbName, vectorDb, smssFile);
			SecurityEngineUtils.addEngine(vectorDbId, false, user);
			
			// even if no security, just add user as database owner
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider ap : logins) {
					SecurityEngineUtils.addEngineOwner(vectorDbId, user.getAccessToken(ap).getId());
				}
			}
			ClusterUtil.pushEngine(vectorDbId);
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			cleanUpCreateNewError(vectorDb, vectorDbId, tempSmss, smssFile, specificEngineFolder);
		}
		
		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), vectorDbId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	/**
	 * Delete all the corresponding files that are generated from the upload the failed
	 */
	private void cleanUpCreateNewError(IVectorDatabaseEngine vectorEngine, String modelId, File tempSmss, File smssFile, File specificEngineFolder) {
		try {
			// close the DB so we can delete it
			if (vectorEngine != null) {
				vectorEngine.close();
			}
			// delete the .temp file
			if (tempSmss != null && tempSmss.exists()) {
				FileUtils.forceDelete(tempSmss);
			}
			// delete the .smss file
			if (smssFile != null && smssFile.exists()) {
				FileUtils.forceDelete(smssFile);
			}
			// delete the engine folder
			if (specificEngineFolder != null && specificEngineFolder.exists()) {
				FileUtils.forceDelete(specificEngineFolder);
			}
			
			UploadUtilities.removeEngineFromDIHelper(modelId);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * 
	 * @return
	 */
	private String getVectorDatabaseName() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.DATABASE.getKey());
		if(grs != null && !grs.isEmpty()) {
			List<String> strValues = grs.getAllStrValues();
			if(strValues != null && !strValues.isEmpty()) {
				return strValues.get(0).trim();
			}
		}
		
		List<String> strValues = this.curRow.getAllStrValues();
		if(strValues != null && !strValues.isEmpty()) {
			return strValues.get(0).trim();
		}
		
		throw new NullPointerException("Must define the name of the new vector database engine");
	}
	
	/**
	 * 
	 * @return
	 */
	private Map<String, String> getVectorDatabaseDetails() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.CONNECTION_DETAILS.getKey());
		if(grs != null && !grs.isEmpty()) {
			List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
			if(mapNouns != null && !mapNouns.isEmpty()) {
				return (Map<String, String>) mapNouns.get(0).getValue();
			}
		}
		
		List<NounMetadata> mapNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapNouns != null && !mapNouns.isEmpty()) {
			return (Map<String, String>) mapNouns.get(0).getValue();
		}
		
		throw new NullPointerException("Must define the properties for the new vector database engine");
	}

	/**
	 * Get inputs
	 * @return list of engines to delete
	 */
	public List<String> getFiles() {
		List<String> filePaths = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[2]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				filePaths.add(grs.get(i).toString());
			}
			return filePaths;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			filePaths.add(this.curRow.get(i).toString());
		}
		return filePaths;
	}
}
