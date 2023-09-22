package prerna.sablecc2.reactor.storage.upload;

import java.io.File;
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
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.StorageTypeEnum;
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

public class CreateStorageEngineReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateStorageEngineReactor.class);

	public CreateStorageEngineReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_DETAILS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account in order to create a storage engine", PixelDataType.CONST_STRING,
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
		
		String storageName = getStorageName();
		Map<String, String> storageDetails = getStorageDetails();
		String storageTypeStr = storageDetails.get(IStorageEngine.STORAGE_TYPE);
		if(storageTypeStr == null || (storageTypeStr=storageTypeStr.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the storage type");
		}
		StorageTypeEnum storageType = null;
		try {
			storageType = StorageTypeEnum.getEnumFromName(storageTypeStr);
		} catch(Exception e) {
			throw new IllegalArgumentException("Invalid storage type " + storageTypeStr);
		}
		
		String storageId = UUID.randomUUID().toString();
		File tempSmss = null;
		File smssFile = null;
		IStorageEngine storage = null;
		try {
			String storageClass = storageType.getStorageClass();
			storage = (IStorageEngine) Class.forName(storageClass).newInstance();
			tempSmss = UploadUtilities.createTemporaryStorageSmss(storageId, storageName, storageClass, storageDetails);

			// store in DIHelper so that when we move temp smss to smss it doesn't try to reload again
			DIHelper.getInstance().setEngineProperty(storageId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
			storage.open(tempSmss.getAbsolutePath());			
			
			smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
			FileUtils.copyFile(tempSmss, smssFile);
			tempSmss.delete();
			storage.setSmssFilePath(smssFile.getAbsolutePath());
			UploadUtilities.updateDIHelper(storageId, storageName, storage, smssFile);
			SecurityEngineUtils.addEngine(storageId, false, user);
			
			// even if no security, just add user as database owner
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider ap : logins) {
					SecurityEngineUtils.addEngineOwner(storageId, user.getAccessToken(ap).getId());
				}
			}
			
			ClusterUtil.pushEngine(storageId);
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			cleanUpCreateNewError(storage, storageId, tempSmss, smssFile);
		}
		
		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), storageId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
	}
	
	/**
	 * Delete all the corresponding files that are generated from the upload the failed
	 */
	private void cleanUpCreateNewError(IStorageEngine storage, String storageId, File tempSmss, File smssFile) {
		try {
			// close the DB so we can delete it
			if (storage != null) {
				storage.close();
			}

			// delete the .temp file
			if (tempSmss != null && tempSmss.exists()) {
				FileUtils.forceDelete(tempSmss);
			}
			// delete the .smss file
			if (smssFile != null && smssFile.exists()) {
				FileUtils.forceDelete(smssFile);
			}
			
			UploadUtilities.removeEngineFromDIHelper(storageId);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * 
	 * @return
	 */
	private String getStorageName() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.STORAGE.getKey());
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
		
		throw new NullPointerException("Must define the name of the new storage engine");
	}
	
	/**
	 * 
	 * @return
	 */
	private Map<String, String> getStorageDetails() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.STORAGE_DETAILS.getKey());
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
		
		throw new NullPointerException("Must define the properties for the new storage engine");
	}

}
