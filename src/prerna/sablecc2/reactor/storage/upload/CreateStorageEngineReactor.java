package prerna.sablecc2.reactor.storage.upload;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IStorage;
import prerna.engine.api.StorageTypeEnum;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.upload.UploadUtilities;

public class CreateStorageEngineReactor extends AbstractReactor {

	public CreateStorageEngineReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_DETAILS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		User user = null;
		boolean security = AbstractSecurityUtils.securityEnabled();
		if (security) {
			user = this.insight.getUser();
			if (user == null) {
				NounMetadata noun = new NounMetadata(
						"User must be signed into an account in order to create a database", PixelDataType.CONST_STRING,
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
		}

		organizeKeys();
		
		String storageName = getStorageName();
		Map<String, String> storageDetails = getSmssDetails();
		String storageTypeStr = storageDetails.get(IStorage.STORAGE_NAME);
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
		String smssFile = null;
		IStorage storage = null;
		try {
			String storageClass = storageType.getStorageClass();
			storage = (IStorage) Class.forName(storageClass).newInstance();
			
			UploadUtilities.createTemporaryStorageSmss(storageId, storageName, storageClass, storageDetails);
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}
//		UploadUtilities.updateDIHelper(storageId, storageName, storageId, smssFile);
		
		ClusterUtil.reactorPushDatabase(storageId);

		Map<String, Object> retMap = UploadUtilities.getEngineReturnData(this.insight.getUser(), storageId);
		return new NounMetadata(retMap, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.MARKET_PLACE_ADDITION);
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
	private Map<String, String> getSmssDetails() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.STORAGE.getKey());
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
