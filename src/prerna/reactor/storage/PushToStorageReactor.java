package prerna.reactor.storage;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IStorageEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class PushToStorageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PushToStorageReactor.class);
	
	public PushToStorageReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(), 
				ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.METADATA.getKey()};
		this.keyRequired = new int[] {0, 0, 1, 0, 1, 0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		IStorageEngine storage = getStorage();
		// check that the user can edit the engine
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), storage.getEngineId())) {
			throw new IllegalArgumentException("User does not have permission to push into the remote storage");
		}
		String storageFolderPath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
		String fileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
		if(!new File(fileLocation).exists()) {
			throw new IllegalArgumentException("Unable to locate file");
		}
		
		Map<String, Object> metadata = getMetadata();
		try {
			storage.copyToStorage(fileLocation, storageFolderPath, metadata);
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred uploading local file to storage");
		}
	}
	
	private IStorageEngine getStorage() {

		String storageEngineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (storageEngineId != null && !storageEngineId.isEmpty()) {
			IStorageEngine storage = (IStorageEngine) Utility.getStorage(storageEngineId);
			if (storage == null) {
				throw new NullPointerException("No storage engine found with id " + storageEngineId);
			}
			return storage;
		}

		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.STORAGE.getKey());
		if(grs != null && !grs.isEmpty()) {
			return (IStorageEngine) grs.get(0);
		}
		
		List<NounMetadata> storageInputs = this.curRow.getNounsOfType(PixelDataType.STORAGE);
		if(storageInputs != null && !storageInputs.isEmpty()) {
			return (IStorageEngine) storageInputs.get(0).getValue();
		}
		
		throw new NullPointerException("No storage engine defined");
	}
	
	private Map<String, Object> getMetadata() {
        GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.METADATA.getKey());
        if(mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if(mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if(mapInputs != null && !mapInputs.isEmpty()) {
            return (Map<String, Object>) mapInputs.get(0).getValue();
        }
        return null;
    }

	@Override
	public String getReactorDescription() {
		return "Push files from a local path to a storage path";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The storage engine is to use";
		} else if(key.equals(ReactorKeysEnum.STORAGE.getKey())) {
			return "The storage engine instance";
		} else if(key.equals(ReactorKeysEnum.STORAGE_PATH.getKey())) {
			return "The storage path to upload files to";
		} else if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The local path(s) to upload from";
		}
		return super.getDescriptionForKey(key);
	}

}
