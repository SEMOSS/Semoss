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
import java.util.*;

public class PushToStorageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PushToStorageReactor.class);
	
	public PushToStorageReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(), 
				ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.METADATA.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
	    organizeKeys();
	    IStorageEngine storage = getStorage();

	    // Check permission
	    if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), storage.getEngineId())) {
	        throw new IllegalArgumentException("User does not have permission to push into the remote storage");
	    }

	    String storageFolderPath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());

	    // Get multiple file paths
	    List<String> fileLocations = UploadInputUtility.getFilePaths(this.store, this.insight);
	    Map<String, Object> metadata = getMetadata();

	    try {
	    	List<String> failedFiles = new ArrayList<>();
	        for (String fileLocation : fileLocations) {
	            fileLocation = Utility.normalizePath(fileLocation);
	            if (!new File(fileLocation).exists()) {
	            	failedFiles.add(fileLocation);
					classLogger.error("Failed to upload file: " + fileLocation);
	            }
	            storage.copyToStorage(fileLocation, storageFolderPath, metadata);
	        }
	        return new NounMetadata(failedFiles, PixelDataType.VECTOR);
	    } catch (Exception e) {
	        classLogger.error(Constants.STACKTRACE, e);
	        throw new IllegalArgumentException("Error occurred uploading local files to storage");
	    }
	}

	
	private IStorageEngine getStorage() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.STORAGE.getKey());
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
        GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.METADATA.getKey());
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

}
