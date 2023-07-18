package prerna.sablecc2.reactor.storage;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IStorage;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;

public class PushToStorageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PushToStorageReactor.class);
	
	public PushToStorageReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(), 
				ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		IStorage storage = getStorage();
		String storageFolderPath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
		String fileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
		if(!new File(fileLocation).exists()) {
			throw new IllegalArgumentException("Unable to locate file");
		}
		
		try {
			storage.copyToStorage(fileLocation, storageFolderPath);
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred uploading local file to storage");
		}
	}
	
	private IStorage getStorage() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.STORAGE.getKey());
		if(grs != null && !grs.isEmpty()) {
			return (IStorage) grs.get(0);
		}
		
		List<NounMetadata> storageInputs = this.curRow.getNounsOfType(PixelDataType.STORAGE);
		if(storageInputs != null && !storageInputs.isEmpty()) {
			return (IStorage) storageInputs.get(0).getValue();
		}
		
		throw new NullPointerException("No storage engine defined");
	}

}
