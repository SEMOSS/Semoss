package prerna.reactor.storage;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IStorageEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class PullFromStorageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PullFromStorageReactor.class);
	
	public PullFromStorageReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(), 
				ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
		this.keyRequired = new int[] {1, 1, 0, 1};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		IStorageEngine storage = getStorage();
		String storagePath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
		String fileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
		if(!(new File(fileLocation).isDirectory())) {
			new File(fileLocation).mkdirs();
		}
		
		try {
			storage.copyToLocal(storagePath, fileLocation);
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred downloading storage file to local");
		}
	}
	
	private IStorageEngine getStorage() {

		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.STORAGE.getKey());
		if(grs != null && !grs.isEmpty()) {
			IStorageEngine storage = null;
			if (grs.get(0) instanceof String) {
				storage = (IStorageEngine) Utility.getStorage((String) grs.get(0));
			} else {
				storage = (IStorageEngine) grs.get(0);
			}
			return storage;
		}
		
		List<NounMetadata> storageInputs = this.curRow.getNounsOfType(PixelDataType.STORAGE);
		if(storageInputs != null && !storageInputs.isEmpty()) {
			return (IStorageEngine) storageInputs.get(0).getValue();
		}
		
		throw new NullPointerException("No storage engine defined");
	}

	@Override
	public String getReactorDescription() {
		return "Pull files from a storage path to a local path";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.STORAGE.getKey())) {
			return "The storage engine instance or id";
		} else if(key.equals(ReactorKeysEnum.STORAGE_PATH.getKey())) {
			return "The storage path(s) to download from";
		} else if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The local path to download files to";
		}
		return super.getDescriptionForKey(key);
	}

}
