package prerna.reactor.storage;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IStorageEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class ListStoragePathDetailsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ListStoragePathDetailsReactor.class);
	
	public ListStoragePathDetailsReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		IStorageEngine storage = getStorage();
		String path = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
		try {
			List<Map<String, Object>> storageList = storage.listDetails(path);
			return new NounMetadata(storageList, PixelDataType.VECTOR);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error listing storage details at path " + path);
		}
	}
	
	private IStorageEngine getStorage() {
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

}
