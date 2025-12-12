package prerna.reactor.storage;

import java.util.List;

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
import prerna.util.Utility;

public class ListStoragePathReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ListStoragePathReactor.class);

	public ListStoragePathReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		IStorageEngine storage = getStorage();
		String path = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
		try {
			List<String> storageList = storage.list(path);
			return new NounMetadata(storageList, PixelDataType.CONST_STRING);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error listing storage details at path " + path);
		}
	}

	private IStorageEngine getStorage() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.STORAGE.getKey());
		if (grs != null && !grs.isEmpty()) {
			IStorageEngine storage = null;
			if (grs.get(0) instanceof String) {
				String storageId = (String) grs.get(0);
				if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), storageId)) {
					throw new IllegalArgumentException(
							"Storage " + storageId + " does not exist or user does not have access to storage");
				}
				storage = Utility.getStorage(storageId);
			} else {
				storage = (IStorageEngine) grs.get(0);
			}
			return storage;
		}

		List<NounMetadata> storageInputs = this.curRow.getNounsOfType(PixelDataType.STORAGE);
		if (storageInputs != null && !storageInputs.isEmpty()) {
			return (IStorageEngine) storageInputs.get(0).getValue();
		}

		throw new NullPointerException("No storage engine defined");
	}

	@Override
	protected MCP_TYPE getMcpTypeForKey(String key) {
		// All keys are strings by default
		return super.getMcpTypeForKey(key);
	}

	@Override
	public String getReactorDescription() {
		return "List the files and folders in a storage path";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.STORAGE.getKey())) {
			return "The storage engine instance or id";
		} else if (key.equals(ReactorKeysEnum.STORAGE_PATH.getKey())) {
			return "The storage path to list contents from";
		}
		return super.getDescriptionForKey(key);
	}

}
