package prerna.reactor.storage;

import java.util.List;
import java.util.ArrayList;

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

public class DeleteFromStorageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteFromStorageReactor.class);

	private static final String LEAVE_FOLDER_STRUCTURE = "leaveFolderStructure";
	private static final String STORAGE_PATHS = "storagePaths";

	public DeleteFromStorageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(),
				LEAVE_FOLDER_STRUCTURE, STORAGE_PATHS };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		IStorageEngine storage = getStorage();
		// check that the user can edit the engine
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), storage.getEngineId())) {
			throw new IllegalArgumentException("User does not have permission to delete from the remote storage");
		}

		List<String> storagePaths = getStoragePaths();
		boolean leaveFolderStructure = Boolean.parseBoolean(this.keyValue.get(LEAVE_FOLDER_STRUCTURE) + "");

		try {
			if (storagePaths.size() == 1) {
				storage.deleteFromStorage(storagePaths.get(0), leaveFolderStructure);
			} else {
				for (String storagePath : storagePaths) {
					storage.deleteFromStorage(storagePath, leaveFolderStructure);
				}
			}
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred deleting files from storage");
		}
	}

	private List<String> getStoragePaths() {
		List<String> storagePaths = new ArrayList<>();

		String storagePath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
		if (storagePath != null && !storagePath.isEmpty()) {
			storagePaths.add(storagePath);
			return storagePaths;
		}

		GenRowStruct grs = this.store.getNoun(STORAGE_PATHS);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				storagePaths.add(grs.get(i).toString());
			}
			return storagePaths;
		}

		List<NounMetadata> storagePathInputs = this.curRow.getNounsOfType(PixelDataType.STRING);
		if (storagePathInputs != null && !storagePathInputs.isEmpty()) {
			for (NounMetadata input : storagePathInputs) {
				storagePaths.add(input.getValue().toString());
			}
			return storagePaths;
		}

		throw new IllegalArgumentException("No storage paths provided for deletion");
	}

	private IStorageEngine getStorage() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.STORAGE.getKey());
		if (grs != null && !grs.isEmpty()) {
			return (IStorageEngine) grs.get(0);
		}

		List<NounMetadata> storageInputs = this.curRow.getNounsOfType(PixelDataType.STORAGE);
		if (storageInputs != null && !storageInputs.isEmpty()) {
			return (IStorageEngine) storageInputs.get(0).getValue();
		}

		throw new NullPointerException("No storage engine defined");
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(LEAVE_FOLDER_STRUCTURE)) {
			return "Boolean value if the folder structure should still be maintained even when deleting the path. Default is false.";
		}
		if (key.equals(STORAGE_PATHS)) {
			return "List of storage paths to delete. If provided, this takes precedence over storagePath.";
		}
		return super.getDescriptionForKey(key);
	}
}
