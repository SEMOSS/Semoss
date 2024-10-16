package prerna.reactor.storage;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IStorageEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class DeleteFileFromStorageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DeleteFileFromStorageReactor.class);

	private static final String LEAVE_FOLDER_STRUCTURE = "leaveFolderStructure";

	public DeleteFileFromStorageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(),
				LEAVE_FOLDER_STRUCTURE };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String storageId = this.keyValue.get(ReactorKeysEnum.STORAGE.getKey());
		IStorageEngine storage = Utility.getStorage(storageId);
		String storagePath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
		boolean leaveFolderStructure = Boolean.parseBoolean(this.keyValue.get(LEAVE_FOLDER_STRUCTURE) + "");
		try {
			storage.deleteFromStorage(storagePath, leaveFolderStructure);
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred delete file from storage");
		}
	}
}
