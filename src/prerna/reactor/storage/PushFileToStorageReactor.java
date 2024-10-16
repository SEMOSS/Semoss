package prerna.reactor.storage;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IStorageEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class PushFileToStorageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PushFileToStorageReactor.class);

	public PushFileToStorageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(),

		};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String storageId = this.keyValue.get(ReactorKeysEnum.STORAGE.getKey());
		IStorageEngine storage = Utility.getStorage(storageId);
		String storageFolderPath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
		String fileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
		if (!new File(fileLocation).exists()) {
			throw new IllegalArgumentException("Unable to locate file");
		}

		try {
			storage.copyToStorage(fileLocation, storageFolderPath, null);
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred uploading local file to storage");
		}
	}

}
