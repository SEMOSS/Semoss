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
import prerna.util.AssetUtility;

public class PullFromStorageReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PullFromStorageReactor.class);

	public PullFromStorageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.STORAGE.getKey(), ReactorKeysEnum.STORAGE_PATH.getKey(),
				ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		IStorageEngine storage = getStorage();
		String storagePath = this.keyValue.get(ReactorKeysEnum.STORAGE_PATH.getKey());
		String filePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());

		String space = this.keyValue.get(ReactorKeysEnum.SPACE.getKey());
		if (space == null || space.isEmpty()) {
			space = "insight"; // default to insight space
		}

		String assetFolder = AssetUtility.getRootFolderPath(this.insight, space, false);
		if (!(new File(assetFolder).isDirectory())) {
			new File(assetFolder).mkdirs();
		}

		String fileLocation = assetFolder + "/" + filePath;

		try {
			storage.copyToLocal(storagePath, fileLocation);
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred downloading storage file to local");
		}
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

}
