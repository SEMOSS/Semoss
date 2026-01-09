package prerna.reactor.engine.fs;

import java.io.File;
import java.util.UUID;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class DownloadEngineAssetReactor extends AbstractReactor {

	public DownloadEngineAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to edit assets.");
		}
		IEngine engine = Utility.getEngine(engineId);

		String relativeFilePath = this.keyValue.get(this.keysToGet[1]);
		if (relativeFilePath != null) {
			relativeFilePath = Utility.normalizePath(relativeFilePath.trim());
			if (!relativeFilePath.isEmpty()) {
				relativeFilePath = relativeFilePath.replace('\\', '/');
				if (!relativeFilePath.startsWith("/")) {
					relativeFilePath = "/" + relativeFilePath;
				}
			}
		}

		String filePath = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		if (relativeFilePath != null && !relativeFilePath.isEmpty()) {
			filePath += relativeFilePath;
		}

		File toDownloadF = new File(filePath);
		String downloadFileLocation = FileSystemUtil.prepareAssetForDownload(toDownloadF, relativeFilePath,
				this.insight.getInsightFolder());

		// store the insight file
		// in the insight so the FE can download it
		// only from the given insight
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setFilePath(downloadFileLocation);
		insightFile.setDeleteOnInsightClose(false);
		this.insight.addExportFile(downloadKey, insightFile);
		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING,
				PixelOperationType.FILE_DOWNLOAD);
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Download a file or directory from within the engine folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the engine";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The relative file path to a file or directory within the engine folder";
		}
		return super.getDescriptionForKey(key);
	}

}