package prerna.reactor.engine.fs;

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class BrowseEngineAssetsReactor extends AbstractReactor {

	public BrowseEngineAssetsReactor() {
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

		String relativeFilePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
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
		int pathSubstringIndex = filePath.length();
		if (relativeFilePath != null && !relativeFilePath.isEmpty()) {
			filePath += relativeFilePath;
		}

		List<Map<String, Object>> retObj = FileSystemUtil.browseFileSystem(user, filePath, relativeFilePath,
				pathSubstringIndex);

		return new NounMetadata(retObj, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "List the files and directories from a relative filePath input from within the engine folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The unique id for the engine";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The relative file path to list contents from.";
		}
		return super.getDescriptionForKey(key);
	}

}
