package prerna.reactor.engine.fs;

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

public class GetEngineAssetsBase64Reactor extends AbstractReactor {

	public GetEngineAssetsBase64Reactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have access to edit assets.");
		}
		// force to pull it from cloud if not in the container
		IEngine engine = Utility.getEngine(engineId);

		String filePath = this.keyValue.get(this.keysToGet[1]);
		if (filePath == null || (filePath = filePath.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass a filePath for the file to retrieve");
		}
		filePath = filePath.replace("\\", "/");
		if (!filePath.startsWith("/")) {
			filePath = "/" + filePath;
		}
		filePath = Utility.normalizePath(filePath);

		String assetFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		String output = FileSystemUtil.getAssetAsBase64(assetFolder, filePath);
		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Retrieve the contents of a file (image, pdf, etc.) as base64 encoded string in the engine";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The unique id for the engine";
		} else if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Name of the file to get the contents as a base64 encoded string. This relative path should assume the prefix of '/version/assets/' and not include the prefix in the string value.";
		}
		return super.getDescriptionForKey(key);
	}

}
