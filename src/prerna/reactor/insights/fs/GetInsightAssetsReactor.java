package prerna.reactor.insights.fs;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class GetInsightAssetsReactor extends AbstractReactor {

	public GetInsightAssetsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String filePath = this.keyValue.get(this.keysToGet[0]);
		if (filePath == null || (filePath = filePath.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass a filePath for the file to retrieve");
		}
		filePath = filePath.replace("\\", "/");
		if (!filePath.startsWith("/")) {
			filePath = "/" + filePath;
		}
		filePath = Utility.normalizePath(filePath);

		String assetFolder = this.insight.getInsightFolder();
		String output = FileSystemUtil.getAssetAsString(assetFolder, filePath);
		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Retrieve the contents of a file in the insight";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "Names of the file to get the contents";
		}
		return super.getDescriptionForKey(key);
	}

}
