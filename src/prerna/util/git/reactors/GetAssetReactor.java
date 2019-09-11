package prerna.util.git.reactors;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitRepoUtils;

public class GetAssetReactor extends AbstractReactor {

	// gets a particular asset in a particular version
	// if the version is not provided - this gets the head

	public GetAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.VERSION.getKey(), ReactorKeysEnum.IN_APP.getKey() };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		// check if user is logged in
		User user = this.insight.getUser();
		if (AbstractSecurityUtils.securityEnabled()) {
			if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				throwAnonymousUserError();
			}
		}

		boolean app = (keyValue.containsKey(keysToGet[2]) && keyValue.get(keysToGet[2]).equalsIgnoreCase("app")) ;//|| (keyValue.containsKey(keysToGet[0]) && keyValue.get(keysToGet[0]).startsWith("app_assets"));
		boolean isUser = (keyValue.containsKey(keysToGet[2]) && keyValue.get(keysToGet[2]).equalsIgnoreCase("user")) ;

		String assetFolder = this.insight.getInsightFolder();
		
		if(isUser)
		{
			// do other things
		}
		if (app) {
			assetFolder = this.insight.getAppFolder();
		}
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		// specify a file
		String asset = keyValue.get(keysToGet[0]);
		asset = asset.replaceAll("app_assets", "");
		// grab the version
		String version = null;
		if (keyValue.containsKey(keysToGet[1])) {
			version = keyValue.get(keysToGet[1]);
		}

		// I need a better way than output
		// probably write the file and volley the file ?
		String output = GitRepoUtils.getFile(version, asset, assetFolder);
		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

}
