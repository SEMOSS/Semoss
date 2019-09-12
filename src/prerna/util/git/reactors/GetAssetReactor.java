package prerna.util.git.reactors;

import java.io.File;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.git.GitRepoUtils;

public class GetAssetReactor extends AbstractReactor {

	// gets a particular asset in a particular version
	// if the version is not provided - this gets the head

	public GetAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.VERSION.getKey(),
				ReactorKeysEnum.SPACE.getKey() };
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

		String space = this.keyValue.get(this.keysToGet[2]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space);

		// specify a file
		String asset = keyValue.get(keysToGet[0]);
		asset = asset.replaceAll("app_assets", "");
		// grab the version
		String version = null;
		if (keyValue.containsKey(keysToGet[1])) {
			version = keyValue.get(keysToGet[1]);
		}

		assetFolder = insight.getInsightFolder();
		File file2 = new File(assetFolder);
		String insightDir = file2.getName();
		assetFolder = file2.getParentFile().getAbsolutePath();

		// I need a better way than output
		// probably write the file and volley the file ?
		String output = GitRepoUtils.getFile(version, insightDir + "/" + asset, assetFolder);
		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

}
