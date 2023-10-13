package prerna.util.git.reactors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
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
		String space = this.keyValue.get(this.keysToGet[2]);
		
		// we need to change this to asset base folder
		String assetFolder = AssetUtility.getAssetVersionBasePath(this.insight, space, false);

		// relative path is used for git if the insight is saved
		String assetDir = "";
		if(space == null || AssetUtility.INSIGHT_SPACE_KEY.equalsIgnoreCase(space)) {
			if (this.insight.isSavedInsight()) {
				assetDir = AssetUtility.getAssetRelativePath(this.insight, space);
			} else {
				// get temp insight folder
				assetFolder = this.insight.getInsightFolder();
			}
		} else {
			// user space + app holds assets in assets folder
			// this should not be assets anymore
			assetDir = "";
		}

		// specify a file
		String asset =Utility.normalizePath( keyValue.get(keysToGet[0]));
		// grab the version
		String version = null;
		if (keyValue.containsKey(keysToGet[1])) {
			version = keyValue.get(keysToGet[1]);
		}

		// I need a better way than output
		// probably write the file and volley the file ?
		// ideally this should be through the sym link
		String output = GitRepoUtils.getFile(version, assetDir + "/" + asset, assetFolder);
		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
}
