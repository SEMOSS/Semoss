package prerna.util.git.reactors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitAssetUtils;

public class SearchAssetReactor extends AbstractReactor {

	public SearchAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SEARCH.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.SPACE.getKey() };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String space = this.keyValue.get(this.keysToGet[2]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, false);

		// get search term
		String search = keyValue.get(keysToGet[0]);

		// get specific search location
		String location = assetFolder;
		if (keyValue.containsKey(keysToGet[1])) {
			location = assetFolder + "/" + Utility.normalizePath(keyValue.get(keysToGet[1]));
			location = location.replaceAll("\\\\", "/");
		}

		// location = location.replaceAll("/app_assets", "");
		return new NounMetadata(GitAssetUtils.listAssetMetadata(location, search, assetFolder, null, null),
				PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

}
