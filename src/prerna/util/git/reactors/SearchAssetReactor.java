package prerna.util.git.reactors;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitAssetUtils;

public class SearchAssetReactor extends AbstractReactor {

	public SearchAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SEARCH.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.IN_APP.getKey()};
		this.keyRequired = new int[] { 1, 0,0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// get asset base folder to create relative path
		//boolean app = keyValue.containsKey(keysToGet[2]);
		boolean app = keyValue.containsKey(keysToGet[2]) || (keyValue.containsKey(keysToGet[1]) && keyValue.get(keysToGet[1]).startsWith("app_assets"));

		// get the asset folder path
		String assetFolder = this.insight.getInsightFolder(); 
		if(app)
			assetFolder = this.insight.getAppFolder();
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		// get search term
		String search = keyValue.get(keysToGet[0]);
		
		// get specific search location
		String location = assetFolder;
		if (keyValue.containsKey(keysToGet[1])) {
			location = assetFolder + "/" + keyValue.get(keysToGet[1]);
			location = location.replaceAll("\\\\", "/");
		}

		//location = location.replaceAll("/app_assets", "");
		return new NounMetadata(GitAssetUtils.listAssetMetadata(location, search, assetFolder, null, null), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

}
