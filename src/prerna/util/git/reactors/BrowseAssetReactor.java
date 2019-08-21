package prerna.util.git.reactors;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitAssetUtils;

public class BrowseAssetReactor extends AbstractReactor {

	// pulls the latest for this project / asset
	// the asset is basically the folder where it sits
	// this can be used enroute in a pipeline

	public BrowseAssetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// base asset folder path
		String assetFolder = this.insight.getInsightFolder();
		assetFolder = assetFolder.replaceAll("\\\\", "/");
		
		// specific folder to browse
		String locFolder = assetFolder;
		if(keyValue.containsKey(keysToGet[0])) {
			locFolder = assetFolder + "/" + keyValue.get(keysToGet[0]);
			locFolder = locFolder.replaceAll("\\\\", "/");
		}

		return new NounMetadata(GitAssetUtils.getAssetMetadata(locFolder, assetFolder), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

}
