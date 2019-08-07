package prerna.util.git.reactors;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitAssetUtils;

public class BrowseReactor extends AbstractReactor {

	// pulls the latest for this project / asset
	// the asset is basically the folder where it sits
	// this can be used enroute in a pipeline

	public BrowseReactor() {
		this.keysToGet = new String[]{"location"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String assetFolder = this.insight.getInsightFolder();
		assetFolder = assetFolder.replaceAll("\\\\", "/");
		String locFolder = assetFolder;
		if(keyValue.containsKey(keysToGet[0])) {
			locFolder = assetFolder + "/" + keyValue.get(keysToGet[0]);
		}

		return new NounMetadata(GitAssetUtils.browse(locFolder, assetFolder), PixelDataType.MAP, PixelOperationType.OPERATION);
	}

}
