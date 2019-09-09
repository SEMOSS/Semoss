package prerna.util.git.reactors;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitAssetUtils;

public class ListAssetReactor extends AbstractReactor {

	// pulls the latest for this project / asset
	// the asset is basically the folder where it sits
	// this can be used enroute in a pipeline
	
	public ListAssetReactor() {
		this.keysToGet = new String[]{"extn", "location"};
		this.keyRequired = new int[]{1, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		boolean app = (keyValue.containsKey(keysToGet[1]) && keyValue.get(keysToGet[1]).startsWith("app_assets"));

		String assetFolder = this.insight.getInsightFolder();

		if(app)
			assetFolder = this.insight.getAppFolder();

		assetFolder = assetFolder.replaceAll("\\\\", "/");
		
		String extn = keyValue.get(keysToGet[0]); 
		String location = assetFolder;

		if(keyValue.containsKey(keysToGet[1])) {
			location = assetFolder + "/" + keyValue.get(keysToGet[1]); 		
		}
		
		location = location.replaceAll("/app_assets", "");
		
		
		// I wonder if we should list fromt the app level as well
		
		return new NounMetadata(GitAssetUtils.listAssets(location, extn,assetFolder,  null, null), PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

}
