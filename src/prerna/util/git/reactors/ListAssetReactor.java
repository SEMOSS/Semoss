package prerna.util.git.reactors;

import java.util.List;

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
		this.keyRequired = new int[]{1,0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String assetFolder = this.insight.getInsightFolder();
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		String extn = keyValue.get(keysToGet[0]);
		String location = assetFolder;

		if(keyValue.containsKey(keysToGet[1]))
			location = assetFolder + "/" + keyValue.get(keysToGet[1]); 		
		
		List output = GitAssetUtils.listAssets(location, extn,assetFolder,  null, null);

		return new NounMetadata(output, PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

}
