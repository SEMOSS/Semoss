package prerna.util.git.reactors;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitAssetUtils;

public class SearchAssetReactor extends AbstractReactor {

	// pulls the latest for this project / asset
	// the asset is basically the folder where it sits
	// this can be used enroute in a pipeline
	
	public SearchAssetReactor() {
		this.keysToGet = new String[]{"extn", ReactorKeysEnum.FILE_PATH.getKey()};
		this.keyRequired = new int[]{1, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String assetFolder = this.insight.getInsightFolder();
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		String extn = keyValue.get(keysToGet[0]);
		String location = assetFolder;

		if(keyValue.containsKey(keysToGet[1])) {
			location = assetFolder + "/" + keyValue.get(keysToGet[1]); 		
		}
		
		System.out.println(location);
		System.out.println(extn);
		System.out.println(assetFolder);
		return new NounMetadata(GitAssetUtils.listAssetMetadata(location, extn,assetFolder,  null, null), PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

}
