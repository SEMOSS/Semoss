package prerna.util.git.reactors;

import java.util.List;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;
import prerna.util.git.GitAssetUtils;
import prerna.util.git.GitRepoUtils;

public class ListCommentReactor extends AbstractReactor {

	// pulls the latest for this project / asset
	// the asset is basically the folder where it sits
	// this can be used enroute in a pipeline
	
	public ListCommentReactor() {
		// specific insight
		this.keysToGet = new String[]{"insight"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// is there a way to get app folder
		String assetFolder = this.insight.getInsightFolder();
		assetFolder = assetFolder.replaceAll("\\\\", "/");


		// the whole replace can be avoided if I know the app folder
		if(keyValue.containsKey(keysToGet[0]))
		{
			// this insight
			String thisInsight = Utility.getInstanceName(assetFolder);			
			assetFolder = assetFolder.replace(thisInsight,keyValue.get(keysToGet[1])); 		
		}
		
		List output = GitRepoUtils.listCommits(assetFolder, null);

		return new NounMetadata(output, PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

}
