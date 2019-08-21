package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;
import prerna.util.git.GitFetchUtils;

public class SaveAssetReactor extends AbstractReactor {

	// pulls the latest for this project / asset
	// the asset is basically the folder where it sits
	// this can be used enroute in a pipeline
	
	public SaveAssetReactor() {
		this.keysToGet = new String[]{"filename", "content"};
		this.keyRequired = new int[]{1,1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String assetFolder = this.insight.getInsightFolder(); // we need it where this would be the cache
		assetFolder = assetFolder.replaceAll("\\\\", "/");
		
		// I need to do the job of creating this directory i.e. the name of the repo
		// TBD
		
		String fileName = assetFolder + "/" + keyValue.get(keysToGet[0]);
		String content = keyValue.get(keysToGet[1]);

		content = Utility.decodeURIComponent(content);
		File file = new File(fileName);
		try {
			FileUtils.writeStringToFile(file, content);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return new NounMetadata("Success!", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

}
