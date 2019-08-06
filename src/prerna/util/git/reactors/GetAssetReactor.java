package prerna.util.git.reactors;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.security.InstallCertNow;
import prerna.util.Utility;
import prerna.util.git.GitFetchUtils;
import prerna.util.git.GitRepoUtils;

public class GetAssetReactor extends AbstractReactor {


	// gets a particular asset in a particular version
	// if the version is not provided - this gets the head
	
	public GetAssetReactor() {
		this.keysToGet = new String[]{"asset", "version"};
		this.keyRequired = new int[]{1,0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String assetFolder = this.insight.getInsightFolder(); // we need it where this would be the cache
		assetFolder = assetFolder.replaceAll("\\\\", "/");
		
		// I need to do the job of creating this directory i.e. the name of the repo
		// TBD

		String asset = keyValue.get(keysToGet[0]);
		String version = null;
		
		if(keyValue.containsKey(keysToGet[1]))
			version = keyValue.get(keysToGet[1]);
		
		// I need a better way than output
		// probably write the file and volley the file ?
		String output = GitRepoUtils.getFile(version, asset, assetFolder);
		
		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

}
