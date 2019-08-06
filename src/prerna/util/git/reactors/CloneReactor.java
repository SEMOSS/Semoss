package prerna.util.git.reactors;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.security.InstallCertNow;
import prerna.util.Utility;
import prerna.util.git.GitFetchUtils;
import prerna.util.git.GitRepoUtils;

public class CloneReactor extends AbstractReactor {

	// pulls the latest for this project / asset
	// the asset is basically the folder where it sits
	// this can be used enroute in a pipeline
	
	public CloneReactor() {
		this.keysToGet = new String[]{"repository"};
		this.keyRequired = new int[]{1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String assetFolder = this.insight.getInsightFolder(); // we need it where this would be the cache
		assetFolder = assetFolder.replaceAll("\\\\", "/");
		
		// I need to do the job of creating this directory i.e. the name of the repo
		// TBD

		String repo = keyValue.get(keysToGet[0]);
		if(!repo.startsWith("http"))
			return new NounMetadata("Not a valid URL - " + repo, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		
		String assetLocation = Utility.getInstanceName(repo);
		//GitRepoUtils.addCertForDomain(repo);
		
		GitFetchUtils.cloneApp(repo, assetFolder + "/" + assetLocation);
		
		return new NounMetadata("Success!", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

}
