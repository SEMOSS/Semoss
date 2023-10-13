package prerna.util.git.reactors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.git.GitRepoUtils;

public class CheckoutVersionReactor extends AbstractReactor {



	// checks out this app to a specific version
	// this is a bit dangerous if another user is operating on this at the same time
	// I dont know if we should use the cache copy and move there
	// if the version is not provided it will reset checkout
	
	public CheckoutVersionReactor() {
		this.keysToGet = new String[]{"version"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String assetFolder = this.insight.getInsightFolder(); // we need it where this would be the cache
		assetFolder = assetFolder.replaceAll("\\\\", "/");
		
		// I need to do the job of creating this directory i.e. the name of the repo
		// TBD
		
		String version = null;
		
		if(keyValue.containsKey(keysToGet[0]))
			version = keyValue.get(keysToGet[0]);
		
		// I need a better way than output
		// probably write the file and volley the file ?
		String output = null;
		try {
			if(version != null)
			{
				GitRepoUtils.checkout(assetFolder, version); 
				output = "Version - " + version + " active now";
			}
			else
			{
				GitRepoUtils.resetCheckout(assetFolder);
				output = "Version - latest" + " active now";
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

}
