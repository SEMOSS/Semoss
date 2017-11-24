package prerna.sablecc2.reactor.git;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;

public class Replicate extends AbstractReactor {

	// clone from remote
	// this assumes the local directory is not there
	
	public Replicate()
	{
		super.keysToGet = new String[]{"remoteapp", "app", "type"};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		// creates a remote repository
		organizeKeys();
		GitHelper helper = new GitHelper();
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String dbName = keyValue.get(keysToGet[1]);	

		helper.makeAppFromRemote(baseFolder, dbName, keyValue.get(keysToGet[0]));
		
		return new NounMetadata("Success", PixelDataType.CONST_STRING);
	}

}
