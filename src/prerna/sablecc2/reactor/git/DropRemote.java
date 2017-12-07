package prerna.sablecc2.reactor.git;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;

public class DropRemote extends AbstractReactor {

	// clone from remote
	// this assumes the local directory is not there
	
	
	public DropRemote()
	{
		this.keysToGet = new String[]{"remoteapp", "app"};

	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		// TODO Auto-generated method stub
		// creates a remote repository
		GitHelper helper = new GitHelper();
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		String dbName = baseFolder + "/" + keyValue.get(keysToGet[0]);	

		helper.removeRemote(dbName, keyValue.get(keysToGet[1]));
		
		return new NounMetadata("Success", PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

}
