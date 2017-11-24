package prerna.sablecc2.reactor.git;

import java.util.Hashtable;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;

public class ListRemotes extends AbstractReactor {

	// clone from remote
	// this assumes the local directory is not there
	
	
	public ListRemotes()
	{
		this.keysToGet = new String[]{"app"};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		// creates a remote repository
		organizeKeys();
		GitHelper helper = new GitHelper();
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		String dbName = baseFolder + "/db/" + keyValue.get(keysToGet[0]);	


		Hashtable <String, String> repoList = helper.listConfigRemotes(dbName);
		
		return new NounMetadata(repoList, PixelDataType.MAP);
	}

}
