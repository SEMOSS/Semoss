package prerna.sablecc2.reactor.git;

import java.util.Hashtable;

import org.apache.log4j.Logger;

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
		Logger logger = getLogger(this.getClass().getName());

		organizeKeys();
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		String dbName = baseFolder + "/db/" + keyValue.get(keysToGet[0]);	
		logger.info("Getting remotes configures on " + dbName);
		GitHelper helper = new GitHelper();

		Hashtable <String, String> repoList = helper.listConfigRemotes(dbName);
		
		return new NounMetadata(repoList, PixelDataType.MAP);
	}

}
