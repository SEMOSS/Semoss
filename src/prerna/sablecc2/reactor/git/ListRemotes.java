package prerna.sablecc2.reactor.git;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
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
		
		// FE wants an array
		List<Map<String, String>> returnList = new Vector<Map<String, String>>();
		for(String key : repoList.keySet()) {
			Map<String, String> singleKey = new HashMap<String, String>();
			singleKey.put("name", key);
			singleKey.put("type", repoList.get(key));
			returnList.add(singleKey);
		}
		
		return new NounMetadata(returnList, PixelDataType.VECTOR, PixelOperationType.MARKET_PLACE);
	}

}
