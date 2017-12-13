package prerna.sablecc2.reactor.git;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;

public class ListRemotes extends AbstractReactor {

	/**
	 * Get the list of remotes for a given app
	 */
	
	public ListRemotes() {
		this.keysToGet = new String[]{"app"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appName = this.keyValue.get(this.keysToGet[0]);
		if(appName == null || appName.isEmpty()) {
			throw new IllegalArgumentException("Need to provide the app name");
		}
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String dbName = baseFolder + "/db/" + keyValue.get(keysToGet[0]) + "/version";	

		Logger logger = getLogger(this.getClass().getName());
		logger.info("Getting remotes configures on " + dbName);
		
		GitHelper helper = new GitHelper();
		List<Map<String, String>> repoList = helper.listConfigRemotes(dbName);
		return new NounMetadata(repoList, PixelDataType.VECTOR, PixelOperationType.MARKET_PLACE);
	}
}
