package prerna.sablecc2.reactor.git;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;

public class Replicate extends AbstractReactor {

	/**
	 * Clone an existing remote app and bring it into the 
	 * local semoss that is running for collaboration
	 */
	
	public Replicate() {
		super.keysToGet = new String[]{"remoteapp", "app", "type"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String remoteApp = this.keyValue.get(this.keysToGet[0]);
		if(remoteApp == null || remoteApp.isEmpty()) {
			throw new IllegalArgumentException("Need to define a remote app");
		}
		String localAppName = this.keyValue.get(this.keysToGet[1]);
		if(localAppName == null || localAppName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the local app name");
		}
		
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Downloading app located at " + remoteApp);
		logger.info("App will be named locally as " + localAppName);

		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		GitHelper helper = new GitHelper();
		try {
			helper.makeAppFromRemote(baseFolder, localAppName, remoteApp);
			logger.info("Congratulations! Downloading your new app has been completed");
		} catch(Exception e) {
			throw new RuntimeException(e.getMessage());
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_ADDITION);
	}
}
