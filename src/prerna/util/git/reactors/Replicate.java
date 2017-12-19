package prerna.util.git.reactors;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitConsumer;

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

		GitConsumer.makeAppFromRemote(localAppName, remoteApp);
		logger.info("Congratulations! Downloading your new app has been completed");
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_ADDITION);
	}
}
