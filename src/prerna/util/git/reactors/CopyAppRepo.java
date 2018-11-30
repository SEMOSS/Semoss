package prerna.util.git.reactors;

import org.apache.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitConsumer;

public class CopyAppRepo extends AbstractReactor {

	/**
	 * Clone an existing remote app and bring it into the 
	 * local semoss that is running for collaboration
	 */
	
	public CopyAppRepo() {
		super.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.REPOSITORY.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String localAppName = this.keyValue.get(this.keysToGet[0]);
		if(localAppName == null || localAppName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the local app name");
		}
		String repository = this.keyValue.get(this.keysToGet[1]);
		if(repository == null || repository.isEmpty()) {
			throw new IllegalArgumentException("Need to define a respository");
		}
		
		// check to see if the user is entering github.com and if so replace
		if(repository.contains("github.com"))
		{
			repository = repository.replace("http://github.com/","");
			repository = repository.replace("https://github.com/","");
		}
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Downloading app located at " + repository);
		logger.info("App will be named locally as " + localAppName);

		try {
			String appId = GitConsumer.makeAppFromRemote(localAppName, repository, logger);
			ClusterUtil.reactorPushApp(appId);
			logger.info("Congratulations! Downloading your new app has been completed");
			return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_ADDITION);
		} catch(Exception e) {
			NounMetadata noun = new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.WARNING);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}
	}
}
