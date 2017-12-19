package prerna.util.git.reactors;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.git.GitRepoUtils;

public class DeleteRemote extends AbstractReactor {

	public DeleteRemote() {
		this.keysToGet = new String[]{"remoteapp", "app", "username", "password"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Removing remote...");
		String remoteApp = this.keyValue.get(this.keysToGet[0]);
		String appName = this.keyValue.get(this.keysToGet[1]);
		String username = this.keyValue.get(this.keysToGet[2]);
		String password = this.keyValue.get(this.keysToGet[3]);

		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String appFolder = baseFolder + "/" + appName;
		// remove it from remote
		GitRepoUtils.removeRemote(appFolder, remoteApp);
		// drop it from external
		GitRepoUtils.deleteRemoteRepository(remoteApp, username, password);
		return new NounMetadata(true, PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

}
