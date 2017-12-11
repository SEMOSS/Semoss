package prerna.sablecc2.reactor.git;

import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;
import prerna.util.Utility;

public class MakeApp extends AbstractReactor {

	/**
	 * Synchronize an existing app to a specified remote
	 */

	public MakeApp() {
		super.keysToGet = new String[]{"app", "remote", "username", "password", "type"};
	}

	@Override
	public NounMetadata execute() {
		boolean output = false;
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Welcome to App IF ANY : SEMOSS Marketplace");
		

		organizeKeys();
		String appName = this.keyValue.get(this.keysToGet[0]);
		if(appName == null || appName.isEmpty()) {
			throw new IllegalArgumentException("Need to specify the app name");
		}
		// kill the engine
		String remote = this.keyValue.get(this.keysToGet[1]);
		if(remote == null || remote.isEmpty()) {
			throw new IllegalArgumentException("Need to specify the remote to publish the app");
		}
		String username = this.keyValue.get(this.keysToGet[2]);
		if(username == null || username.isEmpty()) {
			throw new IllegalArgumentException("Need to specify the username for the remote app");
		}
		String password = this.keyValue.get(this.keysToGet[3]);
		if(password == null || password.isEmpty()) {
			throw new IllegalArgumentException("Need to password for the remote app");
		}
		// this is not actually used at the moment
		// String type = this.keyValue.get(this.keysToGet[5]);

		logger.info("Using app name = " + appName);
		logger.info("Using remote = " + remote);
		logger.info("Using username = " + username);
		logger.info("Beginning process to make your application global");
		logger.info("This can take several minutes");

		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		
		// remove the app
		Utility.getEngine(appName).closeDB();
		DIHelper.getInstance().removeLocalProperty(appName);
		
		GitHelper helper = new GitHelper();
		try {
			output = helper.makeRemoteFromApp(baseFolder, appName, remote, true, username, password);
			logger.info("Congratulations! You have successfully created your app " + remote);
		} catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		} finally {
			// open it back up
			Utility.getEngine(appName);
		}
		
		return new NounMetadata(output, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_INIT);
	}
}
