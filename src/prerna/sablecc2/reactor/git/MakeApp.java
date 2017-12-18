package prerna.sablecc2.reactor.git;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.git.GitCreator;

public class MakeApp extends AbstractReactor {

	/**
	 * Synchronize an existing app to a specified remote
	 */

	public MakeApp() {
		super.keysToGet = new String[]{"app", "remote", "username", "password", "database", "type"};
	}

	@Override
	public NounMetadata execute() {
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
		String databaseStr = this.keyValue.get(this.keysToGet[4]);
		boolean syncDatabase = true;
		if(databaseStr != null && databaseStr.equals("false")) {
			syncDatabase = false;
		}
		// this is not actually used at the moment
		// String type = this.keyValue.get(this.keysToGet[5]);

		logger.info("Using app name = " + appName);
		logger.info("Using remote = " + remote);
		logger.info("Using username = " + username);
		logger.info("Beginning process to make your application global");
		logger.info("This can take several minutes");

		try {
			// close the app
			if(syncDatabase) {
				Utility.getEngine(appName).closeDB();
				DIHelper.getInstance().removeLocalProperty(appName);
			}
			// make app to remote
			GitCreator.makeRemoteFromApp(appName, remote, username, password, syncDatabase);
			logger.info("Congratulations! You have successfully created your app " + remote);
		} finally {
			// open it back up
			if(syncDatabase) {
				Utility.getEngine(appName);
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_INIT);
	}
}
