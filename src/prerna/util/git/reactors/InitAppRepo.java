package prerna.util.git.reactors;

import java.util.Hashtable;

import org.apache.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.AbstractHttpHelper;
import prerna.util.BeanFiller;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.git.GitCreator;

public class InitAppRepo extends GitBaseReactor {

	/**
	 * Synchronize an existing app to a specified remote
	 */

	public InitAppRepo() {
		super.keysToGet = new String[]{
				ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.REPOSITORY.getKey(), 
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(), 
				ReactorKeysEnum.SYNC_DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Welcome to App IF ANY : SEMOSS Marketplace");

		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		if(appId == null || appId.isEmpty()) {
			throw new IllegalArgumentException("Need to specify the app name");
		}
		String appName = null;
		
		// you can only push
		// if you are the owner
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityQueryUtils.userIsOwner(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("App does not exist or user does not have access to edit database");
			}
			appName = SecurityQueryUtils.getEngineAliasForId(appId);
		} else {
			appName = MasterDatabaseUtility.getEngineAliasForId(appId);
		}
		
		
		String repository = this.keyValue.get(this.keysToGet[1]);
		if(repository == null || repository.isEmpty()) {
			throw new IllegalArgumentException("Need to specify the repository to publish the app");
		}
		String username = this.keyValue.get(this.keysToGet[2]);
		if(this.keyValue.size() == 5 && (username == null || username.isEmpty())) {
			throw new IllegalArgumentException("Need to specify the username for the remote app");
		}
		String password = this.keyValue.get(this.keysToGet[3]);
		if(this.keyValue.size() == 5 &&  (password == null || password.isEmpty())) {
			throw new IllegalArgumentException("Need to password for the remote app");
		}
		String databaseStr = this.keyValue.get(this.keysToGet[4]);
		boolean syncDatabase = true;
		if(databaseStr != null && databaseStr.equals("false")) {
			syncDatabase = false;
		}
		// this is not actually used at the moment
		// String type = this.keyValue.get(this.keysToGet[5]);

//		logger.info("Using app name = " + appName);
//		logger.info("Using remote = " + repository);
//		logger.info("Using username = " + username);
		logger.info("Beginning process to make your application global");
		logger.info("This can take several minutes");

		try {
			// close the app
			if(syncDatabase) {
				Utility.getEngine(appId).closeDB();
				DIHelper.getInstance().removeLocalProperty(appId);
			}
			// make app to remote
			if (this.keyValue.size() == 5) {
				GitCreator.makeRemoteFromApp(appId, appName, repository, username, password, syncDatabase, "");
			} else {
				String token = getToken();
				String url = "https://api.github.com/user";
				String [] beanProps = {"name", "profile"};
				String jsonPattern = "[name,login]";
				Hashtable params = new Hashtable();
				params.put("access_token", token);
				
				String output = AbstractHttpHelper.makeGetCall(url, token, params, false);
				AccessToken accessToken2 = (AccessToken)BeanFiller.fillFromJson(output, jsonPattern, beanProps, new AccessToken());
				username = accessToken2.getProfile();

				GitCreator.makeRemoteFromApp(appId, appName, repository, username, "", syncDatabase, token);
				
				ClusterUtil.reactorPushApp(appId);
			}
			logger.info("Congratulations! You have successfully created your app " + repository);
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			// open it back up
			if(syncDatabase) {
				Utility.getEngine(appId);
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_INIT);
	}
}
