package prerna.util.git.reactors;

import java.util.Hashtable;

import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
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
	 * Synchronize an existing database to a specified remote
	 */

	public InitAppRepo() {
		super.keysToGet = new String[]{
				ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.REPOSITORY.getKey(), 
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(), 
				ReactorKeysEnum.SYNC_DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Welcome to App IF ANY : SEMOSS Marketplace");

		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if(databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Need to specify the database id");
		}
		
		// you can only push
		// if you are the owner
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if(!SecurityEngineUtils.userIsOwner(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database does not exist or user does not have access to edit database");
		}
		String databaseName = SecurityEngineUtils.getEngineAliasForId(databaseId);
		
		String repository = this.keyValue.get(this.keysToGet[1]);
		if(repository == null || repository.isEmpty()) {
			throw new IllegalArgumentException("Need to specify the repository to publish the database");
		}
		String username = this.keyValue.get(this.keysToGet[2]);
		if(this.keyValue.size() == 5 && (username == null || username.isEmpty())) {
			throw new IllegalArgumentException("Need to specify the username for the remote database");
		}
		String password = this.keyValue.get(this.keysToGet[3]);
		if(this.keyValue.size() == 5 &&  (password == null || password.isEmpty())) {
			throw new IllegalArgumentException("Need to password for the remote database");
		}
		String databaseStr = this.keyValue.get(this.keysToGet[4]);
		boolean syncDatabase = true;
		if(databaseStr != null && databaseStr.equals("false")) {
			syncDatabase = false;
		}
		// this is not actually used at the moment
		// String type = this.keyValue.get(this.keysToGet[5]);

//		logger.info("Using database name = " + appName);
//		logger.info("Using remote = " + repository);
//		logger.info("Using username = " + username);
		logger.info("Beginning process to make your application global");
		logger.info("This can take several minutes");

		try {
			// close the database
			if(syncDatabase) {
				Utility.getDatabase(databaseId).close();
				DIHelper.getInstance().removeLocalProperty(databaseId);
			}
			// make database to remote
			if (this.keyValue.size() == 5) {
				GitCreator.makeRemoteFromDatabase(databaseId, databaseName, repository, username, password, syncDatabase, "");
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

				GitCreator.makeRemoteFromDatabase(databaseId, databaseName, repository, username, "", syncDatabase, token);
				
				ClusterUtil.pushEngine(databaseId);
			}
			logger.info("Congratulations! You have successfully created your database " + repository);
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			// open it back up
			if(syncDatabase) {
				Utility.getDatabase(databaseId);
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_INIT);
	}
}
