package prerna.util.git.reactors;

import java.util.List;

import org.apache.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
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

		
		// throw error if user is anonymous
		if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
			throwAnonymousUserError();
		}

		// throw error is user doesn't have rights to publish new apps
		if(AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(this.insight.getUser())) {
			throwUserNotPublisherError();
		}
		
		try {
			String appId = GitConsumer.makeAppFromRemote(localAppName, repository, logger);
			ClusterUtil.reactorPushApp(appId);
			User user = this.insight.getUser();
			if(user != null) {
				List<AuthProvider> logins = user.getLogins();
				for(AuthProvider ap : logins) {
					SecurityUpdateUtils.addEngineOwner(appId, user.getAccessToken(ap).getId());
				}
			}
			logger.info("Congratulations! Downloading your new app has been completed");
			return new NounMetadata(UploadUtilities.getAppReturnData(user, appId), PixelDataType.MAP, PixelOperationType.MARKET_PLACE_ADDITION);
//			return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_ADDITION);
		} catch(Exception e) {
			NounMetadata noun = new NounMetadata(e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.WARNING);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}
	}
}
