package prerna.reactor.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public abstract class AbstractEngineFileReactor extends AbstractReactor {

	protected static final Logger classLogger = LogManager.getLogger(AbstractEngineFileReactor.class);

	/**
	 * 
	 * @param user
	 */
	protected void validateUserAndEngineAccess(User user) {
		if (user == null) {
			NounMetadata noun = new NounMetadata(
					"User must be signed into an account to retrieve the function engine files",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		if (user == null || (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous())) {
			throwAnonymousUserError();
		}

		if (!SecurityQueryUtils.userIsPublisher(user)) {
			throwUserNotPublisherError();
		}
	}

}
