package prerna.reactor.engine;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Session;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EmailUtility;
import prerna.util.SocialPropertiesUtil;

public class RequestEngineReactor extends AbstractReactor {
	
	private static final String REQUEST_ENGINE_EMAIL_TEMPLATE = "requestEngine.html";
	private static final Logger classLogger = LogManager.getLogger(RequestEngineReactor.class);

	public RequestEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.PERMISSION.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		String permission = this.keyValue.get(this.keysToGet[1]);
		String requestComment = this.keyValue.get(this.keysToGet[2]);
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to request an engine",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
				
		// turn permission into an integer in case it was added as the string version of the value
		int requestPermission = -1;
		try {
			requestPermission = Integer.parseInt(permission);
		} catch(NumberFormatException ignore) {
			requestPermission = AccessPermissionEnum.getPermissionByValue(permission).getId();
		}
					
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		String userId = token.getId();
		// check user permission for the engine
		Integer currentUserPermission = SecurityEngineUtils.getUserEnginePermission(userId, engineId);
		if(currentUserPermission != null && requestPermission == currentUserPermission) {
			throw new IllegalArgumentException("This user already has access to this engine with the given permission level");
		}
		//check user pending permission for engine
		Integer currentPendingUserPermission = SecurityEngineUtils.getUserAccessRequestDatabasePermission(userId, engineId);
		if(currentPendingUserPermission != null && requestPermission == currentPendingUserPermission) {
			throw new IllegalArgumentException("This user has already requested access to this engine with the given permission level");
		}
		// checking to make sure you can request access
		boolean canRequest = SecurityEngineUtils.engineIsDiscoverable(engineId) || SecurityEngineUtils.userHasExplicitAccess(user, engineId);
		if (canRequest) {
			String userType = token.getProvider().toString();
			SecurityEngineUtils.setUserAccessRequest(userId, userType, engineId, requestComment, requestPermission, user);
			sendEmail(user, engineId, permission, requestComment);
			return NounMetadata.getSuccessNounMessage("Successfully requested access to engine '" + engineId + "'");
		}

		return NounMetadata.getErrorNounMessage("Engine '" + engineId + "' is not requestable");
	}

	/**
	 * 
	 * @param user
	 * @param databaseId
	 * @param permission
	 */
	private void sendEmail(User user, String databaseId, String permission, String requestComment) {
		String template = getTemplateString();
		if (template !=null && !template.isEmpty()) {
			List<String> databaseOwners = SecurityEngineUtils.getEngineOwners(databaseId);
			AccessToken token = user.getAccessToken(user.getPrimaryLogin());
			String userName = token.getName() != null ? token.getName(): "";	
			String userEmail = token.getEmail() != null ? token.getEmail(): "";	
			// clean up permission
			if (permission.length() == 1) {
				permission = AccessPermissionEnum.getPermissionValueById(permission);
			}
			if(requestComment == null || requestComment.isEmpty()) {
				requestComment = "I'd like access, please.";
			}
			if (databaseOwners != null && !databaseOwners.isEmpty()) {
				String engineName = SecurityEngineUtils.getEngineAliasForId(databaseId);
				Session emailSession = SocialPropertiesUtil.getInstance().getEmailSession();
				final String ENGINE_NAME_REPLACEMENT = "$engineName$";
				final String PERMISSION_REPLACEMENT = "$permission$";
				final String USER_NAME_REPLACEMENT = "$userName$";
				final String USER_EMAIL_REPLACEMENT = "$userEmail$";
				final String REQUEST_REASON_COMMENT = "$requestReason$";
				Map<String, String> emailReplacements = SocialPropertiesUtil.getInstance().getEmailStaticProps();
				emailReplacements.put(ENGINE_NAME_REPLACEMENT, engineName);
				emailReplacements.put(PERMISSION_REPLACEMENT, permission);
				emailReplacements.put(USER_NAME_REPLACEMENT, userName);
				emailReplacements.put(USER_EMAIL_REPLACEMENT, userEmail);
				emailReplacements.put(REQUEST_REASON_COMMENT, requestComment);
				String message = EmailUtility.fillEmailComponents(template, emailReplacements);
				EmailUtility.sendEmail(emailSession, databaseOwners.toArray(new String[0]), null, null,
						SocialPropertiesUtil.getInstance().getSmtpSender(), "SEMOSS - Database Access Request", message,
						true, null);
			}
		}
	}

	/**
	 * 
	 * @return
	 */
	private String getTemplateString() {
		String template = null;
		String templatePath = DIHelper.getInstance().getProperty(Constants.EMAIL_TEMPLATES);
		 if (templatePath == null || templatePath.isEmpty()) {
			 	return null;
		    }
		if (templatePath.endsWith("\\") || templatePath.endsWith("/")) {
			templatePath += REQUEST_ENGINE_EMAIL_TEMPLATE;
		} else {
			templatePath += "/" + REQUEST_ENGINE_EMAIL_TEMPLATE;
		}
		File templateFile = new File(templatePath);
		if (templateFile.exists() && templateFile.isFile()) {
			try {
				template = FileUtils.readFileToString(templateFile);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return template;
	}
}
