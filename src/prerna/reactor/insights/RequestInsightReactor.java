package prerna.reactor.insights;

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
import prerna.auth.utils.SecurityInsightUtils;
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

public class RequestInsightReactor extends AbstractReactor {
	private static final String REQUEST_INSIGHT_EMAIL_TEMPLATE = "requestInsight.html";
	private static final Logger classLogger = LogManager.getLogger(RequestInsightReactor.class);

	public RequestInsightReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.PERMISSION.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String insightId = this.keyValue.get(this.keysToGet[1]);
		String permission = this.keyValue.get(this.keysToGet[2]);
		String requestComment = this.keyValue.get(this.keysToGet[3]);
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to request a insight",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}

		// throw error if user is anonymous
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}
		
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		String userId = token.getId();
		// check user permission for the insight
		Integer currentUserPermission = SecurityInsightUtils.getUserInsightPermission(userId, projectId, insightId);
		// make sure requesting new level of permission
		int requestPermission = -1;
		try {
			requestPermission = Integer.parseInt(permission);
		} catch(NumberFormatException ignore) {
			requestPermission = AccessPermissionEnum.getPermissionByValue(permission).getId();
		}
		if(currentUserPermission != null && requestPermission == currentUserPermission) {
			throw new IllegalArgumentException("This user already has access to this project with the given permission level");
		}
		
		//check user pending permission
		Integer currentPendingUserPermission = SecurityInsightUtils.getUserAccessRequestInsightPermission(userId, projectId, insightId);
		if(currentPendingUserPermission != null && requestPermission == currentPendingUserPermission) {
			throw new IllegalArgumentException("This user has already requested access to this insight with the given permission level");
		}

		String userType = token.getProvider().toString();
		SecurityInsightUtils.setUserAccessRequest(userId, userType, projectId, requestComment, insightId, requestPermission, user);
		sendEmail(user, projectId, insightId, permission, requestComment);
		return NounMetadata.getSuccessNounMessage("Successfully requested the insight");
	}

	private void sendEmail(User user, String projectId, String insightId, String permission, String requestComment) {
		String template = getTemplateString();
		if (template !=null && !template.isEmpty()) {
			List<String> insightOwners = SecurityInsightUtils.getInsightOwners(projectId, insightId);
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
			if (insightOwners != null && !insightOwners.isEmpty()) {
				String insightName = SecurityInsightUtils.getInsightAliasForId(projectId, insightId);
				Session emailSession = SocialPropertiesUtil.getInstance().getEmailSession();
				final String INSIGHT_NAME_REPLACEMENT = "$insightName$";
				final String PERMISSION_REPLACEMENT = "$permission$";
				final String USER_NAME_REPLACEMENT = "$userName$";
				final String USER_EMAIL_REPLACEMENT = "$userEmail$";
				final String REQUEST_REASON_COMMENT = "$requestReason$";
				Map<String, String> emailReplacements = SocialPropertiesUtil.getInstance().getEmailStaticProps();
				emailReplacements.put(INSIGHT_NAME_REPLACEMENT, insightName);
				emailReplacements.put(PERMISSION_REPLACEMENT, permission);
				emailReplacements.put(USER_NAME_REPLACEMENT, userName);
				emailReplacements.put(USER_EMAIL_REPLACEMENT, userEmail);
				emailReplacements.put(REQUEST_REASON_COMMENT, requestComment);
				String message = EmailUtility.fillEmailComponents(template, emailReplacements);
				EmailUtility.sendEmail(emailSession, insightOwners.toArray(new String[0]), null, null,
						SocialPropertiesUtil.getInstance().getSmtpSender(), "SEMOSS - Insight Access Request", message,
						true, null);
			}
		}
	}

	private String getTemplateString() {
		String template = null;
		String templatePath = DIHelper.getInstance().getProperty(Constants.EMAIL_TEMPLATES);
		if (templatePath.endsWith("\\") || templatePath.endsWith("/")) {
			templatePath += REQUEST_INSIGHT_EMAIL_TEMPLATE;
		} else {
			templatePath += "/" + REQUEST_INSIGHT_EMAIL_TEMPLATE;
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
