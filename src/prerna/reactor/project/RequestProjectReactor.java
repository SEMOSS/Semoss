package prerna.reactor.project;

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
import prerna.auth.utils.SecurityProjectUtils;
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

public class RequestProjectReactor extends AbstractReactor {
	private static final String REQUEST_PROJECT_EMAIL_TEMPLATE = "requestProject.html";
	private static final Logger classLogger = LogManager.getLogger(RequestProjectReactor.class);

	public RequestProjectReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.PERMISSION.getKey(),  ReactorKeysEnum.COMMENT_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String permission = this.keyValue.get(this.keysToGet[1]);
		String requestComment = this.keyValue.get(this.keysToGet[2]);
		User user = this.insight.getUser();
		if (user == null) {
			NounMetadata noun = new NounMetadata("User must be signed into an account in order to request a project",
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
		// check user permission for the project
		Integer currentUserPermission = SecurityProjectUtils.getUserProjectPermission(userId, projectId);
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
		Integer currentPendingUserPermission = SecurityProjectUtils.getUserAccessRequestProjectPermission(userId, projectId);
		if(currentPendingUserPermission != null && requestPermission == currentPendingUserPermission) {
			throw new IllegalArgumentException("This user has already requested access to this project with the given permission level");
		}
		// checking to make sure project is discoverable
		boolean canRequest = SecurityProjectUtils.canRequestProject(projectId);
		if (canRequest) {
			String userType = token.getProvider().toString();
			SecurityProjectUtils.setUserAccessRequest(userId, userType, projectId, requestComment, requestPermission);
			sendEmail(user, projectId, permission, requestComment);
			return NounMetadata.getSuccessNounMessage("Successfully requested the project");
		} else {
			return NounMetadata.getErrorNounMessage("Unable to request the project");

		}
	}

	private void sendEmail(User user, String projectId, String permission, String requestComment) {
		String template = getTemplateString();
		if (template !=null && !template.isEmpty()) {
			List<String> projectOwners = SecurityProjectUtils.getProjectOwners(projectId);
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
			if (projectOwners != null && !projectOwners.isEmpty()) {
				String projectName = SecurityProjectUtils.getProjectAliasForId(projectId);
				Session emailSession = SocialPropertiesUtil.getInstance().getEmailSession();
				final String PROJECT_NAME_REPLACEMENT = "$projectName$";
				final String PERMISSION_REPLACEMENT = "$permission$";
				final String USER_NAME_REPLACEMENT = "$userName$";
				final String USER_EMAIL_REPLACEMENT = "$userEmail$";
				final String REQUEST_REASON_COMMENT = "$requestReason$";
				Map<String, String> emailReplacements = SocialPropertiesUtil.getInstance().getEmailStaticProps();
				emailReplacements.put(PROJECT_NAME_REPLACEMENT, projectName);
				emailReplacements.put(PERMISSION_REPLACEMENT, permission);
				emailReplacements.put(USER_NAME_REPLACEMENT, userName);
				emailReplacements.put(USER_EMAIL_REPLACEMENT, userEmail);
				emailReplacements.put(REQUEST_REASON_COMMENT, requestComment);
				String message = EmailUtility.fillEmailComponents(template, emailReplacements);
				EmailUtility.sendEmail(emailSession, projectOwners.toArray(new String[0]), null, null,
						SocialPropertiesUtil.getInstance().getSmtpSender(), "SEMOSS - Project Access Request", message,
						true, null);
			}
		}

	}

	private String getTemplateString() {
		String template = null;
		String templatePath = DIHelper.getInstance().getProperty(Constants.EMAIL_TEMPLATES);
		if (templatePath.endsWith("\\") || templatePath.endsWith("/")) {
			templatePath += REQUEST_PROJECT_EMAIL_TEMPLATE;
		} else {
			templatePath += "/" + REQUEST_PROJECT_EMAIL_TEMPLATE;
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
