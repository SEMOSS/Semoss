package prerna.sablecc2.reactor.app;

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
import prerna.auth.utils.SecurityDatabaseUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EmailUtility;
import prerna.util.SocialPropertiesUtil;

public class RequestDatabaseReactor extends AbstractReactor {
	private static final String REQUEST_DATABASE_EMAIL_TEMPLATE = "requestDatabase.html";
	private static final Logger classLogger = LogManager.getLogger(RequestDatabaseReactor.class);

	public RequestDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.PERMISSION.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		String permission = this.keyValue.get(this.keysToGet[1]);

		User user = this.insight.getUser();

		boolean security = AbstractSecurityUtils.securityEnabled();
		if (security) {
			if (user == null) {
				NounMetadata noun = new NounMetadata("User must be signed into an account in order to request an app",
						PixelDataType.CONST_STRING, PixelOperationType.ERROR, PixelOperationType.LOGGIN_REQUIRED_ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}

			// throw error if user is anonymous
			if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				throwAnonymousUserError();
			}
		}
		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		String userId = token.getId();
		// check user permission for the app
		if (SecurityDatabaseUtils.getUserDatabasePermission(userId, databaseId) != null) {
			throw new IllegalArgumentException("This user already has access to this database. Please edit the existing permission level.");
		}

		boolean canRequest = SecurityDatabaseUtils.databaseIsDiscoverable(databaseId);
		if (canRequest) {
			sendEmail(user, databaseId, permission);
			return NounMetadata.getSuccessNounMessage("Successfully requested the database");
		} else {
			return NounMetadata.getErrorNounMessage("Unable to request the database");

		}
	}

	private void sendEmail(User user, String databaseId, String permission) {
		String template = getTemplateString();
		if (template !=null && !template.isEmpty()) {
			List<String> databaseOwners = SecurityDatabaseUtils.getDatabaseOwners(databaseId);
			AccessToken token = user.getAccessToken(user.getPrimaryLogin());
			String userName = token.getName() != null ? token.getName(): "";	
			String userEmail = token.getEmail() != null ? token.getEmail(): "";	
			// clean up permission
			if (permission.length() == 1) {
				permission = AccessPermissionEnum.getPermissionValueById(permission);
			}
			if (databaseOwners != null && !databaseOwners.isEmpty()) {
				String databaseName = SecurityDatabaseUtils.getDatabaseAliasForId(databaseId);
				Session emailSession = SocialPropertiesUtil.getInstance().getEmailSession();
				final String DATABASE_NAME_REPLACEMENT = "$databaseName$";
				final String PERMISSION_REPLACEMENT = "$permission$";
				final String USER_NAME_REPLACEMENT = "$userName$";
				final String USER_EMAIL_REPLACEMENT = "$userEmail$";
				Map<String, String> emailReplacements = SocialPropertiesUtil.getInstance().getEmailStaticProps();
				emailReplacements.put(DATABASE_NAME_REPLACEMENT, databaseName);
				emailReplacements.put(PERMISSION_REPLACEMENT, permission);
				emailReplacements.put(USER_NAME_REPLACEMENT, userName);
				emailReplacements.put(USER_EMAIL_REPLACEMENT, userEmail);
				String message = EmailUtility.fillEmailComponents(template, emailReplacements);
				EmailUtility.sendEmail(emailSession, databaseOwners.toArray(new String[0]), null, null,
						SocialPropertiesUtil.getInstance().getSmtpSender(), "SEMOSS - Database Access Request", message,
						true, null);
			}
		}

	}

	private String getTemplateString() {
		String template = null;
		String templatePath = DIHelper.getInstance().getProperty(Constants.EMAIL_TEMPLATES);
		if (templatePath.endsWith("\\") || templatePath.endsWith("/")) {
			templatePath += REQUEST_DATABASE_EMAIL_TEMPLATE;
		} else {
			templatePath += "/" + REQUEST_DATABASE_EMAIL_TEMPLATE;
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
