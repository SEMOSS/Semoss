package prerna.auth.utils.reactors.admin;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.mail.Session;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.PasswordRequirements;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EmailUtility;
import prerna.util.SocialPropertiesUtil;

public class AdminUserLockAccountWarningReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminUserLockAccountWarningReactor.class);
	private static final String ACCOUNT_LOCK_WARNING_TEMPLATE = "accountLockWarning.html";
	
	public AdminUserLockAccountWarningReactor() {
		this.keysToGet = new String[] {"days"};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		
		int daysToLock = -1;
		try {
			daysToLock = PasswordRequirements.getInstance().getDaysToLock();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		if(daysToLock < 0) {
			throw new IllegalArgumentException("No value set to lock accounts");
		}
		
		List<String> emailsSentTo = new ArrayList<>();
		
		Session emailSession = SocialPropertiesUtil.getInstance().getEmailSession();
		List<Object[]> listToEmail = adminUtils.getUserEmailsGettingLocked();
		if(!listToEmail.isEmpty()) {
			final String DAYS_SINCE_LAST_LOGIN_REPLACEMENT = "$daysSinceLastLogin$";
			final String DAYS_TO_LOCK_REPLACEMENT = "$daysToLock$";
			
			String template = null;
			String templatePath = DIHelper.getInstance().getProperty(Constants.EMAIL_TEMPLATES);
			if(templatePath.endsWith("\\") || templatePath.endsWith("/")) {
				templatePath += ACCOUNT_LOCK_WARNING_TEMPLATE;
			} else {
				templatePath += "/" + ACCOUNT_LOCK_WARNING_TEMPLATE;
			}
			File templateFile = new File(templatePath);
			if(templateFile.exists() && templateFile.isFile()) {
				try {
					template = FileUtils.readFileToString(templateFile);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					classLogger.info("Using default account lock warning text");
					template = "<html><p>Our records show you have not logged into the SEMOSS application for " + DAYS_SINCE_LAST_LOGIN_REPLACEMENT + " days. "
							+ "Your account will be locked if the number of days exceeds " + DAYS_TO_LOCK_REPLACEMENT + " days. "
							+ "If you no longer need access to the application, please ignore this email.</p></html>";
				}
			} else {
				template = "<html><p>Our records show you have not logged into the SEMOSS application for " + DAYS_SINCE_LAST_LOGIN_REPLACEMENT + " days. "
						+ "Your account will be locked if the number of days exceeds " + DAYS_TO_LOCK_REPLACEMENT + " days. "
						+ "If you no longer need access to the application, please ignore this email.</p></html>";
			}
			for(Object[] emailInfo : listToEmail) {
				String email = (String) emailInfo[0];
				int daysSinceLastLogin = ((Number) emailInfo[1]).intValue();
				
				Map<String, String> emailReplacements = SocialPropertiesUtil.getInstance().getEmailStaticProps();
				emailReplacements.put(DAYS_TO_LOCK_REPLACEMENT, daysToLock + "");
				emailReplacements.put(DAYS_SINCE_LAST_LOGIN_REPLACEMENT, daysSinceLastLogin + "");
				String message = EmailUtility.fillEmailComponents(template, emailReplacements);
				
				EmailUtility.sendEmail(emailSession, new String[] {email}, null, null, 
						"no-reply@semoss.org", "WARNING! Account Locking Soon", message, true, null);
				
				emailsSentTo.add(email);
			}
		}
		
		NounMetadata noun = new NounMetadata(emailsSentTo, PixelDataType.CONST_STRING);
		noun.addAdditionalReturn(getSuccess("Emails sent to " + emailsSentTo.size() + " users"));
		return noun;
	}
	
}
