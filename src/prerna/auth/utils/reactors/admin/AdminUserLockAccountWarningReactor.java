package prerna.auth.utils.reactors.admin;

import java.util.ArrayList;
import java.util.List;

import javax.mail.Session;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.PasswordRequirements;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.EmailUtility;
import prerna.util.SocialPropertiesUtil;

public class AdminUserLockAccountWarningReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminUserLockAccountWarningReactor.class);
	
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
		for(Object[] emailInfo : listToEmail) {
			String email = (String) emailInfo[0];
			int daysSinceLastLogin = ((Number) emailInfo[1]).intValue();
			
			String message = "Our records show you have not logged into the SEMOSS application for " + daysSinceLastLogin + " days. "
					+ "Your account will be locked if the number of days exceeds " + daysToLock + " days. "
					+ "If you no longer need access to the application, please ignore this email.";
			
			EmailUtility.sendEmail(emailSession, new String[] {email}, null, null, 
					"no-reply@semoss.org", "Lock Account Warning", message, false, null);
			
			emailsSentTo.add(email);
		}
		
		NounMetadata noun = new NounMetadata(emailsSentTo, PixelDataType.CONST_STRING);
		noun.addAdditionalReturn(getSuccess("Emails sent to " + emailsSentTo.size() + " users"));
		return noun;
	}
	
}
