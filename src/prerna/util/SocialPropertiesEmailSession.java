package prerna.util;

import java.util.Properties;

import javax.mail.PasswordAuthentication;
import javax.mail.Session;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SocialPropertiesEmailSession {

	private static final Logger logger = LogManager.getLogger(SocialPropertiesEmailSession.class);

	private static SocialPropertiesEmailSession instance = null;
	
	private Properties emailProps = null;
	private Session emailSession = null;
	
	public static SocialPropertiesEmailSession getInstance() {
		if(instance != null) {
			return instance;
		}
		
		if(instance == null) {
			instance = new SocialPropertiesEmailSession();
			instance.loadEmailSession();
		}
		
		return instance;
	}
	
	private void loadEmailSession() {
		this.emailProps = SocialPropertiesUtil.getInstance().getEmailProperties();
		if(this.emailProps.isEmpty()) {
			throw new IllegalArgumentException("SMTP properties not defined for this instance. Please reach out to an admin to configure");
		}
		
		String username = this.emailProps.getProperty("username");
		String password = this.emailProps.getProperty("password");

		try {
			if (username != null && password != null) {
				logger.info("Making secured connection to the email server");
				this.emailSession  = Session.getInstance(this.emailProps, new javax.mail.Authenticator() {
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication(username, password);
					}
				});
			} else {
				logger.info("Making connection to the email server");
				this.emailSession = Session.getInstance(this.emailProps);
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occured connecting to the email session defined. Please ensure the proper settings are set for connecting. Detailed error: " + e.getMessage(), e);
		}
	}
	
	public Session getEmailSession() {
		return this.emailSession;
	}
}
