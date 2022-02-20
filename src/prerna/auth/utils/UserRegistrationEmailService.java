package prerna.auth.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import javax.mail.PasswordAuthentication;
import javax.mail.Session;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.PasswordRequirements;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EmailUtility;

public class UserRegistrationEmailService {

	private static final Logger logger = LogManager.getLogger(UserRegistrationEmailService.class);
	
	private static UserRegistrationEmailService instance;
	
	
	private Properties prop = null;
	private Session emailSession = null;
	private String emailTemplatesFolder = "";
	
	private final String EMAIL_TEMPLATES_FOLDER = "emailTemplates";
	private final String REPLACE_LINK = "{{{REPLACE_LINK}}}";
	
	public static UserRegistrationEmailService getInstance() {
		if(instance != null) {
			return instance;
		}

		if(instance == null) {
			synchronized(PasswordRequirements.class) {
				if(instance != null) {
					return instance;
				}
				
				instance = new UserRegistrationEmailService();
				String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
				if(baseFolder.endsWith("/") || baseFolder.endsWith("\\")) {
					instance.emailTemplatesFolder = baseFolder + instance.EMAIL_TEMPLATES_FOLDER + "/";
				} else {
					instance.emailTemplatesFolder = baseFolder + "/" + instance.EMAIL_TEMPLATES_FOLDER + "/";
				}
			}
		}

		return instance;
	}
	
	public void setProperties(Properties prop) {
		this.prop = prop;
		createEmailSession();
	}
	
	public void createEmailSession() {
		//TODO: build out to grab from social.properties
		//TODO: build out to grab from social.properties
		//TODO: build out to grab from social.properties
		//TODO: build out to grab from social.properties

		String smtpHost = "localhost";
		String smtpPort = "1025";
		String username = null;
		String password = null;
		
		// set smtp properties
		Properties props = new Properties();
		props.put("mail.smtp.host", smtpHost);
		props.put("mail.smtp.port", smtpPort);
		if (username != null && password != null) {
			props.put("mail.smtp.auth", true);
			props.put("mail.smtp.starttls.enable", true);
			props.put("mail.smtp.socketFactory.port", smtpPort);
			props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
			// for no man-in-the-middle attacks
			props.put("mail.smtp.ssl.checkserveridentity", true);
			this.emailSession = Session.getInstance(props, new javax.mail.Authenticator() {
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(username, password);
				}
			});
		} else {
			this.emailSession = Session.getInstance(props);
		}
	}
	
	public boolean sendPasswordResetEmail(String recipient, String customUrl) {
		if(this.emailSession == null) {
			createEmailSession();
		}
		String subject = "SEMOSS Reset Password";
		String sender = "no-reply@semoss.org";
		boolean isHtml = true;
		String[] bccRecipients = null;
		String[] attachments = null;

		String[] recipients = new String[] {recipient};

		// construct the message
		String message;
		try {
			message = new String(Files.readAllBytes(Paths.get(this.emailTemplatesFolder + "passReset.html")));
			message = message.replace(this.REPLACE_LINK, customUrl);
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}
		
		// send email
		boolean success = EmailUtility.sendEmail(this.emailSession, recipients, bccRecipients, sender, subject, message, isHtml, attachments);
		return success;
	}
	
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper("/Users/mahkhalil/development/workspace/Semoss_Dev/RDF_Map.prop");
		UserRegistrationEmailService emailInstance = UserRegistrationEmailService.getInstance();
		emailInstance.sendPasswordResetEmail("***REMOVED***", 
				"http://localhost:8080/Monolith_Dev/resetPassword/index.html?token=***REMOVED***");
	}
	
}
