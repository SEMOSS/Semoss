package prerna.sablecc2.reactor.utils;

import java.util.Properties;

import javax.mail.PasswordAuthentication;
import javax.mail.Session;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.EmailUtility;
import prerna.util.SocialPropertiesEmailSession;

public class SendEmailReactor extends AbstractReactor {

	private static final String SMTP_HOST = "smtpHost";
	private static final String SMTP_PORT = "smtpPort";
	private static final String EMAIL_SUBJECT = "subject";
	private static final String EMAIL_RECEIVER = "to";
	private static final String EMAIL_CC_RECEIVER = "cc";
	private static final String EMAIL_BCC_RECEIVER = "bcc";
	private static final String EMAIL_SENDER = "from";
	private static final String EMAIL_MESSAGE = "message";
	private static final String MESSAGE_HTML = "html";
	private static final String ATTACHMENTS = "attachments";

	public SendEmailReactor() {
		this.keysToGet = new String[] { SMTP_HOST, SMTP_PORT, EMAIL_SUBJECT, EMAIL_SENDER, 
				EMAIL_MESSAGE, MESSAGE_HTML,
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(), 
				EMAIL_RECEIVER, EMAIL_CC_RECEIVER, EMAIL_BCC_RECEIVER, ATTACHMENTS };
	}

	@Override
	public NounMetadata execute() {
		// get pixel inputs
		organizeKeys();
		
		// validate as many inputs first before establishing the email session
		String subject = this.keyValue.get(this.keysToGet[2]);
		if (subject == null) {
			throw new IllegalArgumentException("Need to define " + EMAIL_SUBJECT);
		}
		String sender = this.keyValue.get(this.keysToGet[3]);
		if (sender == null) {
			throw new IllegalArgumentException("Need to define " + EMAIL_SENDER);
		}
		String message = this.keyValue.get(this.keysToGet[4]);
		String messageHtml = this.keyValue.get(this.keysToGet[5]);
		boolean isHtml = false;
		if(messageHtml != null && !(messageHtml = messageHtml.trim()).isEmpty()) {
			isHtml = Boolean.parseBoolean(messageHtml);
		}
		if (message == null && messageHtml == null) {
			throw new IllegalArgumentException("Need to define the email message as " + EMAIL_MESSAGE + " or " + MESSAGE_HTML);
		}
		
		String[] to = getEmailRecipients(EMAIL_RECEIVER);
		String[] cc = getEmailRecipients(EMAIL_CC_RECEIVER);
		String[] bcc = getEmailRecipients(EMAIL_BCC_RECEIVER);

		if(to == null && cc == null && bcc == null) {
			throw new IllegalArgumentException("Need to define " + EMAIL_RECEIVER + " or " + EMAIL_CC_RECEIVER + " or " + EMAIL_BCC_RECEIVER);
		}
		
		Session emailSession = null;
		String smtpHost = this.keyValue.get(this.keysToGet[0]);
		String smtpPort = this.keyValue.get(this.keysToGet[1]);
		if( (smtpHost == null || smtpHost.isEmpty())
				&& (smtpPort == null || smtpPort.isEmpty())) {
			// use the default for the application defined in social.properties
			emailSession = SocialPropertiesEmailSession.getInstance().getEmailSession();
		} else {
			String username = this.keyValue.get(this.keysToGet[6]);
			String password = this.keyValue.get(this.keysToGet[7]);
			emailSession = contrustOneTimeEmailSession(smtpHost, smtpPort, username, password);
		}
		
		// attachments are optional
		String[] attachments = getAttachmentLocations();
		
		// send email
		boolean success = EmailUtility.sendEmail(emailSession, to, cc, bcc, sender, subject, message, isHtml, attachments);
		return new NounMetadata(success, PixelDataType.BOOLEAN);
	}

	/**
	 * Construct the email session for the passed in inputs
	 * @param smtpHost
	 * @param smtpPort
	 * @param username
	 * @param password
	 * @return
	 */
	private Session contrustOneTimeEmailSession(String smtpHost, String smtpPort, String username, String password) {
		if (smtpHost == null) {
			throw new IllegalArgumentException("Need to define " + SMTP_HOST);
		}
		if (smtpPort == null) {
			throw new IllegalArgumentException("Need to define " + SMTP_PORT);
		}
		
		// get session to send email may or may not need username and password
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
			return Session.getInstance(props, new javax.mail.Authenticator() {
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(username, password);
				}
			});
		} else {
			return Session.getInstance(props);
		}
		
	}

	private String[] getEmailRecipients(String recipientKey) {
		GenRowStruct grs = this.store.getNoun(recipientKey);
		if (grs != null) {
			String[] input = new String[grs.size()];
			for (int i = 0; i < input.length; i++) {
				input[i] = grs.getNoun(i).getValue().toString();
			}
			
			if(input.length == 0) {
				return null;
			}
			return input;
		}
		
		return null;
	}

	private String[] getAttachmentLocations() {
		GenRowStruct grs = this.store.getNoun(ATTACHMENTS);
		if (grs != null) {
			String[] input = new String[grs.size()];
			for (int i = 0; i < input.length; i++) {
				input[i] = grs.getNoun(i).getValue().toString();
			}
			return input;
		}
		return null;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SMTP_HOST)) {
			return "The smtp host.";
		} else if (key.equals(SMTP_PORT)) {
			return "The smtp port.";
		} else if (key.equals(EMAIL_MESSAGE)) {
			return "The message of the email to send.";
		} else if (key.equals(EMAIL_RECEIVER)) {
			return "The receipient(s) of the email.";
		} else if (key.equals(EMAIL_SENDER)) {
			return "The email sender.";
		} else if (key.equals(EMAIL_SUBJECT)) {
			return "The subject of the email.";
		} else if (key.equals(ATTACHMENTS)) {
			return "The file path of email attachments";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
