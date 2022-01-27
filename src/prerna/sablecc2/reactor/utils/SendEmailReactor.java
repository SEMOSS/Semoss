package prerna.sablecc2.reactor.utils;

import java.util.Properties;

import javax.mail.PasswordAuthentication;
import javax.mail.Session;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.EmailUtility;

public class SendEmailReactor extends AbstractReactor {

	private static final String SMTP_HOST = "smtpHost";
	private static final String SMTP_PORT = "smtpPort";
	private static final String EMAIL_SUBJECT = "subject";
	private static final String EMAIL_RECEIVER = "to";
	private static final String EMAIL_SENDER = "from";
	private static final String EMAIL_MESSAGE = "message";
	private static final String MESSAGE_HTML = "html";
	private static final String ATTACHMENTS = "attachments";

	public SendEmailReactor() {
		this.keysToGet = new String[] { SMTP_HOST, SMTP_PORT, EMAIL_SUBJECT, EMAIL_SENDER, EMAIL_MESSAGE, MESSAGE_HTML,
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(), EMAIL_RECEIVER, ATTACHMENTS };
	}

	@Override
	public NounMetadata execute() {
		// get pixel inputs
		organizeKeys();
		String smtpHost = this.keyValue.get(this.keysToGet[0]);
		if (smtpHost == null) {
			throw new IllegalArgumentException("Need to define " + SMTP_HOST);
		}
		String smtpPort = this.keyValue.get(this.keysToGet[1]);
		if (smtpPort == null) {
			throw new IllegalArgumentException("Need to define " + SMTP_PORT);
		}
		String subject = this.keyValue.get(this.keysToGet[2]);
		if (subject == null) {
			throw new IllegalArgumentException("Need to define " + EMAIL_SUBJECT);
		}
		String sender = this.keyValue.get(this.keysToGet[3]);
		if (sender == null) {
			throw new IllegalArgumentException("Need to define " + EMAIL_SENDER);
		}
		String message = this.keyValue.get(this.keysToGet[4]);
		if (message == null) {
			throw new IllegalArgumentException("Need to define " + EMAIL_MESSAGE);
		}
		String messageHtml = this.keyValue.get(this.keysToGet[5]);
		boolean isHtml = false;
		if(messageHtml != null && !messageHtml.isEmpty()) {
			isHtml = Boolean.parseBoolean(messageHtml);
		}
		
		String username = this.keyValue.get(this.keysToGet[6]);
		String password = this.keyValue.get(this.keysToGet[7]);

		String[] recipients = getEmailRecipients();
		// attachments are optional
		String[] attachments = getAttachmentLocations();

		// get session to send email may or may not need username and password
		Session emailSession = null;
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
			emailSession = Session.getInstance(props, new javax.mail.Authenticator() {
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(username, password);
				}
			});
		} else {
			emailSession = Session.getInstance(props);
		}
		// send email
		boolean success = EmailUtility.sendEmail(emailSession, recipients, null, sender, subject, message, isHtml, attachments);
		return new NounMetadata(success, PixelDataType.BOOLEAN);
	}

	private String[] getEmailRecipients() {
		GenRowStruct grs = this.store.getNoun(EMAIL_RECEIVER);
		if (grs != null) {
			String[] input = new String[grs.size()];
			for (int i = 0; i < input.length; i++) {
				input[i] = grs.getNoun(i).getValue().toString();
			}
			return input;
		}
		throw new IllegalArgumentException("Need to define " + EMAIL_RECEIVER);
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
