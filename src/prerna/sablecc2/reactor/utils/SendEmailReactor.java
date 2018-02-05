package prerna.sablecc2.reactor.utils;

import java.util.Properties;

import javax.mail.MessagingException;
import javax.mail.Session;

import prerna.rpa.quartz.jobs.mail.EmailMessage;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class SendEmailReactor extends AbstractReactor {
	
	private static final String SMTP_HOST = "smtpHost";
	private static final String SMTP_PORT = "smtpPort";
	private static final String EMAIL_SUBJECT = "subject";
	private static final String EMAIL_RECEIVER = "to";
	private static final String EMAIL_SENDER = "from";
	private static final String EMAIL_MESSAGE = "message";

	public SendEmailReactor() {
		this.keysToGet = new String[]{SMTP_HOST, SMTP_PORT, EMAIL_SUBJECT, EMAIL_RECEIVER, EMAIL_SENDER, EMAIL_MESSAGE};
	}

	@Override
	public NounMetadata execute() {
		// get pixel inputs
		String smtpHost = getSmtpHost();
		String smtpPort = getSmtpPort();
		String sender = getEmailSender();
		String[] receivers = getEmailReceivers();
		String subject = getEmailSubject();
		String message = getEmailMessage();
		boolean bodyIsHTML = false;

		// get email session
		Properties emailSessionProps = new Properties();
		emailSessionProps.put("mail.smtp.host", smtpHost);
		emailSessionProps.put("mail.smtp.port", smtpPort);
		Session emailSession = Session.getInstance(emailSessionProps);

		// send email
		EmailMessage email = new EmailMessage(sender, receivers, subject, message, bodyIsHTML, emailSession);
		try {
			email.send();
		} catch (MessagingException e) {
			e.printStackTrace();
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}

	private String getEmailMessage() {
		GenRowStruct grs = this.store.getNoun(EMAIL_MESSAGE);
		if (grs != null) {
			String input = grs.getNoun(0).getValue().toString();
			return input;
		}
		throw new IllegalArgumentException("Need to define " + EMAIL_MESSAGE);
	}

	private String getEmailSubject() {
		GenRowStruct grs = this.store.getNoun(EMAIL_SUBJECT);
		if (grs != null) {
			String input = grs.getNoun(0).getValue().toString();
			return input;
		}
		throw new IllegalArgumentException("Need to define " + EMAIL_SUBJECT);
	}

	private String[] getEmailReceivers() {
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

	private String getEmailSender() {
		GenRowStruct grs = this.store.getNoun(EMAIL_SENDER);
		if (grs != null) {
			String input = grs.getNoun(0).getValue().toString();
			return input;
		}
		throw new IllegalArgumentException("Need to define " + EMAIL_SENDER);
	}

	private String getSmtpPort() {
		GenRowStruct grs = this.store.getNoun(SMTP_PORT);
		if (grs != null) {
			String input = grs.getNoun(0).getValue().toString();
			return input;
		}
		throw new IllegalArgumentException("Need to define " + SMTP_PORT);
	}

	private String getSmtpHost() {
		GenRowStruct grs = this.store.getNoun(SMTP_HOST);
		if (grs != null) {
			String input = grs.getNoun(0).getValue().toString();
			return input;
		}
		throw new IllegalArgumentException("Need to define " + SMTP_HOST);
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
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
