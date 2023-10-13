package prerna.reactor.utils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import prerna.om.FileReference;
import prerna.project.impl.ProjectPropertyEvaluator;
import prerna.reactor.AbstractReactor;
import prerna.reactor.export.mustache.MustacheUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EmailUtility;
import prerna.util.SocialPropertiesUtil;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;

public class SendEmailReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SendEmailReactor.class);

	private static final String SMTP_HOST = "smtpHost";
	private static final String SMTP_PORT = "smtpPort";
	private static final String EMAIL_SUBJECT = "subject";
	private static final String EMAIL_TO_RECEIVER = "to";
	private static final String EMAIL_CC_RECEIVER = "cc";
	private static final String EMAIL_BCC_RECEIVER = "bcc";
	private static final String EMAIL_SENDER = "from";
	private static final String EMAIL_MESSAGE = "message";
	private static final String EMAIL_MESSAGE_ENCODED = "messageEncoded";
	private static final String MESSAGE_HTML = "html";
	private static final String ATTACHMENTS = "attachments";

	public SendEmailReactor() {
		this.keysToGet = new String[] { SMTP_HOST, SMTP_PORT, EMAIL_SUBJECT, EMAIL_SENDER, 
				EMAIL_MESSAGE, EMAIL_MESSAGE_ENCODED, ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.EMAIL_SESSION.getKey(), MESSAGE_HTML, ReactorKeysEnum.MUSTACHE.getKey(), ReactorKeysEnum.MUSTACHE_VARMAP.getKey(),
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(), 
				EMAIL_TO_RECEIVER, EMAIL_CC_RECEIVER, EMAIL_BCC_RECEIVER, ATTACHMENTS };
	}

	@Override
	public NounMetadata execute() {
		// get pixel inputs
		organizeKeys();
		
		// validate as many inputs first before establishing the email session
		String subject = this.keyValue.get(EMAIL_SUBJECT);
		String sender = this.keyValue.get(EMAIL_SENDER);
		if (sender == null) {
			sender = SocialPropertiesUtil.getInstance().getSmtpSender();
			if(sender == null) {
				throw new IllegalArgumentException("Need to define " + EMAIL_SENDER);
			}
		}
		String message = this.keyValue.get(EMAIL_MESSAGE);
		if(message == null || (message=message.trim()).isEmpty()) {
			String messageFileLocation = null;
			try {
				messageFileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
			} catch(IllegalArgumentException e) {
				// ignore
			}
			if(messageFileLocation != null) {
				File messageFile = new File(messageFileLocation);
				if(messageFile.exists() && messageFile.isFile()) {
					try {
						message = FileUtils.readFileToString(messageFile, "UTF-8");
					} catch (IOException e) {
						throw new IllegalArgumentException("Error reading message file with message = " + e.getMessage(), e);
					}
				}
			}
		} else if(Boolean.parseBoolean(this.keyValue.get(EMAIL_MESSAGE_ENCODED) + "")){
			message = Utility.decodeURIComponent(message);
		}
		// depending on the email being used
		// sometimes an email can be sent w/ no message and no subject
		// make sure we have a message to send
//		if (message == null || (message=message.trim()).isEmpty()) {
//			throw new IllegalArgumentException("Need to define the email message as " + EMAIL_MESSAGE + " or passing in file location with message body");
//		}
		boolean isHtml  = Boolean.parseBoolean(this.keyValue.get(MESSAGE_HTML)+"");
		// see if using mustache template format that needs modifications
		if(Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.MUSTACHE.getKey()) + "")) {
			Map<String, Object> variables = mustacheVariables();
			try {
				message = MustacheUtility.compile(message, variables);
			} catch (Exception e) {
				throw new IllegalArgumentException("Invalid mustache template or variables. Detailed error message = " + e.getMessage(), e);
			}
			classLogger.error("Generating final html as: " + message);
		}
		
		String[] to = getEmailRecipients(EMAIL_TO_RECEIVER);
		String[] cc = getEmailRecipients(EMAIL_CC_RECEIVER);
		String[] bcc = getEmailRecipients(EMAIL_BCC_RECEIVER);

		if(to == null && cc == null && bcc == null) {
			throw new IllegalArgumentException("Need to define " + EMAIL_TO_RECEIVER + " or " + EMAIL_CC_RECEIVER + " or " + EMAIL_BCC_RECEIVER);
		}

		Session emailSession = null;

		String smtpHost = this.keyValue.get(SMTP_HOST);
		String smtpPort = this.keyValue.get(SMTP_PORT);
		if( (smtpHost == null || smtpHost.isEmpty())
				&& (smtpPort == null || smtpPort.isEmpty())) {
			// use the email session if the email session is passed into the call 
			emailSession = getEmailSessionFromCall();
			if(emailSession == null) {
				// use the default for the application defined in social.properties
				if(!SocialPropertiesUtil.getInstance().smtpEmailEnabled()) {
					throw new IllegalArgumentException("Need to define an smtp server to utilize this function");
				}
				emailSession = SocialPropertiesUtil.getInstance().getEmailSession();
			}
		} else {
			String username = this.keyValue.get(ReactorKeysEnum.USERNAME.getKey());
			String password = this.keyValue.get(ReactorKeysEnum.PASSWORD.getKey());
			emailSession = contrustOneTimeEmailSession(smtpHost, smtpPort, username, password);
		}
		
		// attachments are optional
		String[] attachments = getAttachmentLocations();
		
		// send email
		boolean success = EmailUtility.sendEmail(emailSession, to, cc, bcc, sender, subject, message, isHtml, attachments);
		return new NounMetadata(success, PixelDataType.BOOLEAN);
	}
	
	/**
	 * Get an email session that is passed in
	 * @return
	 */
	private Session getEmailSessionFromCall() {
		GenRowStruct emailSessionGrs = this.store.getNoun(ReactorKeysEnum.EMAIL_SESSION.getKey());
		if(emailSessionGrs == null || emailSessionGrs.isEmpty()) {
			return null;
		}
		List<NounMetadata> mapInputs = emailSessionGrs.getNounsOfType(PixelDataType.EMAIL_SESSION);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Session) ((ProjectPropertyEvaluator) mapInputs.get(0).getValue()).eval();
		}
		return null;
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
			return Session.getInstance(props, new jakarta.mail.Authenticator() {
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
				NounMetadata noun = grs.getNoun(i);
				if(noun.getOpType().contains(PixelOperationType.FILE_DOWNLOAD)) {
					input[i] = this.insight.getExportFileLocation((String)noun.getValue());
				} else if(noun.getNounType() == PixelDataType.FILE_REFERENCE) {
					FileReference fileRef = (FileReference) noun.getValue();
					input[i] = UploadInputUtility.getFilePath(this.insight, fileRef);
				} else {
					input[i] = grs.getNoun(i).getValue().toString();
				}
			}
			return input;
		}
		return null;
	}
	
	private Map<String, Object> mustacheVariables() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.MUSTACHE_VARMAP.getKey());
		if(grs != null && !grs.isEmpty()) {
			Object obj = grs.get(0);
			if(!(obj instanceof Map)) {
				throw new IllegalArgumentException(ReactorKeysEnum.MUSTACHE_VARMAP.getKey() + " must be a map object");
			}
			return (Map<String, Object>) obj;
		}
		
		List<Object> mapInput = this.curRow.getValuesOfType(PixelDataType.MAP);
		if(mapInput != null && !mapInput.isEmpty()) {
			return (Map<String, Object>) mapInput.get(0);
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
		} else if (key.equals(EMAIL_MESSAGE_ENCODED)) {
			return "Has the message of the email been passed in encoded using <encode></encode> blocks. Default false";
		} else if (key.equals(EMAIL_TO_RECEIVER)) {
			return "The to receipient(s) of the email.";
		} else if (key.equals(EMAIL_CC_RECEIVER)) {
			return "The cc receipient(s) of the email.";
		} else if (key.equals(EMAIL_BCC_RECEIVER)) {
			return "The bcc receipient(s) of the email.";
		} else if (key.equals(EMAIL_SENDER)) {
			return "The email sender.";
		} else if (key.equals(EMAIL_SUBJECT)) {
			return "The subject of the email.";
		} else if (key.equals(ATTACHMENTS)) {
			return "The file path of email attachments";
		} else if (key.equals(MESSAGE_HTML)) {
			return "Boolean is the message is html";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
