package prerna.io.connector.gmail;

import java.io.*;
import java.util.*;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import prerna.sablecc2.om.execptions.SemossPixelException;

public class GoogleGmailHelper {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleGmailHelper.class);
	
	private static final String USER_ID = "me"; 
	private static final String ID_KEY = "id";
	private static final String PRE_CONTENT_KEY = "pre_content";
	private static final String SUBJECT_KEY = "subject";
	private static final String FROM_KEY = "from";
	private static final String SUBJECT_HEADER = "Subject";
	private static final String FROM_HEADER = "From";
	private static final String UNREAD_QUERY = "is:unread";
	private static final String MESSAGE_FORMAT_RAW = "raw";

	public static Message sendEmail(Gmail service, String messageSubject, String bodyText, String toEmailAddress) throws Exception {

		Properties props = new Properties();
		Session session = Session.getDefaultInstance(props, null);
		MimeMessage email = new MimeMessage(session);
		email.setFrom(new InternetAddress(USER_ID));
		email.addRecipient(jakarta.mail.Message.RecipientType.TO, new InternetAddress(toEmailAddress));
		email.setSubject(messageSubject);
		email.setText(bodyText);

		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		email.writeTo(buffer);
		byte[] rawMessageBytes = buffer.toByteArray();
		String encodedEmail = Base64.encodeBase64URLSafeString(rawMessageBytes);
		Message message = new Message();
		message.setRaw(encodedEmail);

		try {
			message = service.users().messages().send(USER_ID, message).execute();
			return message;
		} catch (Exception e) {
			classLogger.error("Failed to send email.");
			throw new SemossPixelException("Failed to send email: " + e.getMessage());
		}
	}
	
	public static MimeMessage readEmail(Gmail service, String messageId) throws Exception {
		Message message = service.users().messages().get(USER_ID, messageId).setFormat(MESSAGE_FORMAT_RAW).execute();
		byte[] emailBytes = java.util.Base64.getUrlDecoder().decode(message.getRaw());
		Session session = Session.getDefaultInstance(new Properties(), null);
		MimeMessage mimeMessage = new MimeMessage(session, new ByteArrayInputStream(emailBytes));
		return mimeMessage;
	}

	public static Profile getGmailProfileById(Gmail service) throws Exception {
		return service.users().getProfile(USER_ID).execute();
	}

	public static Boolean deleteEmail(Gmail service, String messageId) {
		try {
			service.users().messages().delete(USER_ID, messageId).execute();
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to delete event.");
			return false;
		}
	}

	public static List<Map<String, Object>> summarizeTopKEmails(Gmail service, int k) throws Exception {
		List<Map<String, Object>> summaries = new ArrayList<>();
		ListMessagesResponse res = service.users().messages().list(USER_ID).setMaxResults((long) k).execute();
		List<Message> messages = res.getMessages();
		if (messages == null)
			return summaries;
		if (messages != null) {
			for (Message msg : messages) {
				Message msgRes = service.users().messages().get(USER_ID, msg.getId()).execute();
				Map<String, Object> summary = normalizeGmailMessage(msgRes);
				summaries.add(summary);
			}
		}
		return summaries;
	}

	public static List<Map<String, Object>> getUnreadEmails(Gmail service, int k) throws Exception {
		List<Map<String, Object>> unread = new ArrayList<>();
		ListMessagesResponse res = service.users().messages().list(USER_ID).setQ(UNREAD_QUERY).setMaxResults((long) k)
				.execute();
		List<Message> messages = res.getMessages();
		if (messages != null) {
			for (Message msg : messages) {
				Message msgRes = service.users().messages().get(USER_ID, msg.getId()).execute();
				Map<String, Object> result = normalizeGmailMessage(msgRes);
				unread.add(result);
			}
		}
		return unread;

	}

	public static Map<String, Object> normalizeGmailMessage(Message msg) {
		Map<String, Object> map = new HashMap<>();
		String subject = "";
		String from = "";
		if (msg.getPayload() != null && msg.getPayload().getHeaders() != null) {
			for (MessagePartHeader header : msg.getPayload().getHeaders()) {
				if (SUBJECT_HEADER.equalsIgnoreCase(header.getName()))
					subject = header.getValue();
				if (FROM_HEADER.equalsIgnoreCase(header.getName()))
					from = header.getValue();
			}
		}
		map.put(ID_KEY, msg.getId());
		map.put(PRE_CONTENT_KEY, msg.getSnippet());
		map.put(SUBJECT_KEY, subject);
		map.put(FROM_KEY, from);
		return map;
	}

}
