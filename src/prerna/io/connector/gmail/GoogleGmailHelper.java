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

public class GoogleGmailHelper {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleGmailHelper.class);

	public static Message sendEmail(Gmail service, String messageSubject, String bodyText, String toEmailAddress) throws Exception {

		Properties props = new Properties();
		Session session = Session.getDefaultInstance(props, null);
		MimeMessage email = new MimeMessage(session);
		email.setFrom(new InternetAddress("me"));
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
			message = service.users().messages().send("me", message).execute();
			return message;
		} catch (Exception e) {
			classLogger.error("Failed to send email.", e);
			throw e;
		}
	}
	
	public static MimeMessage readEmail(Gmail service, String messageId) throws Exception {
		Message message = service.users().messages().get("me", messageId).setFormat("raw").execute();
		byte[] emailBytes = java.util.Base64.getUrlDecoder().decode(message.getRaw());
		Session session = Session.getDefaultInstance(new Properties(), null);
		MimeMessage mimeMessage = new MimeMessage(session, new ByteArrayInputStream(emailBytes));
		return mimeMessage;
	}

	public static Profile getGmailProfileById(Gmail service) throws Exception {
		return service.users().getProfile("me").execute();
	}

	public static Boolean deleteEmail(Gmail service, String messageId) {
		try {
			service.users().messages().delete("me", messageId).execute();
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to delete event.");
			return false;
		}
	}

	public static List<Map<String, Object>> summarizeTopKEmails(Gmail service, int k) throws Exception {
		List<Map<String, Object>> summaries = new ArrayList<>();
		ListMessagesResponse res = service.users().messages().list("me").setMaxResults((long) k).execute();
		List<Message> messages = res.getMessages();
		if (messages == null)
			return summaries;
		if (messages != null) {
			for (Message msg : messages) {
				Message msgRes = service.users().messages().get("me", msg.getId()).execute();
				Map<String, Object> summary = normalizeGmailMessage(msgRes);
				summaries.add(summary);
			}
		}
		return summaries;
	}

	public static List<Map<String, Object>> getUnreadEmails(Gmail service, int k) throws Exception {
		List<Map<String, Object>> unread = new ArrayList<>();
		ListMessagesResponse res = service.users().messages().list("me").setQ("is:unread").setMaxResults((long) k)
				.execute();
		List<Message> messages = res.getMessages();
		if (messages != null) {
			for (Message msg : messages) {
				Message msgRes = service.users().messages().get("me", msg.getId()).execute();
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
				if ("Subject".equalsIgnoreCase(header.getName()))
					subject = header.getValue();
				if ("From".equalsIgnoreCase(header.getName()))
					from = header.getValue();
			}
		}
		map.put("id", msg.getId());
		map.put("snippet", msg.getSnippet());
		map.put("subject", subject);
		map.put("from", from);
		return map;
	}

}
