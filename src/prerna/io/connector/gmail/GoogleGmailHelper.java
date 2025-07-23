package prerna.io.connector.gmail;

import java.io.*;
import java.util.*;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.*;
import org.apache.commons.codec.binary.Base64;
import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;

public class GoogleGmailHelper {

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
			e.printStackTrace();
		}
		return null;
	}
	
	public static MimeMessage readEmail(Gmail service, String messageId) throws Exception {
		Message message = service.users().messages().get("me", messageId).setFormat("raw").execute();
		byte[] emailBytes = java.util.Base64.getUrlDecoder().decode(message.getRaw());
		Session session = Session.getDefaultInstance(new Properties(), null);
		MimeMessage mimeMessage = new MimeMessage(session, new ByteArrayInputStream(emailBytes));
		return mimeMessage;
	}

	public static Profile getGmailProfileById(Gmail service, String userId) throws Exception {
		return service.users().getProfile(userId).execute();
	}

	public static Boolean deleteEmail(Gmail service, String messageId) {
		try {
			service.users().messages().delete("me", messageId).execute();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public static List<Map<String, Object>> summarizeTopKEmails(Gmail service, int k) throws Exception {
		List<Map<String, Object>> summaries = new ArrayList<>();
		ListMessagesResponse res = service.users().messages().list("me").setMaxResults((long) k).execute();
		List<Message> messages = res.getMessages();
		if (messages == null)
			return summaries;

		for (Message msg : messages) {
			Message msgRes = service.users().messages().get("me", msg.getId()).execute();
			Map<String, Object> summary = normalizeGmailMessage(msgRes);
			summaries.add(summary);
		}
		return summaries;
	}

	public static List<Map<String, Object>> getUnreadEmails(Gmail service, int k) throws Exception {
		List<Map<String, Object>> unread = new ArrayList<>();
		ListMessagesResponse res = service.users().messages().list("me").setQ("is:unread").setMaxResults((long) k)
				.execute();
		List<Message> messages = res.getMessages();

		for (Message msg : messages) {
			Message msgRes = service.users().messages().get("me", msg.getId()).execute();
			Map<String, Object> result = normalizeGmailMessage(msgRes);
			unread.add(result);
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

	public static Label createGmailLabel(Gmail service, Label name) throws Exception {
		Label res = service.users().labels().create("me", name).execute();
		return res;

	}

	public static Void deleteGmailLabel(Gmail service, String LabelId) throws Exception {
		Void res = service.users().labels().delete("me", LabelId).execute();
		return res;
	}

	public static List<Label> listGmailLabels(Gmail service) throws Exception {
		ListLabelsResponse res = service.users().labels().list("me").execute();
		List<Label> labels = res.getLabels();
		return labels;
	}

}
