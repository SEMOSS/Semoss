package prerna.io.connector.gmail;

import java.io.*;
import java.util.*;
import java.util.Base64;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.security.HttpHelperUtility;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import java.nio.charset.StandardCharsets;
import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;

public class GoogleGmailHelper {

	private static final String PARTS = "parts";

	private static final Logger classLogger = LogManager.getLogger(GoogleGmailHelper.class);
	
	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	private static final String USER_ID = "me"; 
	private static final String ID_KEY = "id";
	private static final String PRE_CONTENT_KEY = "pre_content";
	private static final String SUBJECT_KEY = "subject";
	private static final String FROM_KEY = "from";
	private static final String SUBJECT_HEADER = "Subject";
	private static final String FROM_HEADER = "From";
	private static final String TO_HEADER = "To";
    private static final String TO_KEY = "to";
    private static final String CONTENT_KEY = "content";
    private static final String SENT_DATE_KEY = "sentDate";
	private static final String SUCCESS_KEY = "success";
	private static final String HEADER_AUTHORIZATION = "Authorization";
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String BEARER = "Bearer ";
    private static final String GOOGLE_GMAIL_UNREAD_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages?q=is:unread&maxResults=%d";
	private static final String GOOGLE_GMAIL_SUMMARIZE_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages?maxResults=%d";
	private static final String MESSAGES = "messages";
	private static final String SNIPPET = "snippet";
	private static final String GOOGLE_GMAIL_PROFILE_URL = "https://gmail.googleapis.com/gmail/v1/users/me/profile";
	private static final String HISTORY_ID = "historyId";
	private static final String THREADS_TOTAL = "threadsTotal";
	private static final String MESSAGES_TOTAL = "messagesTotal";
	private static final String EMAIL_ADDRESS = "emailAddress";
	private static final String DATE = "Date";
	private static final String DATA = "data";
	private static final String BODY = "body";
	private static final String HEADERS = "headers";
	private static final String PAYLOAD = "payload";
	private static final String GOOGLE_GMAIL_READ_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages/%s";
	private static final String VALUE = "value";
	private static final String NAME = "name";
	private static final String GOOGLE_GMAIL_SEND_URL = "https://gmail.googleapis.com/gmail/v1/users/%s/messages/send";

	public static NounMetadata sendEmail(String accessToken, String messageSubject, String bodyText, String toEmailAddress) throws Exception {
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
		String encodedEmail = Base64.getUrlEncoder().withoutPadding().encodeToString(rawMessageBytes);
		
		Map<String, String> payload = new HashMap<>();
        payload.put("raw", encodedEmail);
        String jsonPayload = gson.toJson(payload);

		try {
			Map<String, String> header = getBearerHeader(accessToken);
			String url = String.format(GOOGLE_GMAIL_SEND_URL, USER_ID);
			String response = HttpHelperUtility.postRequestStringBody(url, header, jsonPayload, null, null, null, null);
			Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
			Map<String, Object> map = new HashMap<>();
			map.put(ID_KEY, json.get(ID_KEY));
			map.put(SUCCESS_KEY, true);
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Failed to send email.");
			throw new SemossPixelException("Failed to send email: " + e.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	public static NounMetadata readEmail(String accessToken, String messageId) throws Exception {
		try {
			Map<String, String> header = getBearerHeader(accessToken);
			String url = String.format(GOOGLE_GMAIL_READ_URL, messageId);
			String response = HttpHelperUtility.getRequest(url, header, null, null, null);
			Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
			Map<String, Object> map = new LinkedHashMap<>();
			Map<String, Object> payload = (Map<String, Object>) json.get(PAYLOAD);
			List<Map<String, String>> headers = (List<Map<String, String>>) payload.get(HEADERS);
			map.put(FROM_KEY, getHeaderValue(headers, FROM_HEADER));
			map.put(TO_KEY, getHeaderValue(headers, TO_HEADER));
			map.put(SUBJECT_KEY, getHeaderValue(headers, SUBJECT_HEADER));
			String body = extractBody(payload);
	        map.put(CONTENT_KEY, body);
			map.put(SENT_DATE_KEY, getHeaderValue(headers, DATE));
	        return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
            classLogger.error("Error reading email", e);
            throw new SemossPixelException("Failed to read email: " + e.getMessage());
		}
		
	}
	
	private static String getHeaderValue(List<Map<String, String>> headers, String name) {
	    for (Map<String, String> header : headers) {
	        if (header.get(NAME).equalsIgnoreCase(name)) {
	            return header.get(VALUE);
	        }
	    }
	    return null;
	}
	
	@SuppressWarnings("unchecked")
	private static String extractBody(Map<String, Object> payload) {
	    String body = extractBodyFromMap(payload);
	    if (body != null && !body.isEmpty()) {
	        return body;
	    }
	    if (payload.get(PARTS) != null) {
	        List<Map<String, Object>> parts = (List<Map<String, Object>>) payload.get(PARTS);
	        for (Map<String, Object> part : parts) {
	            body = extractBody(part);
	            if (body != null && !body.isEmpty()) {
	                return body;
	            }
	        }
	    }
	    return null;
	}
	
	@SuppressWarnings("unchecked")
	private static String extractBodyFromMap(Map<String, Object> map) {
	    if (map.get(BODY) != null) {
	        Map<String, Object> bodyMap = (Map<String, Object>) map.get(BODY);
	        if (bodyMap.get(DATA) != null) {
	            String encoded = (String) bodyMap.get(DATA);
	            try {
	                byte[] decodedBytes = Base64.getUrlDecoder().decode(encoded);
	                return new String(decodedBytes, StandardCharsets.UTF_8);
	            } catch (IllegalArgumentException e) {
	                return null;
	            }
	        }
	    }
	    return null;
	}

	public static NounMetadata getGmailProfileById(String accessToken) throws Exception {
		try {
			Map<String, String> headers = getBearerHeader(accessToken);
			String url = String.format(GOOGLE_GMAIL_PROFILE_URL);
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
			Map<String, Object> map = new LinkedHashMap<>();
			map.put(EMAIL_ADDRESS, json.get(EMAIL_ADDRESS));
	        map.put(MESSAGES_TOTAL, json.get(MESSAGES_TOTAL));
	        map.put(THREADS_TOTAL, json.get(THREADS_TOTAL));
	        map.put(HISTORY_ID, json.get(HISTORY_ID));
	        return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
            classLogger.error("Error getting gmail profile", e);
            throw new SemossPixelException("Failed to get gmail profile: " + e.getMessage());
		}
	}

	@SuppressWarnings("unused")
	public static Boolean deleteEmail(String accessToken, String messageId) {
		try {
			Map<String, String> headers = getBearerHeader(accessToken);
			String url = String.format(GOOGLE_GMAIL_READ_URL, messageId);
			String response = HttpHelperUtility.deleteRequestStringBody(url, headers, null, null, null);
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to delete email.", e.getMessage());
			return false;
		}
	}
	
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> summarizeTopKEmails(String accessToken, int k) throws Exception {
        List<Map<String, Object>> summaries = new ArrayList<>();
        Map<String, String> headers = getBearerHeader(accessToken);
        String url = String.format(GOOGLE_GMAIL_SUMMARIZE_URL, k);
        String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
        Gson gson = new Gson();
        Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
        List<Map<String, Object>> messages = (List<Map<String, Object>>) json.get(MESSAGES);
        if (messages == null) return summaries;
        for (Map<String, Object> msg : messages) {
            String msgId = (String) msg.get(ID_KEY);
            String msgUrl = String.format(GOOGLE_GMAIL_READ_URL, msgId);
            String msgResponse = HttpHelperUtility.getRequest(msgUrl, headers, null, null, null);
            Map<String, Object> msgJson = gson.fromJson(msgResponse, new TypeToken<Map<String, Object>>() {}.getType());
            Map<String, Object> summary = normalizeGmailMessage(msgJson);
            summaries.add(summary);
        }
        return summaries;
    }
	
    @SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getUnreadEmails(String accessToken, int k) throws Exception {
        List<Map<String, Object>> unread = new ArrayList<>();
        Map<String, String> headers = getBearerHeader(accessToken);
        String url = String.format(GOOGLE_GMAIL_UNREAD_URL, k);
        String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
        Gson gson = new Gson();
        Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
        List<Map<String, Object>> messages = (List<Map<String, Object>>) json.get(MESSAGES);
        if (messages == null) return unread;
        for (Map<String, Object> msg : messages) {
            String msgId = (String) msg.get(ID_KEY);
            String msgUrl = String.format(GOOGLE_GMAIL_READ_URL, msgId);
            String msgResponse = HttpHelperUtility.getRequest(msgUrl, headers, null, null, null);
            Map<String, Object> msgJson = gson.fromJson(msgResponse, new TypeToken<Map<String, Object>>() {}.getType());
            Map<String, Object> result = normalizeGmailMessage(msgJson);
            unread.add(result);
        }
        return unread;
    }
    
    @SuppressWarnings("unchecked")
	public static Map<String, Object> normalizeGmailMessage(Map<String, Object> msg) {
        Map<String, Object> map = new HashMap<>();
        String subject = "";
        String from = "";
        Map<String, Object> payload = (Map<String, Object>) msg.get(PAYLOAD);
        if (payload != null) {
            List<Map<String, Object>> headers = (List<Map<String, Object>>) payload.get(HEADERS);
            if (headers != null) {
                for (Map<String, Object> header : headers) {
                    String name = (String) header.get(NAME);
                    if (SUBJECT_HEADER.equalsIgnoreCase(name))
                        subject = (String) header.get(VALUE);
                    if (FROM_HEADER.equalsIgnoreCase(name))
                        from = (String) header.get(VALUE);
                }
            }
        }
        map.put(ID_KEY, msg.get(ID_KEY));
        map.put(PRE_CONTENT_KEY, msg.get(SNIPPET));
        map.put(SUBJECT_KEY, subject);
        map.put(FROM_KEY, from);
        return map;
    }
	
	public static Map<String, String> getBearerHeader(String accessToken) {
        Map<String, String> headers = new HashMap<>();
        headers.put(HEADER_AUTHORIZATION, BEARER + accessToken);
        headers.put(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);
        return headers;
    }

}
