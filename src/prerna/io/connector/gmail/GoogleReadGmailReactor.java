package prerna.io.connector.gmail;

import com.google.api.services.gmail.Gmail;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import jakarta.mail.Multipart;
import jakarta.mail.BodyPart;
import jakarta.mail.internet.MimeMessage;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleReadGmailReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleReadGmailReactor.class);
	
	private static final String FROM_KEY = "from";
    private static final String TO_KEY = "to";
    private static final String SUBJECT_KEY = "subject";
    private static final String CONTENT_KEY = "content";
    private static final String SENT_DATE_KEY = "sentDate";
    private static final String RECEIVED_DATE_KEY = "receivedDate";
    private static final String TEXT_PLAIN_MIME = "text/plain";
    private static final String TEXT_HTML_MIME = "text/html";
	
	public GoogleReadGmailReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String id = this.keyValue.get(this.keysToGet[0]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleGmailUtils.getGoogleAccessToken(user);
			Gmail GmailService = GoogleGmailUtils.getGmailServiceUsingToken(accessToken);
			MimeMessage result = GoogleGmailHelper.readEmail(GmailService, id);
			Map<String, Object> map = new LinkedHashMap<>();
			map.put(FROM_KEY, result.getFrom());
	        map.put(TO_KEY, result.getRecipients(jakarta.mail.Message.RecipientType.TO));
	        map.put(SUBJECT_KEY, result.getSubject());
	        map.put(CONTENT_KEY, getBody(result));
	        map.put(SENT_DATE_KEY, result.getSentDate());
	        map.put(RECEIVED_DATE_KEY, result.getReceivedDate());
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage());
		}
		
	}
	
	public static String getBody(MimeMessage message) throws Exception {
	    Object content = message.getContent();
	    if (content instanceof String) {
	        return (String) content;
	    } else if (content instanceof Multipart) {
	        return getTextFromMultipart((Multipart) content);
	    }
	    return null;
	}

	private static String getTextFromMultipart(Multipart multipart) throws Exception {
	    for (int i = 0; i < multipart.getCount(); i++) {
	        BodyPart bodyPart = multipart.getBodyPart(i);
	        if (bodyPart.isMimeType(TEXT_PLAIN_MIME)) {
	            return (String) bodyPart.getContent();
	        } else if (bodyPart.isMimeType(TEXT_HTML_MIME)) {
	            return (String) bodyPart.getContent();
	        } else if (bodyPart.getContent() instanceof Multipart) {
	            String result = getTextFromMultipart((Multipart) bodyPart.getContent());
	            if (result != null) {
	                return result;
	            }
	        }
	    }
	    return null;
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to read the email";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.ID.getKey())) {
	        return "Unique identifier of the Google Email to be read " + ReactorKeysEnum.ID.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}

}

