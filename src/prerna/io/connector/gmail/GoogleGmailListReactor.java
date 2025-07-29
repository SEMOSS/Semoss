package prerna.io.connector.gmail;

import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import prerna.security.HttpHelperUtility;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleGmailListReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleGmailListReactor.class);
	
	private static final String ID_KEY = "id";
	private static final String SUBJECT_KEY = "subject";
	private static final String SUBJECT_HEADER = "Subject";
	private static final String SENT_LABEL = "SENT"; 
	private static final String VALUE = "value";
	private static final String NAME = "name";
	private static final String HEADERS = "headers";
	private static final String PAYLOAD = "payload";
	private static final String MESSAGES = "messages";
	private static final String GOOGLE_GMAIL_MESSAGE_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages/%s";
	private static final String GOOGLE_GMAIL_LIST_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages?labelIds=%s&maxResults=%d";

	public GoogleGmailListReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NUMBER.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String number = this.keyValue.get(this.keysToGet[0]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleGmailUtils.getGoogleAccessToken(user);
			int num = Integer.parseInt(number);
			List<Map<String, Object>> result = getEmailList(accessToken, num);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getEmailList(String accessToken, int k) throws Exception {
        List<Map<String, Object>> emailList = new ArrayList<>();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String url = String.format(GOOGLE_GMAIL_LIST_URL, SENT_LABEL, k);
        Map<String, String> headers = GoogleGmailHelper.getBearerHeader(accessToken);
        String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
        Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
        List<Map<String, Object>> messages = (List<Map<String, Object>>) json.get(MESSAGES);
        if (messages != null) {
            for (Map<String, Object> msg : messages) {
                String msgId = (String) msg.get(ID_KEY);
                String msgUrl = String.format(GOOGLE_GMAIL_MESSAGE_URL, msgId);
                String msgResponse = HttpHelperUtility.getRequest(msgUrl, headers, null, null, null);
                Map<String, Object> msgJson = gson.fromJson(msgResponse, new TypeToken<Map<String, Object>>() {}.getType());
                String subject = "";
                Map<String, Object> payload = (Map<String, Object>) msgJson.get(PAYLOAD);
                if (payload != null) {
                    List<Map<String, Object>> headersList = (List<Map<String, Object>>) payload.get(HEADERS);
                    if (headersList != null) {
                        for (Map<String, Object> header : headersList) {
                            String name = (String) header.get(NAME);
                            if (SUBJECT_HEADER.equalsIgnoreCase(name)) {
                                subject = (String) header.get(VALUE);
                                break;
                            }
                        }
                    }
                }
                Map<String, Object> map = new HashMap<>();
                map.put(ID_KEY, msgJson.get(ID_KEY));
                map.put(SUBJECT_KEY, subject);
                emailList.add(map);
            }
        }
        return emailList;
    }
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to get the list of sent email";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.NUMBER.getKey())) {
	        return "The number of unread Google emails to get. " + ReactorKeysEnum.NUMBER.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}

}
