package prerna.io.connector.gmail;

import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.ListMessagesResponse;
import com.google.api.services.gmail.model.Message;
import com.google.api.services.gmail.model.MessagePartHeader;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleGmailListReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleGmailListReactor.class);

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
			Gmail GmailService = GoogleGmailUtils.getGmailServiceUsingToken(accessToken);
			int num = Integer.parseInt(number);
			List<Map<String, Object>> result = getEmailList(GmailService, num);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage(), e);
		}
	}

	public static List<Map<String, Object>> getEmailList(Gmail service, int k) throws Exception {
		List<Map<String, Object>> emailList = new ArrayList<>();
		List<String> labels = new ArrayList<>();
		labels.add("SENT");
		ListMessagesResponse res = service.users().messages().list("me").setLabelIds(labels).setMaxResults((long) k).execute();
		List<Message> message = res.getMessages();

		if (message != null) {
			for (Message msg : message) {
				Message fullmsg = service.users().messages().get("me", msg.getId()).execute();

				String subject = "";
				for (MessagePartHeader header : fullmsg.getPayload().getHeaders()) {
					if ("Subject".equalsIgnoreCase(header.getName())) {
						subject = header.getValue();
						break;
					}
				}

				Map<String, Object> map = new HashMap<>();
				map.put("id", fullmsg.getId());
				map.put("subject", subject);
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
