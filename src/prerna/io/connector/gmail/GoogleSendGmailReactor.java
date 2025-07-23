package prerna.io.connector.gmail;

import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.model.Message;
import java.util.*;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleSendGmailReactor extends AbstractReactor {

	public GoogleSendGmailReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TOEMAIL.getKey(), ReactorKeysEnum.SUBJECT.getKey(), ReactorKeysEnum.BODY.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {

		this.organizeKeys();
		String toemail = this.keyValue.get(this.keysToGet[0]);
		String subject = this.keyValue.get(this.keysToGet[1]);
		String body = this.keyValue.get(this.keysToGet[2]);

		try {
			User user = this.insight.getUser();
			String accessToken = GoogleGmailUtils.getGoogleAccessToken(user);
			Gmail GmailService = GoogleGmailUtils.getGmailServiceUsingToken(accessToken);
			boolean success = false;
			String ID = "id";
			String SUCCESS = "success";
			Map<String, Object> map = new HashMap<>();
			try {
				Message result = GoogleGmailHelper.sendEmail(GmailService, subject, body, toemail);
				if (result != null && result.getId() != null) {
					success = true;
					map.put(ID, result.getId());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			map.put(SUCCESS, success);
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new SemossPixelException("Please provide valid input: " + e.getMessage(), e);
		}

	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to send email.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.TOEMAIL.getKey())) {
	        return "Event address of the receiver " + ReactorKeysEnum.TOEMAIL.getKey();
	    } else if (key.equals(ReactorKeysEnum.SUBJECT.getKey())) {
	        return "Subject of the Email " + ReactorKeysEnum.SUBJECT.getKey();
	    } else if (key.equals(ReactorKeysEnum.BODY.getKey())) {
	        return "Body of the Email " + ReactorKeysEnum.BODY.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}
	
}
