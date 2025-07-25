package prerna.io.connector.gmail;

import java.util.List;
import java.util.Map;

import com.google.api.services.gmail.Gmail;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleGetUnreadEmailsReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleGetUnreadEmailsReactor.class);
	
	public GoogleGetUnreadEmailsReactor() {
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
			List<Map<String, Object>> result = GoogleGmailHelper.getUnreadEmails(GmailService, num);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage());
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to get the list of unread email";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.NUMBER.getKey())) {
	        return "The number of unread Google emails to get. " + ReactorKeysEnum.NUMBER.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}

}
