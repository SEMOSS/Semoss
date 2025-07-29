package prerna.io.connector.gmail;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleReadGmailReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleReadGmailReactor.class);
	
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
			return GoogleGmailHelper.readEmail(accessToken, id);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage());
		}
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

