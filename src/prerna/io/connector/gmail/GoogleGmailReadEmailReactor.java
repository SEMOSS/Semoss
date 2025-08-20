package prerna.io.connector.gmail;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleGmailReadEmailReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleGmailReadEmailReactor.class);
	
	public GoogleGmailReadEmailReactor() {
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
			Map<String, Object> retMap = GoogleGmailHelper.readEmail(accessToken, id);
	        return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch(SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred reading the email details. Error message: " + e.getMessage());
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Get the contents of an email based on the id";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.ID.getKey())) {
	        return "Unique identifier of the Google Email to be read " + ReactorKeysEnum.ID.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}

}

