package prerna.io.connector.calendar;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleCalendarSearchEventReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleCalendarSearchEventReactor.class);
	
	public GoogleCalendarSearchEventReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey()};
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String id = this.keyValue.get(this.keysToGet[0]);
		
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleCalendarUtils.getGoogleAccessToken(user);
			return GoogleCalendarHelper.searchEvent(accessToken, id);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage(), e);
		}
		
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to search event in the Google Calender.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.ID.getKey())) {
	        return "Unique identifier of the event to search for " + ReactorKeysEnum.ID.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}
}
