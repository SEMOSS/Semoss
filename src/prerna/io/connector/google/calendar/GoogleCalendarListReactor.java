package prerna.io.connector.google.calendar;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleCalendarListReactor extends AbstractReactor{

	private static final Logger classLogger = LogManager.getLogger(GoogleCalendarListReactor.class);
	
	public GoogleCalendarListReactor() {
		this.keysToGet = new String[] { "startDate", "endDate" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String startdate = this.keyValue.get(this.keysToGet[0]);
		String enddate = this.keyValue.get(this.keysToGet[1]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			List<Map<String, Object>> eventList = GoogleCalendarHelper.getEventList(accessToken, startdate, enddate);
			return new NounMetadata(eventList, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch(SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred retrieving the list of event. Error message: " + e.getMessage());
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Retrieve Google Calendar events occurring between a specified start and end time.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals("startDate")) {
	        return "Start date and time for retrieving events";
	    } else if (key.equals("endDate")) {
	        return "End date and time for retrieving events";
	    }
	    return super.getDescriptionForKey(key);
	}
}
