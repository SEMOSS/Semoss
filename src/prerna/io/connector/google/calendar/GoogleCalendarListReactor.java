package prerna.io.connector.google.calendar;

import java.time.ZoneId;
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
import prerna.util.Utility;

public class GoogleCalendarListReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleCalendarListReactor.class);

	private static final String START_DATE = "startDate";
	private static final String END_DATE = "endDate";

	public GoogleCalendarListReactor() {
		this.keysToGet = new String[] { START_DATE, END_DATE };
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
			ZoneId zoneId = user.getZoneId();
			if (zoneId == null) {
				zoneId = Utility.getApplicationZoneIdObj();
			}
			List<Map<String, Object>> eventList = GoogleCalendarHelper.getEventList(accessToken, startdate, enddate,
					zoneId);
			return new NounMetadata(eventList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"An error occurred retrieving the list of event. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Retrieve Google Calendar events occurring between a specified start and end time.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(START_DATE)) {
			return "Start date and time for retrieving events";
		} else if (key.equals(END_DATE)) {
			return "End date and time for retrieving events";
		}
		return super.getDescriptionForKey(key);
	}
}
