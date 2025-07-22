package prerna.io.connector.calendar;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.Event;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleCalendarRecurringEventReactor extends AbstractReactor {

	public GoogleCalendarRecurringEventReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SUMMARY.getKey(), ReactorKeysEnum.LOCATION.getKey(),
				ReactorKeysEnum.DESCRIPTION.getKey(), ReactorKeysEnum.STARTDATE.getKey(),
				ReactorKeysEnum.ENDDATE.getKey(), ReactorKeysEnum.EMAIL.getKey(), ReactorKeysEnum.FREQUENCY.getKey(),
				ReactorKeysEnum.UNTIL.getKey(), ReactorKeysEnum.VIDEO.getKey()};
		this.keyRequired = new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String summary = this.keyValue.get(this.keysToGet[0]);
		String location = this.keyValue.get(this.keysToGet[1]);
		String desc = this.keyValue.get(this.keysToGet[2]);
		String startdatetime = this.keyValue.get(this.keysToGet[3]);
		String enddatetime = this.keyValue.get(this.keysToGet[4]);
		String emailsInput = this.keyValue.get(this.keysToGet[5]);
		String frequency = this.keyValue.get(this.keysToGet[6]);
		String until = this.keyValue.get(this.keysToGet[7]);
		String enablevideo = this.keyValue.get(this.keysToGet[8]);

		try {
			User user = this.insight.getUser();
			String accessToken = GoogleCalendarUtils.getGoogleAccessToken(user);
			Calendar CalendarService = GoogleCalendarUtils.getCalendarServiceUsingToken(accessToken);
			List<String> attendeeEmails = new ArrayList<>();
			if (emailsInput != null && !emailsInput.isEmpty()) {
				String[] emailArray = emailsInput.split(",");
				for (String email : emailArray) {
					email = email.trim();
					if (!email.isEmpty()) {
						attendeeEmails.add(email);
					}
				}
			}
			boolean video = Boolean.parseBoolean(enablevideo);
			Event recurringEvent = GoogleCalendarHelper.recurringEvent(CalendarService, summary, location, desc,
					startdatetime, enddatetime, attendeeEmails, frequency, until, video);
			Map<String, Object> map = new HashMap<>();
			map.put("id", recurringEvent.getId());
			map.put("link", recurringEvent.getHtmlLink());
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new SemossPixelException("Please provide valid input: " + e.getMessage(), e);
		}

	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to create recurring events in the Google Calender.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.SUMMARY.getKey())) {
	        return "Event summary or title " + ReactorKeysEnum.SUMMARY.getKey();
	    } else if (key.equals(ReactorKeysEnum.LOCATION.getKey())) {
	        return "Location where the event will take place " + ReactorKeysEnum.LOCATION.getKey();
	    } else if (key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
	        return "Detailed description of the event " + ReactorKeysEnum.DESCRIPTION.getKey();
	    } else if (key.equals(ReactorKeysEnum.STARTDATE.getKey())) {
	        return "Event start date and time (RFC3339 format) " + ReactorKeysEnum.STARTDATE.getKey();
	    } else if (key.equals(ReactorKeysEnum.ENDDATE.getKey())) {
	        return "Event end date and time (RFC3339 format) " + ReactorKeysEnum.ENDDATE.getKey();
	    } else if (key.equals(ReactorKeysEnum.EMAIL.getKey())) {
	        return "Email address of the attendee or organizer " + ReactorKeysEnum.EMAIL.getKey();
	    } else if (key.equals(ReactorKeysEnum.FREQUENCY.getKey())) {
	        return "Recurrence frequency (e.g., DAILY, WEEKLY, MONTHLY) " + ReactorKeysEnum.FREQUENCY.getKey();
	    } else if (key.equals(ReactorKeysEnum.UNTIL.getKey())) {
	        return "Date until which the event recurs (RFC3339 format) " + ReactorKeysEnum.UNTIL.getKey();
	    } else if (key.equals(ReactorKeysEnum.VIDEO.getKey())) {
	        return "Video conference link or meeting URL " + ReactorKeysEnum.VIDEO.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}
}
