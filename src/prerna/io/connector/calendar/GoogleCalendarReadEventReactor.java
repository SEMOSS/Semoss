package prerna.io.connector.calendar;

import java.util.List;
import java.util.*;

import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.Event;
import com.google.api.services.calendar.model.EventAttendee;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleCalendarReadEventReactor extends AbstractReactor{
	
	public GoogleCalendarReadEventReactor() {
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
			Calendar CalendarService = GoogleCalendarUtils.getCalendarServiceUsingToken(accessToken);
			Event result = GoogleCalendarHelper.readEvent(CalendarService, id);
			Map<String, Object> map = new LinkedHashMap<>();
			map.put("summary", result.getSummary());
			List<Map<String, Object>> attendeeList = new ArrayList<>();
			if(result.getAttendees() != null) {
				for(EventAttendee i: result.getAttendees()) {
					Map<String, Object> att = new HashMap<>();
					att.put("email", i.getEmail());
					att.put("responseStatus", i.getResponseStatus());
					attendeeList.add(att);
				}
			}
			map.put("attendees", attendeeList);
			map.put("starttime", result.getStart().getDateTime().toStringRfc3339());
			map.put("endtime", result.getEnd().getDateTime().toStringRfc3339());
			map.put("location", result.getLocation());
			map.put("organizer", result.getOrganizer().getEmail());
			map.put("hangoutLink", result.getHangoutLink());
			map.put("htmlLink", result.getHtmlLink());
			map.put("video", result.getHangoutLink()!=null);
			map.put("audio", result.getHangoutLink()!=null);
			
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new SemossPixelException("Please provide valid input: " + e.getMessage(), e);
		}

	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to read event in the Google Calender.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.ID.getKey())) {
	        return "Unique identifier of the Google Calendar event to be read " + ReactorKeysEnum.ID.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}
}
