package prerna.io.connector.calendar;

import java.util.List;
import java.util.*;

import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.Event;
import com.google.api.services.calendar.model.EventAttendee;
import com.google.api.services.calendar.model.Events;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleCalendarReadEventReactor extends AbstractReactor{
	
	public GoogleCalendarReadEventReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SUMMARY.getKey()};
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String summary = this.keyValue.get(this.keysToGet[0]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleCalendarUtils.getGoogleAccessToken(user);
			Calendar CalendarService = GoogleCalendarUtils.getCalendarServiceUsingToken(accessToken);
			String id = getEventID(CalendarService, summary);
			if (id == null) {
				throw new SemossPixelException("Event not found for summary: " + summary);
			}
			Event result = GoogleCalendarHelper.readEvent(CalendarService, id);
			Map<String, Object> mp = new LinkedHashMap<>();
			mp.put("summary", result.getSummary());
			List<Map<String, Object>> attendeeList = new ArrayList<>();
			if(result.getAttendees() != null) {
				for(EventAttendee i: result.getAttendees()) {
					Map<String, Object> att = new HashMap<>();
					att.put("email", i.getEmail());
					att.put("ResponseStatus", i.getResponseStatus());
					attendeeList.add(att);
				}
			}
			mp.put("attendees", attendeeList);
			mp.put("starttime", result.getStart().getDateTime().toStringRfc3339());
			mp.put("endtime", result.getEnd().getDateTime().toStringRfc3339());
			mp.put("location", result.getLocation());
			mp.put("organizer", result.getOrganizer().getEmail());
			mp.put("hangoutLink", result.getHangoutLink());
			mp.put("htmlLink", result.getHtmlLink());
			mp.put("video", result.getHangoutLink()!=null);
			mp.put("audio", result.getHangoutLink()!=null);
			
			return new NounMetadata(mp, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new SemossPixelException("Issue with input: " + e.getMessage(), e);
		}

	}
	
	public static String getEventID(Calendar service, String summary) throws Exception {
	    String pageToken = null;

	    while (true) {
	        Events events = service.events().list("primary")
	            .setOrderBy("startTime")
	            .setSingleEvents(true)
	            .setMaxResults(100)
	            .setPageToken(pageToken)
	            .execute();

	        List<Event> items = events.getItems();

	        for (Event item : items) {
	            if (item.getSummary() != null && item.getSummary().trim().equalsIgnoreCase(summary.trim())) {
	                return item.getId();
	            }
	        }

	        pageToken = events.getNextPageToken();
	        if (pageToken == null) {
	            break;
	        }
	    }

	    return null;
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to read event in the Google Calender.";
	}
}
