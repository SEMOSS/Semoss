package prerna.io.connector.calendar;

import java.util.*;

import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.Event;
import com.google.api.services.calendar.model.Events;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleCalendarDeleteEventReactor extends AbstractReactor {
	
	public GoogleCalendarDeleteEventReactor() {
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
			boolean result = GoogleCalendarHelper.deleteEvent(CalendarService, id);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
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
	            	if(item.getRecurringEventId() == null) {
	            		return item.getId();
	            	}
	            	else {
	            		return item.getRecurringEventId();
	            	}
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
		return "This reactor is used to delete event in the Google Calender.";
	}

}
