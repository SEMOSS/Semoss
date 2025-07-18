package prerna.io.connector.calendar;

import java.util.ArrayList;
import java.util.List;

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

public class GoogleCalendarUpdateEventReactor extends AbstractReactor {
	
	public GoogleCalendarUpdateEventReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SUMMARY.getKey(), ReactorKeysEnum.LOCATION.getKey(),
				ReactorKeysEnum.DESCRIPTION.getKey(), ReactorKeysEnum.STARTDATE.getKey(),
				ReactorKeysEnum.ENDDATE.getKey(), ReactorKeysEnum.VIDEO.getKey(), ReactorKeysEnum.EMAIL.getKey(), ReactorKeysEnum.OLDSUMMARY.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String summary = this.keyValue.get(this.keysToGet[0]);
		String location = this.keyValue.get(this.keysToGet[1]);
		String desc = this.keyValue.get(this.keysToGet[2]);
		String startdatetime = this.keyValue.get(this.keysToGet[3]);
		String enddatetime = this.keyValue.get(this.keysToGet[4]);
		String enablevideo = this.keyValue.get(this.keysToGet[5]);
		String emailsInput = this.keyValue.get(this.keysToGet[6]);
		String oldsummary = this.keyValue.get(this.keysToGet[7]);
		
		
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
			String id = getEventID(CalendarService, oldsummary);
			if (id == null) {
				throw new SemossPixelException("Event not found for summary: " + oldsummary);
			}
			boolean result = GoogleCalendarHelper.updateEvent(CalendarService,id, summary, location, desc, startdatetime,
					enddatetime, attendeeEmails, video);
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
		return "This reactor is used to update an existing event in the Google Calender.";
	}

}
