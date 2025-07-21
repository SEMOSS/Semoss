package prerna.io.connector.calendar;

import java.util.ArrayList;
import java.util.List;

import com.google.api.client.util.DateTime;
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

public class GoogleCalendarListReactor extends AbstractReactor{
	
	public GoogleCalendarListReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.STARTDATE.getKey(), ReactorKeysEnum.ENDDATE.getKey()};
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String startdate = this.keyValue.get(this.keysToGet[0]);
		String enddate = this.keyValue.get(this.keysToGet[1]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleCalendarUtils.getGoogleAccessToken(user);
			Calendar CalendarService = GoogleCalendarUtils.getCalendarServiceUsingToken(accessToken);
			List<List<String>> eventList = getEventList(CalendarService, startdate, enddate);
			return new NounMetadata(eventList, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new SemossPixelException("Issue with input: " + e.getMessage(), e);
		}
	}
	
	public static List<List<String>> getEventList(Calendar service, String startDateTime, String endDateTime) throws Exception {
	    List<List<String>> eventList = new ArrayList<>();
	    DateTime timeMin = new DateTime(startDateTime);
	    DateTime timeMax = new DateTime(endDateTime);

	    String pageToken = null;
	    do {
	        Events events = service.events().list("primary")
	            .setOrderBy("startTime")
	            .setSingleEvents(true)
	            .setTimeMin(timeMin)
	            .setTimeMax(timeMax)
	            .setMaxResults(100)
	            .setPageToken(pageToken)
	            .execute();

	        List<Event> items = events.getItems();
	        if (items != null && !items.isEmpty()) {
	            for (Event item : items) {
	            	List<String> lst = new ArrayList<>();
	                lst.add(item.getSummary());
	                lst.add(item.getId());
	                eventList.add(lst);
	            }
	        }
	        pageToken = events.getNextPageToken();
	    } while (pageToken != null);

	    if (eventList.isEmpty()) {
	        System.out.println("No Events Found In The Given Date Range");
	    }

	    return eventList;
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to get the list of events.";
	}
}
