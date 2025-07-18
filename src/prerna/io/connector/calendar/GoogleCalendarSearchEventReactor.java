package prerna.io.connector.calendar;

import java.util.*;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.Event;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleCalendarSearchEventReactor extends AbstractReactor {
	
	public GoogleCalendarSearchEventReactor() {
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
			Event searchedEvent = GoogleCalendarHelper.searchEvent(CalendarService, summary);
			Map<String, Object> mp = new HashMap<>();
			mp.put("id", searchedEvent.getId());
			mp.put("SingleEvent", searchedEvent.getRecurringEventId() == null);
			mp.put("StartTime", searchedEvent.getStart().getDateTime().toStringRfc3339());
			mp.put("EndTime", searchedEvent.getEnd().getDateTime().toStringRfc3339());
			mp.put("Organizer", searchedEvent.getOrganizer().getEmail());
			return new NounMetadata(mp, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
			
		} catch (Exception e) {
			throw new SemossPixelException("Issue with input: " + e.getMessage(), e);
		}
		
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to search event in the Google Calender.";
	}
}
