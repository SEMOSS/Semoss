package prerna.io.connector.calendar;

import java.util.*;
import com.google.api.services.calendar.Calendar;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleCalendarFreeBusyEventReactor extends AbstractReactor{

	public GoogleCalendarFreeBusyEventReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.EMAIL.getKey(), ReactorKeysEnum.STARTDATE.getKey(),
				ReactorKeysEnum.ENDDATE.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String email = this.keyValue.get(this.keysToGet[0]);
		String starttime = this.keyValue.get(this.keysToGet[1]);
		String endtime = this.keyValue.get(this.keysToGet[2]);
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleCalendarUtils.getGoogleAccessToken(user);
			Calendar CalendarService = GoogleCalendarUtils.getCalendarServiceUsingToken(accessToken);
			boolean isBusy = GoogleCalendarHelper.isUserBusy(CalendarService, email, starttime, endtime);
			Map<String, Object> map = new HashMap<>();
			map.put("status: ", isBusy);
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new SemossPixelException("Please provide valid input: " + e.getMessage(), e);
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to check a person is free or busy for an event.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.EMAIL.getKey())) {
	        return "Email address of the user whose availability is being checked " + ReactorKeysEnum.EMAIL.getKey();
	    } else if (key.equals(ReactorKeysEnum.STARTDATE.getKey())) {
	        return "Start date and time for the availability check " + ReactorKeysEnum.STARTDATE.getKey();
	    } else if (key.equals(ReactorKeysEnum.ENDDATE.getKey())) {
	        return "End date and time for the availability check " + ReactorKeysEnum.ENDDATE.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}

}
