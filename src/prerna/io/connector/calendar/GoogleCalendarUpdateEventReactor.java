package prerna.io.connector.calendar;

import java.util.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleCalendarUpdateEventReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleCalendarUpdateEventReactor.class);
	
	private static final String STATUS_KEY = "status";
	
	public GoogleCalendarUpdateEventReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SUMMARY.getKey(), ReactorKeysEnum.LOCATION.getKey(),
				ReactorKeysEnum.DESCRIPTION.getKey(), ReactorKeysEnum.STARTDATE.getKey(),
				ReactorKeysEnum.ENDDATE.getKey(), ReactorKeysEnum.VIDEO.getKey(), ReactorKeysEnum.EMAIL.getKey(), ReactorKeysEnum.ID.getKey(), 
				ReactorKeysEnum.FREQUENCY.getKey(), ReactorKeysEnum.UNTIL.getKey()};
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String id = this.keyValue.get(this.keysToGet[7]);
		String summary = null;
		String location = null;
		String desc = null;
		String startdatetime = null;
		String enddatetime = null;
		String enablevideo = null;
		String emailsInput = null;
		String frequency = null;
		String until = null;
		if (this.keyValue.get(this.keysToGet[0]) != null && !this.keyValue.get(this.keysToGet[0]).isEmpty()) {
			summary = this.keyValue.get(this.keysToGet[0]);
		}
		if (this.keyValue.get(this.keysToGet[1]) != null && !this.keyValue.get(this.keysToGet[1]).isEmpty()) {
			location = this.keyValue.get(this.keysToGet[1]);
		}
		if (this.keyValue.get(this.keysToGet[2]) != null && !this.keyValue.get(this.keysToGet[2]).isEmpty()) {
			desc = this.keyValue.get(this.keysToGet[2]);
		}
		if (this.keyValue.get(this.keysToGet[3]) != null && !this.keyValue.get(this.keysToGet[3]).isEmpty()) {
			startdatetime = this.keyValue.get(this.keysToGet[3]);
		}
		if (this.keyValue.get(this.keysToGet[4]) != null && !this.keyValue.get(this.keysToGet[4]).isEmpty()) {
			enddatetime = this.keyValue.get(this.keysToGet[4]);
		}
		if (this.keyValue.get(this.keysToGet[5]) != null && !this.keyValue.get(this.keysToGet[5]).isEmpty()) {
			enablevideo = this.keyValue.get(this.keysToGet[5]);
		}
		if (this.keyValue.get(this.keysToGet[6]) != null && !this.keyValue.get(this.keysToGet[6]).isEmpty()) {
			emailsInput = this.keyValue.get(this.keysToGet[6]);
		}
		if (this.keyValue.get(this.keysToGet[8]) != null && !this.keyValue.get(this.keysToGet[8]).isEmpty()) {
			frequency = this.keyValue.get(this.keysToGet[8]);
		}
		if (this.keyValue.get(this.keysToGet[9]) != null && !this.keyValue.get(this.keysToGet[9]).isEmpty()) {
			until = this.keyValue.get(this.keysToGet[9]);
		}
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleCalendarUtils.getGoogleAccessToken(user);
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
			boolean result = GoogleCalendarHelper.updateEvent(accessToken, id, summary, location, desc, startdatetime,
					enddatetime, attendeeEmails, frequency, until, video);
			Map<String, Object> map = new HashMap<>();
			map.put(STATUS_KEY, result);
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage(), e);
		}

	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to update an existing event in the Google Calender.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.SUMMARY.getKey())) {
	        return "Updated event summary or title " + ReactorKeysEnum.SUMMARY.getKey();
	    } else if (key.equals(ReactorKeysEnum.LOCATION.getKey())) {
	        return "Updated location for the event " + ReactorKeysEnum.LOCATION.getKey();
	    } else if (key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
	        return "Updated description of the event " + ReactorKeysEnum.DESCRIPTION.getKey();
	    } else if (key.equals(ReactorKeysEnum.STARTDATE.getKey())) {
	        return "Updated start date and time (RFC3339 format) " + ReactorKeysEnum.STARTDATE.getKey();
	    } else if (key.equals(ReactorKeysEnum.ENDDATE.getKey())) {
	        return "Updated end date and time (RFC3339 format) " + ReactorKeysEnum.ENDDATE.getKey();
	    } else if (key.equals(ReactorKeysEnum.VIDEO.getKey())) {
	        return "Updated video conference link or meeting URL " + ReactorKeysEnum.VIDEO.getKey();
	    } else if (key.equals(ReactorKeysEnum.EMAIL.getKey())) {
	        return "Updated email address of the attendee or organizer " + ReactorKeysEnum.EMAIL.getKey();
	    } else if (key.equals(ReactorKeysEnum.ID.getKey())) {
	        return "Unique identifier of the event to be updated " + ReactorKeysEnum.ID.getKey();
	    } else if (key.equals(ReactorKeysEnum.FREQUENCY.getKey())) {
	        return "Updated Recurrence frequency " + ReactorKeysEnum.FREQUENCY.getKey();
	    } else if (key.equals(ReactorKeysEnum.UNTIL.getKey())) {
	        return "Updated Date until which the event recurs " + ReactorKeysEnum.UNTIL.getKey();
	    } 
	    return super.getDescriptionForKey(key);
	}

}
