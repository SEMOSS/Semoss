package prerna.io.connector.google.calendar;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GoogleCalendarUpdateEventReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleCalendarUpdateEventReactor.class);
	
	private static final String SUMMARY = "summary";
	private static final String LOCATION = "location";
	private static final String START_DATE = "startDate";
	private static final String END_DATE = "endDate";
	private static final String VIDEO = "video";
	private static final String EMAIL = "email";
	private static final String FREQUENCY = "frequency";
	private static final String UNTIL = "until";
	private static final String STATUS_KEY = "status";
	
	public GoogleCalendarUpdateEventReactor() {
		this.keysToGet = new String[] { SUMMARY, LOCATION,
				ReactorKeysEnum.DESCRIPTION.getKey(), START_DATE,
				END_DATE, VIDEO, EMAIL, ReactorKeysEnum.ID.getKey(), 
				FREQUENCY, UNTIL };
		this.keyRequired = new int[] { 0, 0, 0, 1, 1, 0, 0, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String startdatetime = this.keyValue.get(this.keysToGet[3]);
		String enddatetime = this.keyValue.get(this.keysToGet[4]);
		String id = this.keyValue.get(this.keysToGet[7]);
		String summary = null;
		String location = null;
		String desc = null;
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
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
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
			
			ZoneId zoneId = user.getZoneId();
			if(zoneId == null) {
				zoneId = Utility.getApplicationZoneIdObj();
			}
			
			boolean video = Boolean.parseBoolean(enablevideo);
			boolean result = GoogleCalendarHelper.updateEvent(accessToken, id, summary, location, desc, startdatetime,
					enddatetime, zoneId, attendeeEmails, frequency, until, video);
			Map<String, Object> map = new HashMap<>();
			map.put(STATUS_KEY, result);
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch(SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred updating the event. Error message: " + e.getMessage());
		}

	}

	@Override
	public String getReactorDescription() {
		return "Update an existing event in Google Calender";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(SUMMARY)) {
	        return "Update the event's title or summary";
	    } else if (key.equals(LOCATION)) {
	        return "Update the event's location";
	    } else if (key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
	        return "Update the detailed description of the event";
	    } else if (key.equals(START_DATE)) {
	        return "Update the start date and time of the event";
	    } else if (key.equals(END_DATE)) {
	        return "Update the end date and time of the event";
	    } else if (key.equals(VIDEO)) {
	        return "Update the video conferencing details for the event";
	    } else if (key.equals(EMAIL)) {
	        return "Update the email address of the organizer or attendees";
	    } else if (key.equals(ReactorKeysEnum.ID.getKey())) {
	        return "Unique identifier of the event to be updated";
	    } else if (key.equals(FREQUENCY)) {
	        return "Update the recurrence frequency of the event";
	    } else if (key.equals(UNTIL)) {
	        return "Update the end date for the recurring event";
	    }
	    return super.getDescriptionForKey(key);
	}

}
