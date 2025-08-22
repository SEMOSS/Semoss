package prerna.io.connector.google.calendar;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleCalendarCreateEventReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleCalendarCreateEventReactor.class);

	private static final String SUMMARY = "summary";
	private static final String LOCATION = "location";
	private static final String START_DATE = "startDate";
	private static final String END_DATE = "endDate";
	private static final String EMAIL = "email";
	private static final String FREQUENCY = "frequency";
	private static final String UNTIL = "until";
	private static final String VIDEO = "video";
	private static final String NONE = "NONE";
	
	public GoogleCalendarCreateEventReactor() {
		this.keysToGet = new String[] { SUMMARY, LOCATION,
				ReactorKeysEnum.DESCRIPTION.getKey(), START_DATE,
				END_DATE, EMAIL, FREQUENCY, UNTIL, VIDEO};
		this.keyRequired = new int[] { 1, 1, 1, 1, 1, 1, 0, 0, 1 };
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
		String frequency = null;
		String until = null;
		String enablevideo = this.keyValue.get(this.keysToGet[8]);
		
		if (this.keyValue.get(this.keysToGet[6]) != null && !this.keyValue.get(this.keysToGet[6]).isEmpty()) {
			frequency = this.keyValue.get(this.keysToGet[6]);
		}
		if (this.keyValue.get(this.keysToGet[7]) != null && !this.keyValue.get(this.keysToGet[7]).isEmpty()) {
			until = this.keyValue.get(this.keysToGet[7]);
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
			boolean video = Boolean.parseBoolean(enablevideo);
			boolean isRecurring = frequency != null && !frequency.isEmpty() && !frequency.equals(NONE);
			if (!isRecurring) {
				Map<String, Object> result = GoogleCalendarHelper.createEvent(accessToken, summary, location, desc, startdatetime,
						enddatetime, attendeeEmails, video);
			    return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
			}
			else {
				Map<String, Object> result = GoogleCalendarHelper.recurringEvent(accessToken, summary, location, desc,
						startdatetime, enddatetime, attendeeEmails, frequency, until, video);
			    return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
			}
		} catch(SemossPixelException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred creating the event. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Create an event (non-recurring or recurring event) in Google Calender";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(SUMMARY)) {
	        return "Summary or title of the event";
	    } else if (key.equals(LOCATION)) {
	        return "Location where the event will take place";
	    } else if (key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
	        return "Detailed description of the event " + ReactorKeysEnum.DESCRIPTION.getKey();
	    } else if (key.equals(START_DATE)) {
	        return "Date and time when the event starts";
	    } else if (key.equals(END_DATE)) {
	        return "Date and time when the event ends";
	    } else if (key.equals(EMAIL)) {
	        return "Email address of the event organizer or attendee";
	    } else if (key.equals(FREQUENCY)) {
	        return "Frequency of the recurring event (e.g., daily, weekly)";
	    } else if (key.equals(UNTIL)) {
	        return "End date for the recurring event";
	    } else if (key.equals(VIDEO)) {
	        return "Video conferencing details for the event";
	    }
	    return super.getDescriptionForKey(key);
	}
}
