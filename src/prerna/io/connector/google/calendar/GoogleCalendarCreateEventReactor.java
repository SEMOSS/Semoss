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

	private static final String NONE = "NONE";
	
	public GoogleCalendarCreateEventReactor() {
		this.keysToGet = new String[] { "summary", "location",
				ReactorKeysEnum.DESCRIPTION.getKey(), "startDate",
				"endDate", "email", "frequency", "until", "video"};
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
	    if (key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
	        return "Detailed description of the event " + ReactorKeysEnum.DESCRIPTION.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}
}
