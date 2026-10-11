package prerna.io.connector.google.meet;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleMeetCreateMeetingReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleMeetCreateMeetingReactor.class);

	private static final String SUMMARY = "summary";
	private static final String START_TIME = "startTime";
	private static final String END_TIME = "endTime";
	private static final String NO_TITLE = "No title";
	private static final String ATTENDEES = "attendees";
    private static final String LOCATION = "location";
    private static final String DESCRIPTION = "description";

	public GoogleMeetCreateMeetingReactor() {
		this.keysToGet = new String[] { SUMMARY, START_TIME, END_TIME, ATTENDEES, LOCATION, DESCRIPTION };
		this.keyRequired = new int[] { 0, 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		String summary = "";
		String startTime = this.keyValue.get(this.keysToGet[1]);
		String endTime = this.keyValue.get(this.keysToGet[2]);
		String attendees = this.keyValue.get(this.keysToGet[3]);
        String location = this.keyValue.get(this.keysToGet[4]);
        String description = this.keyValue.get(this.keysToGet[5]);

		if (startTime == null || startTime.trim().isEmpty()) {
			throw new SemossPixelException("Start time is required.");
		}

		if (endTime == null || endTime.trim().isEmpty()) {
			throw new SemossPixelException("End time is required.");
		}

		if (this.keyValue.get(this.keysToGet[0]) != null && !this.keyValue.get(this.keysToGet[0]).isEmpty()) {
			summary = this.keyValue.get(this.keysToGet[0]);
		} else {
			summary = NO_TITLE;
		}

		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);

			Map<String, Object> result = GoogleMeetHelper.createMeeting(accessToken, summary, startTime, endTime, attendees, location, description);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);

		} catch (SemossPixelException e) {
			classLogger.error("Error while creating a Google Meet meeting", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to create a Google Meet meeting", e);
			throw new SemossPixelException("An error occurred creating the meeting. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Create a Google Meet meeting. This reactor is called after Google Login.";
	}

	@Override
	protected String getDescriptionForKey(String key) {

		if (key.equals(SUMMARY)) {
			return "Title or summary for the meeting.";
		} else if (key.equals(START_TIME)) {
			return "Start date and time of the meeting in RFC3339 format.";
		} else if (key.equals(END_TIME)) {
			return "End date and time of the meeting in RFC3339 format.";
		} else if (key.equals(ATTENDEES)) {
            return "Comma-separated email IDs of attendees.";
        } else if (key.equals(LOCATION)) {
            return "Location of the meeting.";
        } else if (key.equals(DESCRIPTION)) {
            return "Description or agenda of the meeting.";
        }
		return super.getDescriptionForKey(key);
	}
}