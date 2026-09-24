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

public class GoogleMeetUpdateMeetingReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleMeetUpdateMeetingReactor.class);

	private static final String EVENT_ID = "eventId";
	private static final String SUMMARY = "summary";
	private static final String START_TIME = "startTime";
	private static final String END_TIME = "endTime";
	private static final String ATTENDEES = "attendees";
    private static final String LOCATION = "location";
    private static final String DESCRIPTION = "description";

	public GoogleMeetUpdateMeetingReactor() {
		this.keysToGet = new String[] { EVENT_ID, SUMMARY, START_TIME, END_TIME, ATTENDEES, LOCATION, DESCRIPTION };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String eventId = this.keyValue.get(this.keysToGet[0]);
		String summary = this.keyValue.get(this.keysToGet[1]);
		String startTime = this.keyValue.get(this.keysToGet[2]);
		String endTime = this.keyValue.get(this.keysToGet[3]);
		String attendees = this.keyValue.get(this.keysToGet[4]);
        String location = this.keyValue.get(this.keysToGet[5]);
        String description = this.keyValue.get(this.keysToGet[6]);

		if (eventId == null || eventId.trim().isEmpty()) {
			throw new SemossPixelException("Event ID is required.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);

			Map<String, Object> result = GoogleMeetHelper.updateMeeting(accessToken, eventId, summary, startTime,
					endTime, attendees, location, description);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);

		} catch (SemossPixelException e) {
			classLogger.error("Error while updating meeting", e);
			throw e;

		} catch (Exception e) {
			classLogger.error("Failed to update meeting", e);
			throw new SemossPixelException("An error occurred updating meeting. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Update a Google Meet meeting. This reactor is called after Google Login.";
	}

	@Override
	protected String getDescriptionForKey(String key) {

		if (key.equals(EVENT_ID)) {
			return "Google Calendar event ID";
		} else if (key.equals(SUMMARY)) {
			return "Updated title or summary for the meeting.";
		} else if (key.equals(START_TIME)) {
			return "Updated start date and time of the meeting in RFC3339 format.";
		} else if (key.equals(END_TIME)) {
			return "Updated end date and time of the meeting in RFC3339 format.";
		} else if (key.equals(ATTENDEES)) {
            return "Updated comma-separated email IDs of attendees.";
        } else if (key.equals(LOCATION)) {
            return "Updated location of the meeting.";
        } else if (key.equals(DESCRIPTION)) {
            return "Updated description or agenda of the meeting.";
        }
		return super.getDescriptionForKey(key);
	}
}