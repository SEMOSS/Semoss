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

public class GoogleMeetDeleteMeetingReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleMeetDeleteMeetingReactor.class);

	private static final String EVENT_ID = "eventId";

	public GoogleMeetDeleteMeetingReactor() {
		this.keysToGet = new String[] { EVENT_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String eventId = this.keyValue.get(this.keysToGet[0]);
		if (eventId == null || eventId.trim().isEmpty()) {
			throw new SemossPixelException("Event ID is required.");
		}
		
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);

			Map<String, Object> result = GoogleMeetHelper.deleteMeeting(accessToken, eventId);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
			
		} catch (SemossPixelException e) {
			classLogger.error("Error while deleting meeting", e);
			throw e;
			
		} catch (Exception e) {
			classLogger.error("Failed to delete meeting", e);
			throw new SemossPixelException("An error occurred deleting meeting. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Delete a Google Meet meeting. This reactor is called after Google Login.";
	}

	@Override
	protected String getDescriptionForKey(String key) {

		if (key.equals(EVENT_ID)) {
			return "Google Calendar event ID";
		}
		return super.getDescriptionForKey(key);
	}
}