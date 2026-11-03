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

public class GoogleMeetGetMeetingReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleMeetGetMeetingReactor.class);

	private static final String EVENT_ID = "eventId";

	public GoogleMeetGetMeetingReactor() {
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
			Map<String, Object> result = GoogleMeetHelper.getMeeting(accessToken, eventId);

			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while fetching meeting", e);
			throw e;

		} catch (Exception e) {
			classLogger.error("Failed to fetch meeting", e);
			throw new SemossPixelException("An error occurred fetching meeting. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Get Google Meet meeting details. This reactor is called after Google Login.";
	}

	@Override
	protected String getDescriptionForKey(String key) {

		if (key.equals(EVENT_ID)) {
			return "Google Calendar event ID";
		}
		return super.getDescriptionForKey(key);
	}
}