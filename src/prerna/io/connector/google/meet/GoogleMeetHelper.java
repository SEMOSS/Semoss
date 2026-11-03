package prerna.io.connector.google.meet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.io.connector.google.GoogleLoginUtils;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;

public class GoogleMeetHelper {
	private static final Logger classLogger = LogManager.getLogger(GoogleMeetHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String START = "start";
	private static final String END = "end";
	private static final String START_TIME = "startTime";
	private static final String END_TIME = "endTime";
	private static final String DATE_TIME = "dateTime";
	private static final String LOCATION = "location";
	private static final String DESCRIPTION = "description";
	private static final String ATTENDEES = "attendees";
	private static final String EMAIL = "email";

	private static final String SUMMARY = "summary";
	private static final String BASE_URL = "https://www.googleapis.com/calendar/v3/calendars/primary/events";
	private static final String SUCCESS_KEY = "success";
	private static final String MEETINGS_KEY = "meetings";
	private static final String SLASH_KEY = "/";

	private static final String CONFERENCE_DATA = "conferenceData";
	private static final String CONFERENCE_SOLUTION_KEY = "conferenceSolutionKey";
	private static final String TYPE = "type";
	private static final String HANGOUTS_MEET = "hangoutsMeet";
	private static final String CREATE_REQUEST = "createRequest";
	private static final String REQUEST_ID = "requestId";
	private static final String LINK = "link";
	private static final String HANGOUT_LINK = "hangoutLink";
	private static final String ID = "id";

	private GoogleMeetHelper() {
	}

	/**
	 * Creates a Google Meet meeting using Google Calendar API.
	 *
	 * @param accessToken    OAuth access token for authentication.
	 * @param summary        Title or summary for the meeting.
	 * @param startDateTime  Start date and time of the meeting.
	 * @param endDateTime    End date and time of the meeting.
	 * @param attendeesInput Comma-separated email IDs of the attendees.
	 * @param location       Location of the meeting.
	 * @param description    Description or agenda of the meeting.
	 *
	 * @return Map containing meeting details like eventId and meeting link.
	 *
	 * @throws Exception if meeting creation fails.
	 */
	public static Map<String, Object> createMeeting(String accessToken, String summary, String startTime,
			String endTime, String attendeesInput, String location, String description) throws Exception {
		try {
			String url = BASE_URL + "?conferenceDataVersion=1";

			Map<String, Object> event = new HashMap<>();
			event.put(SUMMARY, summary);

			Map<String, Object> start = new HashMap<>();
			start.put(DATE_TIME, startTime);
			event.put(START, start);

			Map<String, Object> end = new HashMap<>();
			end.put(DATE_TIME, endTime);
			event.put(END, end);

			if (location != null && !location.trim().isEmpty()) {
				event.put(LOCATION, location);
			}

			if (description != null && !description.trim().isEmpty()) {
				event.put(DESCRIPTION, description);
			}

			if (attendeesInput != null && !attendeesInput.trim().isEmpty()) {
				List<Map<String, String>> attendees = new ArrayList<>();
				String[] emails = attendeesInput.split(",");
				for (String email : emails) {
					email = email.trim();
					if (!email.isEmpty()) {
						Map<String, String> attendee = new HashMap<>();
						attendee.put(EMAIL, email);
						attendees.add(attendee);
					}
				}
				event.put(ATTENDEES, attendees);
			}

			Map<String, Object> conferenceSolutionKey = new HashMap<>();
			conferenceSolutionKey.put(TYPE, HANGOUTS_MEET);

			Map<String, Object> createConferenceRequest = new HashMap<>();
			createConferenceRequest.put(REQUEST_ID, UUID.randomUUID().toString());
			createConferenceRequest.put(CONFERENCE_SOLUTION_KEY, conferenceSolutionKey);

			Map<String, Object> conferenceData = new HashMap<>();
			conferenceData.put(CREATE_REQUEST, createConferenceRequest);
			event.put(CONFERENCE_DATA, conferenceData);

			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);

			String jsonBody = GSON.toJson(event);
			String response = HttpHelperUtility.postRequestStringBody(url, headers, jsonBody,
					ContentType.APPLICATION_JSON, null, null, null);

			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());

			Map<String, Object> result = new HashMap<>();
			result.put(Constants.USER_MAP_ID, json.get(ID));
			result.put(SUCCESS_KEY, true);

			try {
				Map<String, Object> confData = (Map<String, Object>) json.get(CONFERENCE_DATA);

				if (confData != null) {
					List<Map<String, Object>> entryPoints = (List<Map<String, Object>>) confData.get("entryPoints");

					if (entryPoints != null) {
						for (Map<String, Object> entry : entryPoints) {
							if ("video".equals(entry.get("entryPointType"))) {
								result.put("meetLink", entry.get("uri"));
								break;
							}
						}
					}
				}
			} catch (Exception e) {
				classLogger.warn("Meet link not found in response");
			}

			return result;

		} catch (Exception e) {
			classLogger.error("Error creating Google Meet meeting", e);
			throw e;
		}
	}

	/**
	 * Lists all Google Meet meetings (Calendar events with conferencing enabled).
	 *
	 * @param accessToken OAuth access token.
	 *
	 * @return List of meetings with details like summary, time, and Meet link.
	 *
	 * @throws Exception if listing fails.
	 */
	public static Map<String, Object> listMeetings(String accessToken) throws Exception {
		try {
			String url = BASE_URL;

			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);

			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());

			List<Map<String, Object>> items = (List<Map<String, Object>>) json.get("items");
			List<Map<String, Object>> meetings = new ArrayList<>();

			if (items != null) {
				for (Map<String, Object> event : items) {
					// Only include events with Meet link
					if (event.get(HANGOUT_LINK) == null) {
						continue;
					}
					
					Map<String, Object> meeting = new HashMap<>();
					meeting.put(ID, event.get(ID));
					meeting.put(SUMMARY, event.get(SUMMARY));

					Map<String, Object> start = (Map<String, Object>) event.get(START);
					if (start != null) {
						meeting.put(START_TIME, start.get(DATE_TIME));
					}

					Map<String, Object> end = (Map<String, Object>) event.get(END);
					if (end != null) {
						meeting.put(END_TIME, end.get(DATE_TIME));
					}
					meeting.put(LINK, event.get(HANGOUT_LINK));
					meetings.add(meeting);
				}
			}

			Map<String, Object> result = new HashMap<>();
			result.put(MEETINGS_KEY, meetings);
			result.put(SUCCESS_KEY, true);

			return result;

		} catch (Exception e) {
			classLogger.error("Error listing meetings", e);
			throw e;
		}
	}

	/**
	 * Retrieves a Google Meet meeting (Google Calendar event with conferencing) by
	 * ID.
	 *
	 * @param accessToken OAuth access token.
	 * @param eventId     ID of the meeting.
	 *
	 * @return Map containing meeting details such as summary, time, and Meet link.
	 *
	 * @throws Exception if retrieval fails.
	 */
	public static Map<String, Object> getMeeting(String accessToken, String eventId) throws Exception {
		try {
			String url = BASE_URL + SLASH_KEY + eventId;

			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);

			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());

			Map<String, Object> result = new HashMap<>();
			if (json.get(HANGOUT_LINK) == null) {
				throw new SemossPixelException("This event is not a Google Meet meeting.");
			}

			result.put(ID, json.get(ID));
			result.put(SUMMARY, json.get(SUMMARY));

			Map<String, Object> start = (Map<String, Object>) json.get(START);
			if (start != null) {
				result.put(START_TIME, start.get(DATE_TIME));
			}
			
			Map<String, Object> end = (Map<String, Object>) json.get(END);
			if (end != null) {
				result.put(END_TIME, end.get(DATE_TIME));
			}
			result.put(LINK, json.get(HANGOUT_LINK));
			result.put(SUCCESS_KEY, true);

			return result;

		} catch (Exception e) {
			classLogger.error("Error fetching meeting", e);
			throw e;
		}
	}

	/**
	 * Updates an existing Google Meet meeting.
	 *
	 * @param accessToken    OAuth access token.
	 * @param eventId        ID of the meeting to update.
	 * @param summary        Updated meeting title.
	 * @param startDateTime  Updated start time.
	 * @param endDateTime    Updated end time.
	 * @param attendeesInput Comma-separated email IDs of the attendees.
	 * @param location       Location of the meeting.
	 * @param description    Description or agenda of the meeting.
	 *
	 * @return Map containing updated meeting details.
	 *
	 * @throws Exception if update fails.
	 */
	public static Map<String, Object> updateMeeting(String accessToken, String eventId, String summary,
			String startTime, String endTime, String attendeesInput, String location, String description)
			throws Exception {
		try {
			String url = BASE_URL + SLASH_KEY + eventId;

			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String existingResponse = HttpHelperUtility.getRequest(url, headers, null, null, null);

			// checking if event is a meeting
			Map<String, Object> existingEvent = GSON.fromJson(existingResponse, new TypeToken<Map<String, Object>>() {
			}.getType());

			Object meetLink = existingEvent.get(HANGOUT_LINK);
			if (meetLink == null || meetLink.toString().isEmpty()) {
				throw new SemossPixelException("This event is not a Google Meet meeting.");
			}

			Map<String, Object> event = new HashMap<>();
			if (summary != null && !summary.isEmpty()) {
				event.put(SUMMARY, summary);
			}
			
			if (startTime != null) {
				Map<String, Object> start = new HashMap<>();
				start.put(DATE_TIME, startTime);
				event.put(START, start);
			}
			
			if (endTime != null) {
				Map<String, Object> end = new HashMap<>();
				end.put(DATE_TIME, endTime);
				event.put(END, end);
			}
			
			if (location != null && !location.trim().isEmpty()) {
				event.put(LOCATION, location);
			}
			
			if (description != null && !description.trim().isEmpty()) {
				event.put(DESCRIPTION, description);
			}
			
			if (attendeesInput != null && !attendeesInput.trim().isEmpty()) {
				List<Map<String, String>> attendees = new ArrayList<>();
				String[] emails = attendeesInput.split(",");
				
				for (String email : emails) {
					email = email.trim();
					if (!email.isEmpty()) {
						Map<String, String> attendee = new HashMap<>();
						attendee.put(EMAIL, email);
						attendees.add(attendee);
					}
				}
				event.put(ATTENDEES, attendees);
			}

			String jsonBody = GSON.toJson(event);
			String response = HttpHelperUtility.patchRequestStringBody(url, headers, jsonBody,
					ContentType.APPLICATION_JSON, null, null, null);

			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			Map<String, Object> result = new HashMap<>();

			result.put(ID, json.get(ID));
			result.put(SUMMARY, json.get(SUMMARY));
			Map<String, Object> start = (Map<String, Object>) json.get(START);
			if (start != null) {
				result.put(START_TIME, start.get(DATE_TIME));
			}
			Map<String, Object> end = (Map<String, Object>) json.get(END);
			if (end != null) {
				result.put(END_TIME, end.get(DATE_TIME));
			}
			result.put(LINK, json.get(HANGOUT_LINK));
			result.put(SUCCESS_KEY, true);

			return result;

		} catch (Exception e) {
			classLogger.error("Error updating Google Meet meeting", e);
			throw e;
		}
	}

	/**
	 * Deletes an existing Google Meet meeting.
	 *
	 * @param accessToken OAuth access token.
	 * @param eventId     ID of the meeting to delete.
	 *
	 * @return Map containing deletion status or confirmation details.
	 *
	 * @throws Exception if deletion fails.
	 */
	public static Map<String, Object> deleteMeeting(String accessToken, String eventId) throws Exception {
		try {
			String url = BASE_URL + SLASH_KEY + eventId;

			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String existingResponse = HttpHelperUtility.getRequest(url, headers, null, null, null);

			Map<String, Object> existingEvent = GSON.fromJson(existingResponse, new TypeToken<Map<String, Object>>() {
			}.getType());

			Object meetLink = existingEvent.get(HANGOUT_LINK);
			if (meetLink == null || meetLink.toString().isEmpty()) {
				throw new SemossPixelException("This event is not a Google Meet meeting.");
			}

			HttpHelperUtility.deleteRequestStringBody(url, headers, null, null, null);

			Map<String, Object> result = new HashMap<>();
			result.put(SUCCESS_KEY, true);

			return result;

		} catch (Exception e) {
			classLogger.error("Error deleting Google Meet meeting", e);
			throw e;
		}
	}
}