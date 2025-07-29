package prerna.io.connector.calendar;

import java.util.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import prerna.security.HttpHelperUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;

public class GoogleCalendarHelper {
	
	private static final String UNTIL_PREFIX = ";UNTIL=";
	private static final String NONE = "NONE";
	private static final String WEEKLY = "WEEKLY";
	private static final String DAILY = "DAILY";
	private static final String STATUS_KEY = "status";
	private static final String HEADER_AUTHORIZATION = "Authorization";
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String BEARER = "Bearer ";
    private static final String CALENDAR_ID = "primary";
    private static final String GOOGLE_CALENDAR_URL_TEMPLATE = "https://www.googleapis.com/calendar/v3/calendars/%s/events?conferenceDataVersion=1";
    private static final String GOOGLE_CALENDAR_EVENT_URL_TEMPLATE = "https://www.googleapis.com/calendar/v3/calendars/primary/events/%s";
    private static final String GOOGLE_CALENDAR_UPDATE_URL_TEMPLATE = "https://www.googleapis.com/calendar/v3/calendars/%s/events/%s?conferenceDataVersion=1";
    private static final String SUMMARY = "summary";
    private static final String LOCATION = "location";
    private static final String DESCRIPTION = "description";
    private static final String START = "start";
    private static final String END = "end";
    private static final String DATE_TIME = "dateTime";
    private static final String TIME_ZONE = "timeZone";
    private static final String ASIA_KOLKATA = "Asia/Kolkata";
    private static final String ATTENDEES = "attendees";
    private static final String EMAIL = "email";
    private static final String REMINDERS = "reminders";
    private static final String USE_DEFAULT = "useDefault";
    private static final String OVERRIDES = "overrides";
    private static final String METHOD = "method";
    private static final String MINUTES = "minutes";
    private static final String EMAIL_METHOD = "email";
    private static final String POPUP_METHOD = "popup";
    private static final String CONFERENCE_DATA = "conferenceData";
    private static final String CREATE_REQUEST = "createRequest";
    private static final String REQUEST_ID = "requestId";
    private static final String CONFERENCE_SOLUTION_KEY = "conferenceSolutionKey";
    private static final String TYPE = "type";
    private static final String HANGOUTS_MEET = "hangoutsMeet";
    private static final String ID = "id";            //eventId
    private static final String HTML_LINK = "htmlLink";
    private static final String LINK = "link";
    private static final String RESPONSE_STATUS = "responseStatus";
    private static final String ORGANIZER = "organizer";
    private static final String HANGOUT_LINK = "hangoutLink";
    private static final String RECURRENCE = "recurrence";
    private static final String FREQUENCY = "frequency";
    private static final String UNTIL = "until";
    private static final String RRULE_PREFIX = "RRULE:FREQ=";
    private static final String SINGLE_EVENT = "singleEvent";
    private static final String RECURRING_EVENT_ID = "recurringEventId";
    private static final String START_TIME = "startTime";
    private static final String END_TIME = "endTime";
	
	private static final Logger classLogger = LogManager.getLogger(GoogleCalendarHelper.class);
	
	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public static NounMetadata createEvent(String accessToken, String summary, String location, String desc, String startdatetime,
			String enddatetime, List<String> attendeeEmails, Boolean enableVideoConferencing) throws Exception {
		try {
			String url = String.format(GOOGLE_CALENDAR_URL_TEMPLATE, CALENDAR_ID);

		    Map<String, Object> event = new HashMap<>();
		    event.put(SUMMARY, summary);
		    event.put(LOCATION, location);
		    event.put(DESCRIPTION, desc);

		    Map<String, Object> start = new HashMap<>();
		    start.put(DATE_TIME, startdatetime);
		    start.put(TIME_ZONE, ASIA_KOLKATA);
		    event.put(START, start);

		    Map<String, Object> end = new HashMap<>();
		    end.put(DATE_TIME, enddatetime);
		    end.put(TIME_ZONE, ASIA_KOLKATA);
		    event.put(END, end);

		    List<Map<String, Object>> attendees = new ArrayList<>();
		    for (String email : attendeeEmails) {
		        Map<String, Object> attendee = new HashMap<>();
		        attendee.put(EMAIL, email);
		        attendees.add(attendee);
		    }
		    event.put(ATTENDEES, attendees);

		    Map<String, Object> emailReminder = new HashMap<>();
		    emailReminder.put(METHOD, EMAIL_METHOD);
		    emailReminder.put(MINUTES, 24 * 60);

		    Map<String, Object> popupReminder = new HashMap<>();
		    popupReminder.put(METHOD, POPUP_METHOD);
		    popupReminder.put(MINUTES, 10);

		    Map<String, Object> reminders = new HashMap<>();
		    reminders.put(USE_DEFAULT, false);
		    reminders.put(OVERRIDES, Arrays.asList(emailReminder, popupReminder));
		    event.put(REMINDERS, reminders);

		    if (enableVideoConferencing) {
		        Map<String, Object> conferenceSolutionKey = new HashMap<>();
		        conferenceSolutionKey.put(TYPE, HANGOUTS_MEET);

		        Map<String, Object> createConferenceRequest = new HashMap<>();
		        createConferenceRequest.put(REQUEST_ID, UUID.randomUUID().toString());
		        createConferenceRequest.put(CONFERENCE_SOLUTION_KEY, conferenceSolutionKey);

		        Map<String, Object> conferenceData = new HashMap<>();
		        conferenceData.put(CREATE_REQUEST, createConferenceRequest);

		        event.put(CONFERENCE_DATA, conferenceData);
		    }

		    Map<String, String> headers = getBearerHeader(accessToken);
		    String jsonBody = gson.toJson(event);
		    String response = HttpHelperUtility.postRequestStringBody(url, headers, jsonBody, ContentType.APPLICATION_JSON, null, null, null);
		    Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
		    Map<String, Object> map = new HashMap<>();
		    map.put(ID, json.get(ID));
		    map.put(LINK, json.get(HTML_LINK));
		    return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
            classLogger.error("Error creating calendar event", e);
            throw new SemossPixelException("Failed to create calendar event: " + e.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	public static NounMetadata readEvent(String accessToken, String id) throws Exception {
		try {
	        Map<String, String> headers = getBearerHeader(accessToken);
	        String url = String.format(GOOGLE_CALENDAR_EVENT_URL_TEMPLATE, id);

	        String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
	        Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());

	        Map<String, Object> map = new HashMap<>();
	        map.put(SUMMARY, json.get(SUMMARY));
	        map.put(DESCRIPTION, json.get(DESCRIPTION));

	        List<Map<String, Object>> attendeeList = new ArrayList<>();
	        List<Map<String, Object>> attendees = (List<Map<String, Object>>) json.get(ATTENDEES);

	        if (attendees != null) {
	            for (Map<String, Object> att : attendees) {
	                Map<String, Object> attendeeMap = new HashMap<>();
	                attendeeMap.put(EMAIL, att.get(EMAIL));
	                attendeeMap.put(RESPONSE_STATUS, att.get(RESPONSE_STATUS));
	                attendeeList.add(attendeeMap);
	            }
	        }
	        map.put(ATTENDEES, attendeeList);

	        Map<String, Object> start = (Map<String, Object>) json.get(START);
	        Map<String, Object> end = (Map<String, Object>) json.get(END);

	        map.put("startTime", start != null ? start.get(DATE_TIME) : null);
	        map.put("endTime", end != null ? end.get(DATE_TIME) : null);

	        Map<String, Object> organizer = (Map<String, Object>) json.get(ORGANIZER);
	        map.put(ORGANIZER, organizer != null ? organizer.get(EMAIL) : null);

	        map.put(HANGOUT_LINK, json.get(HANGOUT_LINK));
	        map.put(HTML_LINK, json.get(HTML_LINK));

	        boolean hasVideo = json.get(HANGOUT_LINK) != null;
	        map.put("video", hasVideo);
	        map.put("audio", hasVideo);

	        String frequency = null;
	        String until = null;

	        List<String> recurrence = (List<String>) json.get(RECURRENCE);
	        if (recurrence != null) {
	            for (String rule : recurrence) {
	                if (rule.startsWith("RRULE:")) {
	                    String[] parts = rule.substring(6).split(";");
	                    for (String part : parts) {
	                        if (part.startsWith("FREQ=")) {
	                            frequency = part.substring(5);
	                        } else if (part.startsWith("UNTIL=")) {
	                            until = part.substring(6);
	                        }
	                    }
	                }
	            }
	        }

	        map.put(FREQUENCY, frequency);
	        map.put(UNTIL, until);

	        return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

	    } catch (Exception e) {
	        classLogger.error("Error searching calendar event", e);
	        throw new SemossPixelException("Failed to search calendar event: " + e.getMessage());
	    }
	}
	
	public static Boolean updateEvent(String accessToken, String id, String summary, String location, String desc, String startdatetime,
	String enddatetime, List<String> attendeeEmails, String frequency, String until, Boolean enableVideoConferencing) throws Exception {
		
		try {
		    String url = String.format(GOOGLE_CALENDAR_UPDATE_URL_TEMPLATE, CALENDAR_ID, id);

		    Map<String, Object> event = new HashMap<>();
		    event.put(SUMMARY, summary);
		    event.put(LOCATION, location);
		    event.put(DESCRIPTION, desc);

		    Map<String, Object> start = new HashMap<>();
		    start.put(DATE_TIME, startdatetime);
		    start.put(TIME_ZONE, ASIA_KOLKATA);
		    event.put(START, start);

		    Map<String, Object> end = new HashMap<>();
		    end.put(DATE_TIME, enddatetime);
		    end.put(TIME_ZONE, ASIA_KOLKATA);
		    event.put(END, end);

		    List<Map<String, Object>> attendees = new ArrayList<>();
		    for (String email : attendeeEmails) {
		        Map<String, Object> attendee = new HashMap<>();
		        attendee.put(EMAIL, email);
		        attendees.add(attendee);
		    }
		    event.put(ATTENDEES, attendees);

		    if (enableVideoConferencing) {
		        Map<String, Object> conferenceSolutionKey = new HashMap<>();
		        conferenceSolutionKey.put(TYPE, HANGOUTS_MEET);

		        Map<String, Object> createConferenceRequest = new HashMap<>();
		        createConferenceRequest.put(REQUEST_ID, UUID.randomUUID().toString());
		        createConferenceRequest.put(CONFERENCE_SOLUTION_KEY, conferenceSolutionKey);

		        Map<String, Object> conferenceData = new HashMap<>();
		        conferenceData.put(CREATE_REQUEST, createConferenceRequest);

		        event.put(CONFERENCE_DATA, conferenceData);
		    } else {
		        event.put(CONFERENCE_DATA, null);
		    }

		    if (frequency != null) {
		        frequency = frequency.trim().toUpperCase();
		        if (!frequency.equals(DAILY) && !frequency.equals(WEEKLY) && !frequency.equals(NONE)) {
		            throw new IllegalArgumentException("Frequency must be 'DAILY' or 'WEEKLY' or 'NONE'");
		        }
		        if (frequency.equals(NONE)) {
		            event.put(RECURRENCE, null);
		        } else if (frequency.equals(DAILY) || frequency.equals(WEEKLY)) {
		            if (until == null || until.trim().isEmpty()) {
		                throw new IllegalArgumentException("Until date must be provided for recurring events.");
		            }
		            event.put(RECURRENCE, Arrays.asList(RRULE_PREFIX + frequency + UNTIL_PREFIX + until));
		        }
		    }

		    Map<String, String> headers = getBearerHeader(accessToken);
		    String jsonBody = gson.toJson(event);
		    String response = HttpHelperUtility.putRequestStringBody(url, headers, jsonBody, ContentType.APPLICATION_JSON, null, null, null);
		    return true;
		} catch (Exception e) {
		    classLogger.error("Failed to update event");
		    return false;
		}
	}
	
	public static NounMetadata deleteEvent(String accessToken, String id) throws Exception {
		try {
            Map<String, String> headers = getBearerHeader(accessToken);
            String url = String.format(GOOGLE_CALENDAR_EVENT_URL_TEMPLATE, id);
            HttpHelperUtility.deleteRequestStringBody(url, headers, null, null, null);
            Map<String, Object> result = new HashMap<>();
            result.put(STATUS_KEY, true);
            return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error("Error deleting calendar event", e);
            throw new SemossPixelException("Failed to delete calendar event: " + e.getMessage());
        }
	}
	
	public static NounMetadata recurringEvent(String accessToken, String summary, String location, String description, String startdatetime, String enddatetime, List<String> attendeeEmails, String frequency, String until, Boolean enableVideoConferencing) throws Exception {
		try {
			if (frequency == null) {
			    throw new IllegalArgumentException("Frequency must not be null and must be 'DAILY' or 'WEEKLY'");
			}
			frequency = frequency.trim().toUpperCase();
			if (!frequency.equals(DAILY) && !frequency.equals(WEEKLY)) {
			    throw new IllegalArgumentException("Frequency must be 'DAILY' or 'WEEKLY'");
			}
		    String url = String.format(GOOGLE_CALENDAR_URL_TEMPLATE, CALENDAR_ID);

		    Map<String, Object> event = new HashMap<>();
		    event.put(SUMMARY, summary);
		    event.put(LOCATION, location);
		    event.put(DESCRIPTION, description);

		    Map<String, Object> start = new HashMap<>();
		    start.put(DATE_TIME, startdatetime);
		    start.put(TIME_ZONE, ASIA_KOLKATA);
		    event.put(START, start);

		    Map<String, Object> end = new HashMap<>();
		    end.put(DATE_TIME, enddatetime);
		    end.put(TIME_ZONE, ASIA_KOLKATA);
		    event.put(END, end);
		    List<Map<String, Object>> attendees = new ArrayList<>();
		    for (String email : attendeeEmails) {
		        Map<String, Object> attendee = new HashMap<>();
		        attendee.put(EMAIL, email);
		        attendees.add(attendee);
		    }
		    event.put(ATTENDEES, attendees);
		    if (enableVideoConferencing) {
		        Map<String, Object> conferenceSolutionKey = new HashMap<>();
		        conferenceSolutionKey.put(TYPE, HANGOUTS_MEET);

		        Map<String, Object> createConferenceRequest = new HashMap<>();
		        createConferenceRequest.put(REQUEST_ID, UUID.randomUUID().toString());
		        createConferenceRequest.put(CONFERENCE_SOLUTION_KEY, conferenceSolutionKey);

		        Map<String, Object> conferenceData = new HashMap<>();
		        conferenceData.put(CREATE_REQUEST, createConferenceRequest);

		        event.put(CONFERENCE_DATA, conferenceData);
		    } else {
		        event.put(CONFERENCE_DATA, null);
		    }
		    event.put(RECURRENCE, Arrays.asList(RRULE_PREFIX + frequency + UNTIL_PREFIX + until));
		    Map<String, String> headers = getBearerHeader(accessToken);
		    String jsonBody = gson.toJson(event);
		    String response = HttpHelperUtility.postRequestStringBody(url,headers,jsonBody,ContentType.APPLICATION_JSON,null, null, null);
		    Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
		    Map<String, Object> map = new HashMap<>();
		    map.put(ID, json.get(ID));
		    map.put(LINK, json.get(HTML_LINK));
		    return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
            classLogger.error("Error creating calendar event", e);
            throw new SemossPixelException("Failed to create calendar event: " + e.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	public static NounMetadata searchEvent(String accessToken, String eventId) throws Exception {
		try {
	        Map<String, String> headers = getBearerHeader(accessToken);
	        String url = String.format(GOOGLE_CALENDAR_EVENT_URL_TEMPLATE, eventId);

	        String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
	        Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());

	        Map<String, Object> map = new HashMap<>();
	        map.put(SUMMARY, json.get(SUMMARY));
	        map.put(SINGLE_EVENT, json.get(RECURRING_EVENT_ID) == null);

	        Map<String, Object> start = (Map<String, Object>) json.get(START);
	        Map<String, Object> end = (Map<String, Object>) json.get(END);

	        map.put(START_TIME, start != null ? start.get(DATE_TIME) : null);
	        map.put(END_TIME, end != null ? end.get(DATE_TIME) : null);

	        Map<String, Object> organizer = (Map<String, Object>) json.get(ORGANIZER);
	        map.put(ORGANIZER, organizer != null ? organizer.get(EMAIL) : null);

	        return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

	    } catch (Exception e) {
	        classLogger.error("Error searching calendar event", e);
	        throw new SemossPixelException("Failed to search calendar event: " + e.getMessage());
	    }
	}
	
	public static Map<String, String> getBearerHeader(String accessToken) {
        Map<String, String> headers = new HashMap<>();
        headers.put(HEADER_AUTHORIZATION, BEARER + accessToken);
        headers.put(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);
        return headers;
    }

}
