package prerna.io.connector.google.calendar;

import java.net.URLEncoder;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.io.connector.google.GoogleLoginUtils;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;

public class GoogleCalendarHelper {

	private static final Logger classLogger = LogManager.getLogger(GoogleCalendarHelper.class);

	private static final Gson GSON = new GsonBuilder()
			.disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.setPrettyPrinting()
			.create();
	
	// Calendar event time fields
	private static final String START = "start";
	private static final String END = "end";
	private static final String START_TIME = "startTime";
	private static final String END_TIME = "endTime";
	private static final String DATE_TIME = "dateTime";
	private static final String TIME_ZONE = "timeZone";

	// Event details fields
	private static final String SUMMARY = "summary";
	private static final String LOCATION = "location";
	private static final String DESCRIPTION = "description";

	// Recurrence rule fields
	private static final String RRULE_PREFIX = "RRULE:FREQ=";
	private static final String UNTIL_PREFIX = ";UNTIL=";
	private static final String RECURRENCE = "recurrence";
	private static final String RECURRING_EVENT_ID = "recurringEventId";
	private static final String WEEKLY = "WEEKLY";
	private static final String DAILY = "DAILY";

	// Attendees and Organizer information fields
	private static final String ATTENDEES = "attendees";
	private static final String EMAIL = "email";
	private static final String ORGANIZER = "organizer";
	
	// Calendar and event identifiers
	private static final String CALENDAR_ID = "primary";
	private static final String ID = "id";

	// Conference data fields
	private static final String CONFERENCE_DATA = "conferenceData";
	private static final String CONFERENCE_SOLUTION_KEY = "conferenceSolutionKey";
	private static final String TYPE = "type";
	private static final String HANGOUTS_MEET = "hangoutsMeet";
	private static final String CREATE_REQUEST = "createRequest";
	private static final String REQUEST_ID = "requestId";
	private static final String HTML_LINK = "htmlLink";
	private static final String LINK = "link";
    
	// URL-related
    private static final String GOOGLE_CALENDAR_URL_TEMPLATE = "https://www.googleapis.com/calendar/v3/calendars/%s/events?conferenceDataVersion=1";
	private static final String GOOGLE_CALENDAR_EVENT_URL_TEMPLATE = "https://www.googleapis.com/calendar/v3/calendars/primary/events/%s";
    
    private GoogleCalendarHelper() {

	}
    
    /**
     * 
     * @param accessToken
     * @param summary
     * @param location
     * @param desc
     * @param startdatetime
     * @param enddatetime
     * @param zoneId
     * @param attendeeEmails
     * @param enableVideoConferencing
     * @return
     * @throws Exception
     */
	public static Map<String, Object> createEvent(String accessToken, String summary, String location, String desc,
			String startdatetime, String enddatetime, ZoneId zoneId, List<String> attendeeEmails, Boolean enableVideoConferencing)
			throws Exception {
		final String REMINDERS = "reminders";
		final String USE_DEFAULT = "useDefault";
		final String OVERRIDES = "overrides";
		final String METHOD = "method";
		final String MINUTES = "minutes";
		final String EMAIL_METHOD = "email";
		final String POPUP_METHOD = "popup";
		
		try {
			String url = String.format(GOOGLE_CALENDAR_URL_TEMPLATE, CALENDAR_ID);

			Map<String, Object> event = new HashMap<>();
			event.put(SUMMARY, summary);
			event.put(LOCATION, location);
			event.put(DESCRIPTION, desc);
			
			Map<String, Object> start = new HashMap<>();
			start.put(DATE_TIME, startdatetime);
			start.put(TIME_ZONE, zoneId.getId());
			event.put(START, start);

			Map<String, Object> end = new HashMap<>();
			end.put(DATE_TIME, enddatetime);
			end.put(TIME_ZONE, zoneId.getId());
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

			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String jsonBody = GSON.toJson(event);
			String response = HttpHelperUtility.postRequestStringBody(url, headers, jsonBody,
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
			Map<String, Object> map = new HashMap<>();
			map.put(ID, json.get(ID));
			map.put(LINK, json.get(HTML_LINK));
			return map;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}

	/**
	 * 
	 * @param accessToken
	 * @param id
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> readEvent(String accessToken, String id, ZoneId zoneId) throws Exception {
		final String FREQUENCY = "frequency";
		final String UNTIL = "until";
		final String AUDIO = "audio";
		final String VIDEO = "video";
		final String FREQ = "FREQ=";
		final String RRULE = "RRULE:";
		final String UNTIL_PREFIX = "UNTIL=";
		final String HANGOUT_LINK = "hangoutLink";
		final String RESPONSE_STATUS = "responseStatus";
		
		try {
			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String url = String.format(GOOGLE_CALENDAR_EVENT_URL_TEMPLATE, id);

			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());

			Map<String, Object> map = new HashMap<>();
			map.put(SUMMARY, json.get(SUMMARY));
			map.put(DESCRIPTION, json.get(DESCRIPTION));
			map.put(LOCATION, json.get(LOCATION));
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

			if (start != null && start.get(DATE_TIME) != null) {
			    String startDateTimeStr = (String) start.get(DATE_TIME);
			    String localStartDateTime = startDateTimeStr.substring(0, 19);
			    map.put(START_TIME, localStartDateTime);
			} else {
			    map.put(START_TIME, null);
			}

			if (end != null && end.get(DATE_TIME) != null) {
			    String endDateTimeStr = (String) end.get(DATE_TIME);
			    String localEndDateTime = endDateTimeStr.substring(0, 19);
			    map.put(END_TIME, localEndDateTime);
			} else {
			    map.put(END_TIME, null);
			}

			Map<String, Object> organizer = (Map<String, Object>) json.get(ORGANIZER);
			map.put(ORGANIZER, organizer != null ? organizer.get(EMAIL) : null);

			map.put(HANGOUT_LINK, json.get(HANGOUT_LINK));
			map.put(HTML_LINK, json.get(HTML_LINK));

			boolean hasVideo = json.get(HANGOUT_LINK) != null;
			map.put(VIDEO, hasVideo);
			map.put(AUDIO, hasVideo);

			String frequency = null;
			String until = null;
			String untilDateTime = null;
			
			List<String> recurrence = (List<String>) json.get(RECURRENCE);
			if (recurrence != null) {
				for (String rule : recurrence) {
					if (rule.startsWith(RRULE)) {
						String[] parts = rule.substring(6).split(";");
						for (String part : parts) {
							if (part.startsWith(FREQ)) {
								frequency = part.substring(5);
							} else if (part.startsWith(UNTIL_PREFIX)) {
								until = part.substring(6);
							}
						}
					}
				}
			}
			if (until != null) {
				untilDateTime = localDateTimeFormatConverter(until, zoneId.getId());
			}
			map.put(FREQUENCY, frequency);
			map.put(UNTIL, untilDateTime);
			return map;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}

	/**
	 * 
	 * @param accessToken
	 * @param id
	 * @param summary
	 * @param location
	 * @param desc
	 * @param startdatetime
	 * @param enddatetime
	 * @param zoneId
	 * @param attendeeEmails
	 * @param frequency
	 * @param until
	 * @param enableVideoConferencing
	 * @return
	 * @throws Exception
	 */
	public static Boolean updateEvent(String accessToken, String id, String summary, String location, String desc,
			String startdatetime, String enddatetime, ZoneId zoneId, List<String> attendeeEmails, String frequency, String untilTime,
			Boolean enableVideoConferencing) throws Exception {
		final String NONE = "NONE";
		final String GOOGLE_CALENDAR_UPDATE_URL_TEMPLATE = "https://www.googleapis.com/calendar/v3/calendars/%s/events/%s?conferenceDataVersion=1";
		
		try {
			String url = String.format(GOOGLE_CALENDAR_UPDATE_URL_TEMPLATE, CALENDAR_ID, id);
			
			String until = null;
			if (untilTime != null && !untilTime.isEmpty()) {
				String dateTimeWithOffset = toRfc3339(untilTime, zoneId.getId());
				until = untilFormatConverter(dateTimeWithOffset);
			}
			
			Map<String, Object> event = new HashMap<>();
			event.put(SUMMARY, summary);
			event.put(LOCATION, location);
			event.put(DESCRIPTION, desc);

			Map<String, Object> start = new HashMap<>();
			start.put(DATE_TIME, startdatetime);
			start.put(TIME_ZONE, zoneId.getId());
			event.put(START, start);

			Map<String, Object> end = new HashMap<>();
			end.put(DATE_TIME, enddatetime);
			end.put(TIME_ZONE, zoneId.getId());
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

			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String jsonBody = GSON.toJson(event);
			HttpHelperUtility.putRequestStringBody(url, headers, jsonBody, ContentType.APPLICATION_JSON, null, null, null);
			return true;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			classLogger.warn("Failed to update event", e.getMessage());
			return false;
		}
	}

	/**
	 * 
	 * @param accessToken
	 * @param id
	 * @return
	 * @throws Exception
	 */
	public static Map<String, Object> deleteEvent(String accessToken, String id) throws Exception {
		final String STATUS_KEY = "status";
		
		try {
			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String url = String.format(GOOGLE_CALENDAR_EVENT_URL_TEMPLATE, id);
			HttpHelperUtility.deleteRequestStringBody(url, headers, null, null, null);
			Map<String, Object> result = new HashMap<>();
			result.put(STATUS_KEY, true);
			return result;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}

	/**
	 * 
	 * @param accessToken
	 * @param summary
	 * @param location
	 * @param description
	 * @param startdatetime
	 * @param enddatetime
	 * @param zoneId
	 * @param attendeeEmails
	 * @param frequency
	 * @param until
	 * @param enableVideoConferencing
	 * @return
	 * @throws Exception
	 */
	public static Map<String, Object> recurringEvent(String accessToken, String summary, String location,
			String description, String startdatetime, String enddatetime, ZoneId zoneId, List<String> attendeeEmails, String frequency,
			String untilTime, Boolean enableVideoConferencing) throws Exception {
		
		try {
			if (frequency == null) {
				throw new IllegalArgumentException("Frequency must not be null and must be 'DAILY' or 'WEEKLY'");
			}
			frequency = frequency.trim().toUpperCase();
			if (!frequency.equals(DAILY) && !frequency.equals(WEEKLY)) {
				throw new IllegalArgumentException("Frequency must be 'DAILY' or 'WEEKLY'");
			}
			String url = String.format(GOOGLE_CALENDAR_URL_TEMPLATE, CALENDAR_ID);
			
			String until = null;
			if (untilTime != null && !untilTime.isEmpty()) {
				String dateTimeWithOffset = toRfc3339(untilTime, zoneId.getId());
				until = untilFormatConverter(dateTimeWithOffset);
			}
			Map<String, Object> event = new HashMap<>();
			event.put(SUMMARY, summary);
			event.put(LOCATION, location);
			event.put(DESCRIPTION, description);
			
			Map<String, Object> start = new HashMap<>();
			start.put(DATE_TIME, startdatetime);
			start.put(TIME_ZONE, zoneId.getId());
			event.put(START, start);

			Map<String, Object> end = new HashMap<>();
			end.put(DATE_TIME, enddatetime);
			end.put(TIME_ZONE, zoneId.getId());
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
			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String jsonBody = GSON.toJson(event);
			String response = HttpHelperUtility.postRequestStringBody(url, headers, jsonBody,
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
			Map<String, Object> map = new HashMap<>();
			map.put(ID, json.get(ID));
			map.put(LINK, json.get(HTML_LINK));
			return map;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}
	
	/**
	 * 
	 * @param accessToken
	 * @param startDateTime
	 * @param endDateTime
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getEventList(String accessToken, String startDateTime, String endDateTime, ZoneId zoneId) throws Exception {
	    final String GOOGLE_CALENDAR_LIST_TEMPLATE = "https://www.googleapis.com/calendar/v3/calendars/%s/events";
	    final String EVENTS = "events";
		final String DATE = "date";
	    final String ORDER_BY = "orderBy";
	    final String SINGLE_EVENTS = "singleEvents";
	    final String SINGLE_EVENTS_TRUE = "true";
	    final String TIME_MIN = "timeMin";
	    final String TIME_MAX = "timeMax";
	    final String MAX_RESULTS = "maxResults";
	    final String MAX_RESULTS_100 = "100";
	    final String PAGE_TOKEN = "pageToken";
	    final String ITEMS = "items";
	    final String NEXT_PAGE_TOKEN = "nextPageToken";
	    
		Map<String, List<Map<String, Object>>> events = new LinkedHashMap<>();
		String url = String.format(GOOGLE_CALENDAR_LIST_TEMPLATE, CALENDAR_ID);
        String pageToken = null;
        do {
            Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
            Map<String, String> params = new HashMap<>();
            params.put(ORDER_BY, START_TIME);
            params.put(SINGLE_EVENTS, SINGLE_EVENTS_TRUE);
            String startTime = null;
            if (startDateTime != null && !startDateTime.isEmpty()) {
                startTime = toRfc3339(startDateTime, zoneId.getId());
            }

            String endTime = null;
            if (endDateTime != null && !endDateTime.isEmpty()) {
                endTime = toRfc3339(endDateTime, zoneId.getId());
            }
            params.put(TIME_MIN, startTime);
            params.put(TIME_MAX, endTime);
            params.put(MAX_RESULTS, MAX_RESULTS_100);
            if (pageToken != null) {
                params.put(PAGE_TOKEN, pageToken);
            }
            StringBuilder fullUrl = new StringBuilder(url);
            fullUrl.append("?");
            for (Map.Entry<String, String> entry : params.entrySet()) {
                fullUrl.append(URLEncoder.encode(entry.getKey(), "UTF-8")).append("=").append(URLEncoder.encode(entry.getValue(), "UTF-8")).append("&");
            }
            fullUrl.setLength(fullUrl.length() - 1);
            String response = HttpHelperUtility.getRequest(fullUrl.toString(), headers, null, null, null);
            
            Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
            List<Map<String, Object>> items = (List<Map<String, Object>>) json.get(ITEMS);
            if (items != null && !items.isEmpty()) {
                for (Map<String, Object> item : items) {
                	Map<String, Object> map = new HashMap<>();
                	map.put(SUMMARY, item.get(SUMMARY));
                	map.put(ID, item.get(ID));
                	String recurringEventId = (String) item.get(RECURRING_EVENT_ID);
                	if (recurringEventId != null) {
                		map.put(RECURRING_EVENT_ID, item.get(RECURRING_EVENT_ID));
                	}
                	Map<String, Object> start = (Map<String, Object>) item.get(START);
                	String dateTime = (String) start.get(DATE_TIME);
                	String date = dateTime.substring(0,10);
                	if(!events.containsKey(date)) {
                		events.put(date, new ArrayList<>());
                	}
                	events.get(date).add(map);
                }
            }
            pageToken = (String) json.get(NEXT_PAGE_TOKEN);
        } while (pageToken != null);
        
	    if (events.isEmpty()) {
	        classLogger.info("No events found in the given date range");
	    }
	    
	    List<Map<String, Object>> eventList = new ArrayList<>();
	    for (Map.Entry<String, List<Map<String, Object>>> entry : events.entrySet()) {
	        Map<String, Object> map = new LinkedHashMap<>();
	        map.put(DATE, entry.getKey());
	        map.put(EVENTS, entry.getValue());
	        eventList.add(map);
	    }
	    return eventList;
	}

	/**
	 * 
	 * @param accessToken
	 * @param eventId
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> searchEvent(String accessToken, String eventId) throws Exception {
		final String SINGLE_EVENT = "singleEvent";
		
		try {
			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String url = String.format(GOOGLE_CALENDAR_EVENT_URL_TEMPLATE, eventId);

			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());

			Map<String, Object> map = new HashMap<>();
			map.put(SUMMARY, json.get(SUMMARY));
			map.put(SINGLE_EVENT, json.get(RECURRING_EVENT_ID) == null);

			Map<String, Object> start = (Map<String, Object>) json.get(START);
			Map<String, Object> end = (Map<String, Object>) json.get(END);

			map.put(START_TIME, start != null ? start.get(DATE_TIME) : null);
			map.put(END_TIME, end != null ? end.get(DATE_TIME) : null);

			Map<String, Object> organizer = (Map<String, Object>) json.get(ORGANIZER);
			map.put(ORGANIZER, organizer != null ? organizer.get(EMAIL) : null);

			return map;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}
	
	/**
     * 
     * @param untilTime
     * @return
     * @throws Exception
     */
	public static String untilFormatConverter(String untilTime) throws Exception {
		final String YYYY_M_MDD_T_H_HMMSS_Z = "yyyyMMdd'T'HHmmss'Z'";
		
		try {
			OffsetDateTime parsedDateTime = OffsetDateTime.parse(untilTime);
			OffsetDateTime utcDateTime = parsedDateTime.withOffsetSameInstant(ZoneOffset.UTC);
			DateTimeFormatter format = DateTimeFormatter.ofPattern(YYYY_M_MDD_T_H_HMMSS_Z);
			return utcDateTime.format(format);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}
	
	/**
     * 
     * @param untilDateTime
     * @param zoneId
     * @return
     * @throws Exception
     */
	public static String localDateTimeFormatConverter(String untilDateTime, String zoneId) throws Exception {
		final String YYYY_MM_DD_T_HH_MM_SS = "yyyy-MM-dd'T'HH:mm:ss";
		final String YYYY_M_MDD_T_H_HMMSS_X = "yyyyMMdd'T'HHmmssX";
		
		try {
			DateTimeFormatter format = DateTimeFormatter.ofPattern(YYYY_M_MDD_T_H_HMMSS_X);
			OffsetDateTime utcDateTime = OffsetDateTime.parse(untilDateTime, format);
			ZoneId zone = ZoneId.of(zoneId);
			ZonedDateTime localDateTime = utcDateTime.atZoneSameInstant(zone);
			DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern(YYYY_MM_DD_T_HH_MM_SS);
	        return localDateTime.format(outputFormatter);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}
	
	/**
     * 
     * @param dateTime
     * @param zoneId
     * @return
     * @throws Exception
     */
	public static String toRfc3339(String dateTime, String zoneId) throws Exception {
	    try {
	        ZoneId zone = ZoneId.of(zoneId);
	        LocalDateTime localDateTime = LocalDateTime.parse(dateTime);
	        ZonedDateTime zonedDateTime = localDateTime.atZone(zone);
	        return zonedDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
	    } catch (Exception e) {
	        classLogger.error(Constants.STACKTRACE, e);
	        throw e;
	    }
	}
}
