/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.io.connector.google.calendar;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import prerna.security.HttpHelperUtility;

public class GoogleCalendarHelper {

	private static final Logger classLogger = LogManager.getLogger(GoogleCalendarHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	// Calendar event time fields
	private static final String START = "start";
	private static final String END = "end";
	private static final String START_TIME = "startTime";
	private static final String END_TIME = "endTime";
	private static final String DATE_TIME = "dateTime";
	private static final String DATE = "date";
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
	 * Creates a non-recurring Google Calendar event.
	 *
	 * @param accessToken             OAuth access token for Google APIs.
	 * @param summary                 event title.
	 * @param location                event location.
	 * @param desc                    event description.
	 * @param startdatetime           event start timestamp in RFC3339-compatible
	 *                                format.
	 * @param enddatetime             event end timestamp in RFC3339-compatible
	 *                                format.
	 * @param zoneId                  time zone used for event date-time fields.
	 * @param attendeeEmails          attendee email addresses to include on the
	 *                                invite.
	 * @param enableVideoConferencing whether to attach Google Meet conference
	 *                                details.
	 * @return a map containing the created event ID and HTML link.
	 * @throws Exception if the event creation request fails.
	 */
	public static Map<String, Object> createEvent(String accessToken, String summary, String location, String desc,
			String startdatetime, String enddatetime, ZoneId zoneId, List<String> attendeeEmails,
			Boolean enableVideoConferencing) throws Exception {
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
			List<String> safeAttendeeEmails = attendeeEmails != null ? attendeeEmails : new ArrayList<>();
			for (String email : safeAttendeeEmails) {
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
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			Map<String, Object> map = new HashMap<>();
			map.put(ID, json.get(ID));
			map.put(LINK, json.get(HTML_LINK));
			return map;
		} catch (Exception e) {
			classLogger.error("Failed to create Google Calendar event with summary '{}'", summary, e);
			throw e;
		}
	}

	/**
	 * Reads event details from Google Calendar for a given event ID.
	 *
	 * @param accessToken OAuth access token for Google APIs.
	 * @param id          unique event ID.
	 * @param zoneId      time zone used to convert recurrence-until values to local
	 *                    time.
	 * @return a map containing event details, attendee metadata, and recurrence
	 *         information.
	 * @throws Exception if the event cannot be fetched or parsed.
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
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());

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

			map.put(START_TIME, extractEventTimeValue(start));
			map.put(END_TIME, extractEventTimeValue(end));

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
			classLogger.error("Failed to read Google Calendar event id {}", id, e);
			throw e;
		}
	}

	/**
	 * Updates a Google Calendar event and optionally modifies recurrence and Meet
	 * details.
	 *
	 * @param accessToken             OAuth access token for Google APIs.
	 * @param id                      unique event ID.
	 * @param summary                 updated event title.
	 * @param location                updated event location.
	 * @param desc                    updated event description.
	 * @param startdatetime           updated start timestamp in RFC3339-compatible
	 *                                format.
	 * @param enddatetime             updated end timestamp in RFC3339-compatible
	 *                                format.
	 * @param zoneId                  time zone used for date-time conversion.
	 * @param attendeeEmails          updated attendee email addresses.
	 * @param frequency               recurrence frequency (DAILY, WEEKLY, or NONE).
	 * @param untilTime               optional local end date-time for recurring
	 *                                events. When omitted, recurrence is
	 *                                open-ended.
	 * @param enableVideoConferencing whether to attach Google Meet conference
	 *                                details.
	 * @return {@code true} when the update request succeeds.
	 * @throws Exception if request validation or update fails.
	 */
	@SuppressWarnings("unchecked")
	public static Boolean updateEvent(String accessToken, String id, String summary, String location, String desc,
			String startdatetime, String enddatetime, ZoneId zoneId, List<String> attendeeEmails, String frequency,
			String untilTime, Boolean enableVideoConferencing) throws Exception {
		final String NONE = "NONE";
		final String GOOGLE_CALENDAR_UPDATE_URL_TEMPLATE = "https://www.googleapis.com/calendar/v3/calendars/%s/events/%s?conferenceDataVersion=1";

		try {
			String url = String.format(GOOGLE_CALENDAR_UPDATE_URL_TEMPLATE, CALENDAR_ID, id);
			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String getEventUrl = String.format(GOOGLE_CALENDAR_EVENT_URL_TEMPLATE, id);
			String existingResponse = HttpHelperUtility.getRequest(getEventUrl, headers, null, null, null);
			Map<String, Object> existingEvent = GSON.fromJson(existingResponse, new TypeToken<Map<String, Object>>() {
			}.getType());

			String until = null;
			if (untilTime != null && !untilTime.isEmpty()) {
				String dateTimeWithOffset = toRfc3339(untilTime, zoneId.getId());
				until = untilFormatConverter(dateTimeWithOffset);
			}

			Map<String, Object> event = new HashMap<>();
			event.put(SUMMARY, summary != null ? summary : existingEvent.get(SUMMARY));
			event.put(LOCATION, location != null ? location : existingEvent.get(LOCATION));
			event.put(DESCRIPTION, desc != null ? desc : existingEvent.get(DESCRIPTION));

			Map<String, Object> existingStart = (Map<String, Object>) existingEvent.get(START);
			Map<String, Object> start = buildEventTimePayload(existingStart, startdatetime, zoneId);
			if (!start.isEmpty()) {
				event.put(START, start);
			}

			Map<String, Object> existingEnd = (Map<String, Object>) existingEvent.get(END);
			Map<String, Object> end = buildEventTimePayload(existingEnd, enddatetime, zoneId);
			if (!end.isEmpty()) {
				event.put(END, end);
			}

			if (attendeeEmails != null) {
				List<Map<String, Object>> attendees = new ArrayList<>();
				for (String email : attendeeEmails) {
					Map<String, Object> attendee = new HashMap<>();
					attendee.put(EMAIL, email);
					attendees.add(attendee);
				}
				event.put(ATTENDEES, attendees);
			} else if (existingEvent.get(ATTENDEES) != null) {
				event.put(ATTENDEES, existingEvent.get(ATTENDEES));
			}

			if (Boolean.TRUE.equals(enableVideoConferencing)) {
				Map<String, Object> conferenceSolutionKey = new HashMap<>();
				conferenceSolutionKey.put(TYPE, HANGOUTS_MEET);

				Map<String, Object> createConferenceRequest = new HashMap<>();
				createConferenceRequest.put(REQUEST_ID, UUID.randomUUID().toString());
				createConferenceRequest.put(CONFERENCE_SOLUTION_KEY, conferenceSolutionKey);

				Map<String, Object> conferenceData = new HashMap<>();
				conferenceData.put(CREATE_REQUEST, createConferenceRequest);

				event.put(CONFERENCE_DATA, conferenceData);
			} else if (Boolean.FALSE.equals(enableVideoConferencing)) {
				event.put(CONFERENCE_DATA, null);
			}

			if (frequency != null && !frequency.trim().isEmpty()) {
				frequency = frequency.trim().toUpperCase();
				if (!frequency.equals(DAILY) && !frequency.equals(WEEKLY) && !frequency.equals(NONE)) {
					throw new IllegalArgumentException("Frequency must be 'DAILY' or 'WEEKLY' or 'NONE'");
				}
				if (frequency.equals(NONE)) {
					event.put(RECURRENCE, null);
				} else if (frequency.equals(DAILY) || frequency.equals(WEEKLY)) {
					if (until == null || until.trim().isEmpty()) {
						event.put(RECURRENCE, Arrays.asList(RRULE_PREFIX + frequency));
					} else {
						event.put(RECURRENCE, Arrays.asList(RRULE_PREFIX + frequency + UNTIL_PREFIX + until));
					}
				}
			} else if (existingEvent.get(RECURRENCE) != null) {
				event.put(RECURRENCE, existingEvent.get(RECURRENCE));
			}

			String jsonBody = GSON.toJson(event);
			HttpHelperUtility.putRequestStringBody(url, headers, jsonBody, ContentType.APPLICATION_JSON, null, null,
					null);
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to update Google Calendar event id {}", id, e);
			throw e;
		}
	}

	/**
	 * Deletes a Google Calendar event by ID.
	 *
	 * @param accessToken OAuth access token for Google APIs.
	 * @param id          unique event ID.
	 * @return a status map indicating successful deletion.
	 * @throws Exception if the delete request fails.
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
			classLogger.error("Failed to delete Google Calendar event id {}", id, e);
			throw e;
		}
	}

	/**
	 * Creates a recurring Google Calendar event.
	 *
	 * @param accessToken             OAuth access token for Google APIs.
	 * @param summary                 event title.
	 * @param location                event location.
	 * @param description             event description.
	 * @param startdatetime           event start timestamp in RFC3339-compatible
	 *                                format.
	 * @param enddatetime             event end timestamp in RFC3339-compatible
	 *                                format.
	 * @param zoneId                  time zone used for date-time conversion.
	 * @param attendeeEmails          attendee email addresses to include on the
	 *                                invite.
	 * @param frequency               recurrence frequency (DAILY or WEEKLY).
	 * @param untilTime               optional local end date-time for the recurring
	 *                                schedule. When omitted, recurrence is
	 *                                open-ended.
	 * @param enableVideoConferencing whether to attach Google Meet conference
	 *                                details.
	 * @return a map containing the created event ID and HTML link.
	 * @throws Exception if validation fails or the create request fails.
	 */
	public static Map<String, Object> recurringEvent(String accessToken, String summary, String location,
			String description, String startdatetime, String enddatetime, ZoneId zoneId, List<String> attendeeEmails,
			String frequency, String untilTime, Boolean enableVideoConferencing) throws Exception {

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
			List<String> safeAttendeeEmails = attendeeEmails != null ? attendeeEmails : new ArrayList<>();
			for (String email : safeAttendeeEmails) {
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
			if (until == null || until.trim().isEmpty()) {
				event.put(RECURRENCE, Arrays.asList(RRULE_PREFIX + frequency));
			} else {
				event.put(RECURRENCE, Arrays.asList(RRULE_PREFIX + frequency + UNTIL_PREFIX + until));
			}
			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String jsonBody = GSON.toJson(event);
			String response = HttpHelperUtility.postRequestStringBody(url, headers, jsonBody,
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			Map<String, Object> map = new HashMap<>();
			map.put(ID, json.get(ID));
			map.put(LINK, json.get(HTML_LINK));
			return map;
		} catch (Exception e) {
			classLogger.error("Failed to create recurring Google Calendar event with summary '{}'", summary, e);
			throw e;
		}
	}

	/**
	 * Lists Google Calendar events within a date-time window.
	 *
	 * @param accessToken   OAuth access token for Google APIs.
	 * @param startDateTime window start in local date-time format (for example,
	 *                      {@code 2026-03-31T09:00:00}).
	 * @param endDateTime   window end in local date-time format.
	 * @param zoneId        user time zone used to convert local date-time values to
	 *                      RFC3339.
	 * @return a date-grouped event list.
	 * @throws Exception if list retrieval or date conversion fails.
	 */
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getEventList(String accessToken, String startDateTime, String endDateTime,
			ZoneId zoneId) throws Exception {
		final String GOOGLE_CALENDAR_LIST_TEMPLATE = "https://www.googleapis.com/calendar/v3/calendars/%s/events";
		final String EVENTS = "events";
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
			if (startTime != null) {
				params.put(TIME_MIN, startTime);
			}
			if (endTime != null) {
				params.put(TIME_MAX, endTime);
			}
			params.put(MAX_RESULTS, MAX_RESULTS_100);
			if (pageToken != null) {
				params.put(PAGE_TOKEN, pageToken);
			}
			StringBuilder fullUrl = new StringBuilder(url);
			fullUrl.append("?");
			for (Map.Entry<String, String> entry : params.entrySet()) {
				fullUrl.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)).append("=")
						.append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8)).append("&");
			}
			fullUrl.setLength(fullUrl.length() - 1);
			String response = HttpHelperUtility.getRequest(fullUrl.toString(), headers, null, null, null);

			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
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
					String date = extractEventDate(start);
					if (date == null || date.isEmpty()) {
						continue;
					}
					if (!events.containsKey(date)) {
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
	 * Searches for a single Google Calendar event by ID.
	 *
	 * @param accessToken OAuth access token for Google APIs.
	 * @param eventId     unique event ID.
	 * @return a map with basic event details and recurrence type metadata.
	 * @throws Exception if lookup fails.
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> searchEvent(String accessToken, String eventId) throws Exception {
		final String SINGLE_EVENT = "singleEvent";

		try {
			Map<String, String> headers = GoogleLoginUtils.getBearerHeader(accessToken);
			String url = String.format(GOOGLE_CALENDAR_EVENT_URL_TEMPLATE, eventId);

			String response = HttpHelperUtility.getRequest(url, headers, null, null, null);
			Map<String, Object> json = GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());

			Map<String, Object> map = new HashMap<>();
			map.put(SUMMARY, json.get(SUMMARY));
			map.put(SINGLE_EVENT, json.get(RECURRING_EVENT_ID) == null);

			Map<String, Object> start = (Map<String, Object>) json.get(START);
			Map<String, Object> end = (Map<String, Object>) json.get(END);

			map.put(START_TIME, extractEventTimeValue(start));
			map.put(END_TIME, extractEventTimeValue(end));

			Map<String, Object> organizer = (Map<String, Object>) json.get(ORGANIZER);
			map.put(ORGANIZER, organizer != null ? organizer.get(EMAIL) : null);

			return map;
		} catch (Exception e) {
			classLogger.error("Failed to search Google Calendar event id {}", eventId, e);
			throw e;
		}
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> buildEventTimePayload(Map<String, Object> existingTime, String updatedDateTime,
			ZoneId zoneId) {
		Map<String, Object> time = new HashMap<>();
		if (existingTime != null) {
			time.putAll(existingTime);
		}
		if (updatedDateTime != null && !updatedDateTime.isEmpty()) {
			time.put(DATE_TIME, updatedDateTime);
			time.put(TIME_ZONE, zoneId.getId());
			time.remove(DATE);
		}
		return time;
	}

	private static String extractEventTimeValue(Map<String, Object> timeMap) {
		if (timeMap == null) {
			return null;
		}
		Object dateTimeObj = timeMap.get(DATE_TIME);
		if (dateTimeObj instanceof String) {
			String dateTime = (String) dateTimeObj;
			return dateTime.length() >= 19 ? dateTime.substring(0, 19) : dateTime;
		}
		Object dateObj = timeMap.get(DATE);
		if (dateObj instanceof String) {
			return (String) dateObj;
		}
		return null;
	}

	private static String extractEventDate(Map<String, Object> timeMap) {
		if (timeMap == null) {
			return null;
		}
		Object dateTimeObj = timeMap.get(DATE_TIME);
		if (dateTimeObj instanceof String) {
			String dateTime = (String) dateTimeObj;
			return dateTime.length() >= 10 ? dateTime.substring(0, 10) : dateTime;
		}
		Object dateObj = timeMap.get(DATE);
		if (dateObj instanceof String) {
			return (String) dateObj;
		}
		return null;
	}

	/**
	 * Converts an RFC3339 timestamp to Google Calendar recurrence {@code UNTIL}
	 * format in UTC.
	 *
	 * @param untilTime RFC3339 date-time string with offset.
	 * @return formatted recurrence end time ({@code yyyyMMdd'T'HHmmss'Z'}).
	 * @throws Exception if parsing or formatting fails.
	 */
	public static String untilFormatConverter(String untilTime) throws Exception {
		final String YYYY_M_MDD_T_H_HMMSS_Z = "yyyyMMdd'T'HHmmss'Z'";

		try {
			OffsetDateTime parsedDateTime = OffsetDateTime.parse(untilTime);
			OffsetDateTime utcDateTime = parsedDateTime.withOffsetSameInstant(ZoneOffset.UTC);
			DateTimeFormatter format = DateTimeFormatter.ofPattern(YYYY_M_MDD_T_H_HMMSS_Z);
			return utcDateTime.format(format);
		} catch (Exception e) {
			classLogger.error("Failed to convert RFC3339 value '{}' to UNTIL format", untilTime, e);
			throw e;
		}
	}

	/**
	 * Converts a Google recurrence {@code UNTIL} value into local date-time text.
	 *
	 * @param untilDateTime recurrence {@code UNTIL} value
	 *                      ({@code yyyyMMdd'T'HHmmssX}).
	 * @param zoneId        target zone ID for conversion.
	 * @return local date-time string ({@code yyyy-MM-dd'T'HH:mm:ss}).
	 * @throws Exception if parsing or time-zone conversion fails.
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
			classLogger.error("Failed to convert UNTIL value '{}' to local time for zone {}", untilDateTime, zoneId, e);
			throw e;
		}
	}

	/**
	 * Converts a local date-time string into RFC3339 format for Google Calendar
	 * requests.
	 *
	 * @param dateTime local date-time string ({@code yyyy-MM-dd'T'HH:mm:ss}).
	 * @param zoneId   zone ID used to apply the correct offset.
	 * @return RFC3339 date-time string with offset.
	 * @throws Exception if parsing or time-zone conversion fails.
	 */
	public static String toRfc3339(String dateTime, String zoneId) throws Exception {
		try {
			ZoneId zone = ZoneId.of(zoneId);
			LocalDateTime localDateTime = LocalDateTime.parse(dateTime);
			ZonedDateTime zonedDateTime = localDateTime.atZone(zone);
			return zonedDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
		} catch (Exception e) {
			classLogger.error("Failed to convert local date-time '{}' in zone {} to RFC3339", dateTime, zoneId, e);
			throw e;
		}
	}
}
