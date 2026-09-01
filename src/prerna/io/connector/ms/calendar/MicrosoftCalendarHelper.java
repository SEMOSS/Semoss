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
package prerna.io.connector.ms.calendar;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.security.HttpHelperUtility;

/**
 * The calendar operations of Microsoft Graph, as plain calls.
 *
 * <p>
 * Everything here is delegated: the token says who the signed in user is, so
 * every url is rooted at {@code /me} and nothing a caller passes can read or
 * write somebody else's calendar. A calendar id narrows the call to one of the
 * user's own calendars, and leaving it out uses their default one.
 * </p>
 *
 * <p>
 * What the methods return is Graph's own json, parsed into maps, rather than a
 * type of this codebase's invention. {@link MicrosoftCalendarEventMapper} turns
 * that into the tidier shape the reactors answer with, so a caller that wants a
 * field this class never thought about can still reach it.
 * </p>
 *
 * <p>
 * Two Graph rules are worth knowing. A listing reads {@code /calendarView}
 * rather than {@code /events}, because only the former expands a recurring
 * series into the occurrences that actually fall in the window. And a naive
 * date and time, one carrying no offset, is read in UTC unless the request
 * carries a {@code Prefer: outlook.timezone} header, which is what the
 * {@code timeZone} argument sets.
 * </p>
 */
public class MicrosoftCalendarHelper {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftCalendarHelper.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String GRAPH_BASE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";

	/** The fields an event listing asks for when the caller wants the body. */
	private static final String EVENT_FIELDS = "id,subject,bodyPreview,body,start,end,isAllDay,location,attendees,"
			+ "organizer,webLink,onlineMeeting,isOnlineMeeting,isCancelled,showAs,importance,responseStatus,"
			+ "reminderMinutesBeforeStart,categories,seriesMasterId,type";

	/** The same without the body, for a listing that only wants the headline. */
	private static final String EVENT_FIELDS_NO_BODY = "id,subject,bodyPreview,start,end,isAllDay,location,attendees,"
			+ "organizer,webLink,onlineMeeting,isOnlineMeeting,isCancelled,showAs,importance,responseStatus,"
			+ "reminderMinutesBeforeStart,categories,seriesMasterId,type";

	private static final String CALENDAR_FIELDS = "id,name,color,canEdit,canShare,isDefaultCalendar,owner";

	private static final String VALUE = "value";
	private static final String EVENTS = "/events";
	private static final String DATE_TIME = "dateTime";
	private static final String TIME_ZONE = "timeZone";
	private static final String PREFER_HEADER = "Prefer";

	/**
	 * The zone Graph reads a naive date and time in when nothing says otherwise.
	 */
	public static final String DEFAULT_TIME_ZONE = "UTC";

	/** How many events one page of a listing asks Graph for. */
	private static final int PAGE_SIZE = 100;

	/** The three replies Graph accepts to a meeting invitation. */
	private static final String ACCEPT = "accept";
	private static final String DECLINE = "decline";
	private static final String TENTATIVELY_ACCEPT = "tentativelyAccept";
	private static final List<String> RESPONSES = Arrays.asList(ACCEPT, DECLINE, TENTATIVELY_ACCEPT);

	/**
	 * How the free and busy view is sliced when the caller does not say, in
	 * minutes. Graph allows 5 through 1440.
	 */
	private static final int DEFAULT_AVAILABILITY_INTERVAL = 30;

	/**
	 * Utility class constructor intentionally hidden.
	 */
	private MicrosoftCalendarHelper() {

	}

	/**
	 * Lists the calendars the signed in user can see.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param limit       maximum number of calendars to return; values less than or
	 *                    equal to 0 return every calendar
	 * @return list of calendar metadata maps
	 * @throws Exception if the list retrieval fails
	 */
	public static List<Map<String, Object>> listCalendars(String accessToken, int limit) throws Exception {
		final String CALENDARS = GRAPH_BASE + "/me/calendars?$select=" + CALENDAR_FIELDS + "&$top=%s";

		try {
			String url = String.format(CALENDARS, limit > 0 ? limit : PAGE_SIZE);
			String response = HttpHelperUtility.getRequest(url, headers(accessToken, null), null, null, null);
			List<Map<String, Object>> calendars = new ArrayList<>();
			for (Map<String, Object> calendar : getValueList(response)) {
				calendars.add(MicrosoftCalendarEventMapper.toCalendar(calendar));
				if (limit > 0 && calendars.size() >= limit) {
					break;
				}
			}
			return calendars;
		} catch (Exception e) {
			classLogger.error("Failed to list the Microsoft calendars for the current user.", e);
			throw e;
		}
	}

	/**
	 * Lists the events that fall within a window.
	 *
	 * <p>
	 * The window is what makes this a calendar view rather than a list of stored
	 * events, so a weekly meeting comes back once for every week it lands in rather
	 * than once as the series it was created as.
	 * </p>
	 *
	 * @param accessToken  Microsoft Graph access token for the user
	 * @param calendarId   optional id of the calendar to read; the user's default
	 *                     calendar is read when blank
	 * @param start        start of the window, as an ISO 8601 date and time
	 * @param end          end of the window, as an ISO 8601 date and time
	 * @param subject      optional text the subject has to contain, matched after
	 *                     the events come back because Graph cannot be asked for it
	 *                     alongside a calendar view
	 * @param includeBody  whether the event body comes back
	 * @param maxBodyChars the longest body to return before truncating it, or 0 to
	 *                     return whatever length it is
	 * @param timeZone     optional zone that naive times are read and answered in;
	 *                     defaults to UTC
	 * @param limit        maximum number of events to return; values less than or
	 *                     equal to 0 return every event in the window
	 * @return list of event maps, earliest first
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws Exception                if the list retrieval fails
	 */
	public static List<Map<String, Object>> listEvents(String accessToken, String calendarId, String start, String end,
			String subject, boolean includeBody, int maxBodyChars, String timeZone, int limit) throws Exception {
		final String CALENDAR_VIEW = "%s/calendarView?startDateTime=%s&endDateTime=%s&$select=%s&$orderby=%s&$top=%s";

		try {
			requireValue(start, "A start of the window is required to list Microsoft calendar events.");
			requireValue(end, "An end of the window is required to list Microsoft calendar events.");

			String windowStart = normalizeQueryDateTime(start);
			String windowEnd = normalizeQueryDateTime(end);
			// narrowing by subject happens here rather than on the server, so a whole
			// page is read even when only a few of it are wanted
			boolean narrowing = subject != null && !subject.trim().isEmpty();
			int pageSize = !narrowing && limit > 0 ? limit : PAGE_SIZE;

			String url = String.format(CALENDAR_VIEW, calendarPath(calendarId), encode(windowStart), encode(windowEnd),
					includeBody ? EVENT_FIELDS : EVENT_FIELDS_NO_BODY, encode("start/dateTime"), pageSize);
			String response = HttpHelperUtility.getRequest(url, headers(accessToken, timeZone), null, null, null);

			String wanted = narrowing ? subject.trim().toLowerCase(Locale.ROOT) : null;
			List<Map<String, Object>> events = new ArrayList<>();
			for (Map<String, Object> event : getValueList(response)) {
				if (wanted != null) {
					Object eventSubject = event.get("subject");
					if (eventSubject == null || !eventSubject.toString().toLowerCase(Locale.ROOT).contains(wanted)) {
						continue;
					}
				}
				events.add(MicrosoftCalendarEventMapper.toEvent(event, includeBody, maxBodyChars));
				if (limit > 0 && events.size() >= limit) {
					break;
				}
			}
			return events;
		} catch (Exception e) {
			classLogger.error("Failed to list the Microsoft calendar events between '{}' and '{}'.", start, end, e);
			throw e;
		}
	}

	/**
	 * Reads one event.
	 *
	 * @param accessToken  Microsoft Graph access token for the user
	 * @param calendarId   optional id of the calendar holding the event; the user's
	 *                     default calendar is read when blank
	 * @param eventId      id of the event to read
	 * @param maxBodyChars the longest body to return before truncating it, or 0 to
	 *                     return whatever length it is
	 * @param timeZone     optional zone the times are answered in; defaults to UTC
	 * @return the event
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the read fails
	 */
	public static Map<String, Object> getEvent(String accessToken, String calendarId, String eventId, int maxBodyChars,
			String timeZone) throws Exception {
		try {
			requireValue(eventId, "Event ID is required to read a Microsoft calendar event.");

			String url = calendarPath(calendarId) + EVENTS + "/" + encode(eventId.trim()) + "?$select=" + EVENT_FIELDS;
			String response = HttpHelperUtility.getRequest(url, headers(accessToken, timeZone), null, null, null);
			Map<String, Object> event = readMap(response);
			if (event == null) {
				throw new IllegalStateException("Microsoft Graph returned no event for event id = " + eventId);
			}
			return MicrosoftCalendarEventMapper.toEvent(event, true, maxBodyChars);
		} catch (Exception e) {
			classLogger.error("Failed to read Microsoft calendar event '{}'.", eventId, e);
			throw e;
		}
	}

	/**
	 * Creates an event.
	 *
	 * @param accessToken  Microsoft Graph access token for the user
	 * @param calendarId   optional id of the calendar to create in; the user's
	 *                     default calendar is used when blank
	 * @param event        the event, as built by
	 *                     {@link #buildEvent(String, String, boolean, String, String, String, boolean, String, String[], String[], Boolean, Integer, String, String, String[])}
	 * @param maxBodyChars the longest body to return before truncating it, or 0 to
	 *                     return whatever length it is
	 * @param timeZone     optional zone the times are answered in; defaults to UTC
	 * @return the event as Graph created it, which carries the id it was given and
	 *         a webLink that opens it in Outlook
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the create fails
	 */
	public static Map<String, Object> createEvent(String accessToken, String calendarId, Map<String, Object> event,
			int maxBodyChars, String timeZone) throws Exception {
		try {
			if (event == null || event.isEmpty()) {
				throw new IllegalArgumentException("An event is required to create a Microsoft calendar event.");
			}

			String url = calendarPath(calendarId) + EVENTS;
			String response = HttpHelperUtility.postRequestStringBody(url, headers(accessToken, timeZone),
					GSON.toJson(event), ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> created = readMap(response);
			if (created == null) {
				throw new IllegalStateException("Microsoft Graph returned no event for the create request.");
			}
			return MicrosoftCalendarEventMapper.toEvent(created, true, maxBodyChars);
		} catch (Exception e) {
			classLogger.error("Failed to create a Microsoft calendar event.", e);
			throw e;
		}
	}

	/**
	 * Changes an event.
	 *
	 * <p>
	 * Only the fields present in {@code changes} are sent, so everything the caller
	 * left out stays as it was.
	 * </p>
	 *
	 * @param accessToken  Microsoft Graph access token for the user
	 * @param calendarId   optional id of the calendar holding the event; the user's
	 *                     default calendar is used when blank
	 * @param eventId      id of the event to change
	 * @param changes      the fields to set, in the shape Graph reads them
	 * @param maxBodyChars the longest body to return before truncating it, or 0 to
	 *                     return whatever length it is
	 * @param timeZone     optional zone the times are answered in; defaults to UTC
	 * @return the event as Graph left it
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the change fails
	 */
	public static Map<String, Object> updateEvent(String accessToken, String calendarId, String eventId,
			Map<String, Object> changes, int maxBodyChars, String timeZone) throws Exception {
		try {
			requireValue(eventId, "Event ID is required to change a Microsoft calendar event.");
			if (changes == null || changes.isEmpty()) {
				throw new IllegalArgumentException("Nothing was passed to change on the Microsoft calendar event.");
			}

			String url = calendarPath(calendarId) + EVENTS + "/" + encode(eventId.trim());
			String response = HttpHelperUtility.patchRequestStringBody(url, headers(accessToken, timeZone),
					GSON.toJson(changes), ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> updated = readMap(response);
			if (updated == null) {
				throw new IllegalStateException("Microsoft Graph returned no event for event id = " + eventId);
			}
			return MicrosoftCalendarEventMapper.toEvent(updated, true, maxBodyChars);
		} catch (Exception e) {
			classLogger.error("Failed to change Microsoft calendar event '{}'.", eventId, e);
			throw e;
		}
	}

	/**
	 * Deletes an event.
	 *
	 * <p>
	 * An event the user organized is cancelled for everybody invited, and one they
	 * were invited to is only removed from their own calendar.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param calendarId  optional id of the calendar holding the event; the user's
	 *                    default calendar is used when blank
	 * @param eventId     id of the event to delete
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the delete fails
	 */
	public static void deleteEvent(String accessToken, String calendarId, String eventId) throws Exception {
		try {
			requireValue(eventId, "Event ID is required to delete a Microsoft calendar event.");

			String url = calendarPath(calendarId) + EVENTS + "/" + encode(eventId.trim());
			// a successful delete answers 204 with no body
			HttpHelperUtility.deleteRequestStringBody(url, headers(accessToken, null), null, null, null);
		} catch (Exception e) {
			classLogger.error("Failed to delete Microsoft calendar event '{}'.", eventId, e);
			throw e;
		}
	}

	/**
	 * Replies to a meeting invitation.
	 *
	 * @param accessToken  Microsoft Graph access token for the user
	 * @param eventId      id of the event to reply to
	 * @param response     the reply, one of {@code accept}, {@code decline} or
	 *                     {@code tentative}
	 * @param comment      optional note sent with the reply
	 * @param sendResponse whether the organizer is told; true sends the reply
	 * @return the reply Graph was asked to record
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws Exception                if the reply fails
	 */
	public static String respondToEvent(String accessToken, String eventId, String response, String comment,
			boolean sendResponse) throws Exception {
		try {
			requireValue(eventId, "Event ID is required to reply to a Microsoft calendar invitation.");
			String action = normalizeResponse(response);

			Map<String, Object> body = new LinkedHashMap<>();
			if (comment != null && !comment.trim().isEmpty()) {
				body.put("comment", comment.trim());
			}
			body.put("sendResponse", sendResponse);

			String url = GRAPH_BASE + "/me" + EVENTS + "/" + encode(eventId.trim()) + "/" + action;
			// answers 202 with no body, so there is nothing to read back
			HttpHelperUtility.postRequestStringBody(url, headers(accessToken, null), GSON.toJson(body),
					ContentType.APPLICATION_JSON, null, null, null);
			return action;
		} catch (Exception e) {
			classLogger.error("Failed to reply '{}' to Microsoft calendar event '{}'.", response, eventId, e);
			throw e;
		}
	}

	/**
	 * Reads the free and busy view of one or more mailboxes.
	 *
	 * <p>
	 * This is how a caller finds a time that suits everybody without reading
	 * anybody's events. What comes back says when each mailbox is busy, and says
	 * what they are busy with only where the mailbox has shared that much.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param schedules   the mailboxes to look at, by email address
	 * @param start       start of the window, as an ISO 8601 date and time
	 * @param end         end of the window, as an ISO 8601 date and time
	 * @param timeZone    optional zone that naive times are read and answered in;
	 *                    defaults to UTC
	 * @param interval    how many minutes each slot of the availability view
	 *                    covers; values less than or equal to 0 use 30
	 * @return one entry for each mailbox asked about
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws Exception                if the read fails
	 */
	public static List<Map<String, Object>> getSchedule(String accessToken, List<String> schedules, String start,
			String end, String timeZone, int interval) throws Exception {
		try {
			if (schedules == null || schedules.isEmpty()) {
				throw new IllegalArgumentException("At least one email address is required to read a schedule.");
			}
			requireValue(start, "A start of the window is required to read a Microsoft calendar schedule.");
			requireValue(end, "An end of the window is required to read a Microsoft calendar schedule.");

			String zone = timeZone == null || timeZone.trim().isEmpty() ? DEFAULT_TIME_ZONE : timeZone.trim();
			Map<String, Object> body = new LinkedHashMap<>();
			body.put("schedules", schedules);
			body.put("startTime", buildDateTimeTimeZone(start, zone, false));
			body.put("endTime", buildDateTimeTimeZone(end, zone, false));
			body.put("availabilityViewInterval", interval > 0 ? interval : DEFAULT_AVAILABILITY_INTERVAL);

			String url = GRAPH_BASE + "/me/calendar/getSchedule";
			String response = HttpHelperUtility.postRequestStringBody(url, headers(accessToken, timeZone),
					GSON.toJson(body), ContentType.APPLICATION_JSON, null, null, null);

			List<Map<String, Object>> entries = new ArrayList<>();
			for (Map<String, Object> entry : getValueList(response)) {
				entries.add(MicrosoftCalendarEventMapper.toSchedule(entry));
			}
			return entries;
		} catch (Exception e) {
			classLogger.error("Failed to read the Microsoft calendar schedule between '{}' and '{}'.", start, end, e);
			throw e;
		}
	}

	/**
	 * Builds an event in the shape Graph reads.
	 *
	 * <p>
	 * Pure assembly, so a caller can hold it, log it, or hand it to
	 * {@link #createEvent(String, String, Map, int, String)} later. Only what was
	 * passed is set, which is what lets the same method build the body of a change
	 * as well as of a create.
	 * </p>
	 *
	 * @param subject           the subject line, or null
	 * @param body              the body, or null
	 * @param html              whether the body is html rather than plain text
	 * @param start             when it starts, as an ISO 8601 date and time, or
	 *                          null
	 * @param end               when it ends, as an ISO 8601 date and time, or null
	 * @param timeZone          the zone a naive start and end are read in; defaults
	 *                          to UTC
	 * @param isAllDay          whether it covers whole days, in which case the
	 *                          start and end are read as dates and the end is the
	 *                          day after the last day
	 * @param location          where it is, or null
	 * @param attendees         the required attendees, or null
	 * @param optionalAttendees the optional attendees, or null
	 * @param isOnlineMeeting   whether Teams makes a meeting link for it, or null
	 *                          to leave it as it is
	 * @param reminderMinutes   how many minutes before the start the reminder
	 *                          fires, or null to leave it as it is
	 * @param showAs            how the time reads on the calendar, one of
	 *                          {@code free}, {@code tentative}, {@code busy},
	 *                          {@code oof}, {@code workingElsewhere} or
	 *                          {@code unknown}, or null
	 * @param importance        one of {@code low}, {@code normal} or {@code high},
	 *                          or null
	 * @param categories        the categories to tag it with, or null
	 * @return the event
	 * @throws IllegalArgumentException if a date and time cannot be read
	 */
	public static Map<String, Object> buildEvent(String subject, String body, boolean html, String start, String end,
			String timeZone, boolean isAllDay, String location, String[] attendees, String[] optionalAttendees,
			Boolean isOnlineMeeting, Integer reminderMinutes, String showAs, String importance, String[] categories) {
		String zone = timeZone == null || timeZone.trim().isEmpty() ? DEFAULT_TIME_ZONE : timeZone.trim();

		Map<String, Object> event = new LinkedHashMap<>();
		if (subject != null) {
			event.put("subject", subject);
		}
		if (body != null) {
			Map<String, Object> content = new LinkedHashMap<>();
			content.put("contentType", html ? "HTML" : "Text");
			content.put("content", body);
			event.put("body", content);
		}
		if (start != null && !start.trim().isEmpty()) {
			event.put("start", buildDateTimeTimeZone(start, zone, isAllDay));
		}
		if (end != null && !end.trim().isEmpty()) {
			event.put("end", buildDateTimeTimeZone(end, zone, isAllDay));
		}
		if (isAllDay) {
			event.put("isAllDay", true);
		}
		if (location != null && !location.trim().isEmpty()) {
			event.put("location", Map.of("displayName", location.trim()));
		}

		List<Map<String, Object>> invited = new ArrayList<>();
		addAttendees(invited, attendees, "required");
		addAttendees(invited, optionalAttendees, "optional");
		if (!invited.isEmpty()) {
			event.put("attendees", invited);
		}

		if (isOnlineMeeting != null) {
			event.put("isOnlineMeeting", isOnlineMeeting);
			if (isOnlineMeeting) {
				// the only provider a work or school account has, and Graph will not
				// make a link without being told which one to use
				event.put("onlineMeetingProvider", "teamsForBusiness");
			}
		}
		if (reminderMinutes != null) {
			event.put("reminderMinutesBeforeStart", reminderMinutes);
			event.put("isReminderOn", reminderMinutes >= 0);
		}
		if (showAs != null && !showAs.trim().isEmpty()) {
			event.put("showAs", showAs.trim());
		}
		if (importance != null && !importance.trim().isEmpty()) {
			event.put("importance", importance.trim());
		}
		if (categories != null && categories.length > 0) {
			event.put("categories", Arrays.asList(categories));
		}
		return event;
	}

	/**
	 * Adds one set of attendees in the shape Graph reads them.
	 *
	 * @param invited   the attendees being built
	 * @param addresses the addresses, or null when there are none
	 * @param type      whether they are required or optional
	 */
	private static void addAttendees(List<Map<String, Object>> invited, String[] addresses, String type) {
		if (addresses == null || addresses.length == 0) {
			return;
		}
		for (String address : addresses) {
			Map<String, Object> emailAddress = new LinkedHashMap<>();
			emailAddress.put("address", address);
			Map<String, Object> attendee = new LinkedHashMap<>();
			attendee.put("emailAddress", emailAddress);
			attendee.put("type", type);
			invited.add(attendee);
		}
	}

	/**
	 * Builds the object Graph reads a moment out of.
	 *
	 * <p>
	 * A value carrying an offset is turned into UTC and labelled as such, because
	 * Graph reads the zone off the label rather than off the value. A value without
	 * one is left alone and labelled with the zone the caller named, which is the
	 * shape somebody writing {@code 2026-09-01T13:00:00} in their own zone means.
	 * </p>
	 *
	 * @param dateTime the moment, as an ISO 8601 date and time
	 * @param timeZone the zone a naive value is read in
	 * @param dateOnly whether only the date matters, as it does for a whole day
	 *                 event
	 * @return the moment in the shape Graph reads
	 * @throws IllegalArgumentException if the value cannot be read
	 */
	private static Map<String, Object> buildDateTimeTimeZone(String dateTime, String timeZone, boolean dateOnly) {
		String value = dateTime.trim();
		Map<String, Object> moment = new LinkedHashMap<>();

		if (dateOnly) {
			moment.put(DATE_TIME, toLocalDate(value) + "T00:00:00");
			moment.put(TIME_ZONE, timeZone);
			return moment;
		}

		OffsetDateTime offset = readOffsetDateTime(value);
		if (offset != null) {
			moment.put(DATE_TIME, offset.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime()
					.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
			moment.put(TIME_ZONE, DEFAULT_TIME_ZONE);
			return moment;
		}

		moment.put(DATE_TIME, readLocalDateTime(value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		moment.put(TIME_ZONE, timeZone);
		return moment;
	}

	/**
	 * Moves a moment forward by whole days, keeping the shape it was written in.
	 *
	 * <p>
	 * This is how a caller that named only one end of a window gets the other, and
	 * it keeps the offset, or the lack of one, so the two ends are still read the
	 * same way as each other.
	 * </p>
	 *
	 * @param dateTime the moment, as an ISO 8601 date and time
	 * @param days     how many days to move it by
	 * @return the moved moment
	 * @throws IllegalArgumentException if the value cannot be read
	 */
	public static String plusDays(String dateTime, int days) {
		String value = dateTime.trim();
		OffsetDateTime offset = readOffsetDateTime(value);
		if (offset != null) {
			return offset.plusDays(days).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
		}
		try {
			return LocalDateTime.parse(value).plusDays(days).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
		} catch (DateTimeParseException e) {
			classLogger.debug("Moving '{}' by {} days as a whole date", value, days, e);
		}
		return toLocalDate(value).plusDays(days).atStartOfDay().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
	}

	/**
	 * Puts a moment into the form a calendar view query parameter takes.
	 *
	 * <p>
	 * An offset is collapsed to UTC so the window means one thing no matter what
	 * the {@code Prefer} header says, and a naive value is passed through for that
	 * header to interpret.
	 * </p>
	 *
	 * @param dateTime the moment, as an ISO 8601 date and time
	 * @return the moment as Graph reads it in a query
	 * @throws IllegalArgumentException if the value cannot be read
	 */
	private static String normalizeQueryDateTime(String dateTime) {
		String value = dateTime.trim();
		OffsetDateTime offset = readOffsetDateTime(value);
		if (offset != null) {
			return DateTimeFormatter.ISO_INSTANT.format(offset.toInstant());
		}
		try {
			return readLocalDateTime(value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
		} catch (IllegalArgumentException e) {
			classLogger.debug("Reading '{}' as a whole date instead of a date and time", value, e);
			return toLocalDate(value) + "T00:00:00";
		}
	}

	/**
	 * @param value the value to read
	 * @return the moment when it carries an offset, or null when it does not
	 */
	private static OffsetDateTime readOffsetDateTime(String value) {
		try {
			return OffsetDateTime.parse(value);
		} catch (DateTimeParseException e) {
			classLogger.debug("The date and time '{}' carries no offset", value, e);
			return null;
		}
	}

	/**
	 * @param value the value to read
	 * @return the moment, with midnight assumed when only a date was passed
	 * @throws IllegalArgumentException if the value cannot be read
	 */
	private static LocalDateTime readLocalDateTime(String value) {
		try {
			return LocalDateTime.parse(value);
		} catch (DateTimeParseException e) {
			classLogger.debug("The date and time '{}' carries no time", value, e);
			throw new IllegalArgumentException(
					"The date and time must be ISO 8601, such as 2026-09-01T13:00:00, but received: " + value);
		}
	}

	/**
	 * @param value the value to read, as a date, a date and time, or either with an
	 *              offset
	 * @return the date of it
	 * @throws IllegalArgumentException if the value cannot be read
	 */
	private static LocalDate toLocalDate(String value) {
		OffsetDateTime offset = readOffsetDateTime(value);
		if (offset != null) {
			return offset.toLocalDate();
		}
		try {
			return LocalDateTime.parse(value).toLocalDate();
		} catch (DateTimeParseException e) {
			classLogger.debug("The value '{}' carries no time, reading it as a date", value, e);
		}
		try {
			return LocalDate.parse(value);
		} catch (DateTimeParseException e) {
			throw new IllegalArgumentException("The date must be ISO 8601, such as 2026-09-01, but received: " + value,
					e);
		}
	}

	/**
	 * The part of a Graph url that says which calendar this is.
	 *
	 * @param calendarId the calendar, or null for the user's default one
	 * @return the url up to the calendar
	 */
	private static String calendarPath(String calendarId) {
		if (calendarId == null || calendarId.trim().isEmpty()) {
			return GRAPH_BASE + "/me";
		}
		return GRAPH_BASE + "/me/calendars/" + encode(calendarId.trim());
	}

	/**
	 * Builds the headers a calendar call carries.
	 *
	 * @param accessToken OAuth access token
	 * @param timeZone    optional zone that naive times are read and answered in
	 * @return the headers
	 */
	private static Map<String, String> headers(String accessToken, String timeZone) {
		Map<String, String> headers = new HashMap<>(MicrosoftLoginUtils.getBearerHeader(accessToken));
		if (timeZone != null && !timeZone.trim().isEmpty()) {
			headers.put(PREFER_HEADER, "outlook.timezone=\"" + timeZone.trim() + "\"");
		}
		return headers;
	}

	/**
	 * Validates the requested reply and puts it into the form Graph names it.
	 *
	 * @param response the reply the caller asked for
	 * @return the Graph action to call
	 * @throws IllegalArgumentException if the reply is not one Graph accepts
	 */
	private static String normalizeResponse(String response) {
		requireValue(response, "A reply of accept, decline or tentative is required.");
		String normalized = response.trim().toLowerCase(Locale.ROOT);
		if (ACCEPT.equals(normalized)) {
			return ACCEPT;
		}
		if (DECLINE.equals(normalized)) {
			return DECLINE;
		}
		if ("tentative".equals(normalized) || TENTATIVELY_ACCEPT.toLowerCase(Locale.ROOT).equals(normalized)) {
			return TENTATIVELY_ACCEPT;
		}
		throw new IllegalArgumentException("The reply must be one of " + RESPONSES + " but received: " + response);
	}

	/**
	 * Reads the {@code value} collection out of a Graph list response.
	 */
	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> getValueList(String response) {
		Map<String, Object> json = readMap(response);
		if (json == null) {
			return new ArrayList<>();
		}
		Object value = json.get(VALUE);
		if (!(value instanceof List)) {
			return new ArrayList<>();
		}
		return (List<Map<String, Object>>) value;
	}

	/**
	 * @param response the response body
	 * @return the response as a map, or null when there is nothing to read
	 */
	private static Map<String, Object> readMap(String response) {
		if (response == null || response.trim().isEmpty()) {
			return null;
		}
		return GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	/**
	 * URL encodes a value going into the path or the query. Graph ids are opaque
	 * but already url safe, so they come through as they were received.
	 */
	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}

	/**
	 * Guards against missing required string inputs.
	 */
	private static void requireValue(String value, String message) {
		if (value == null || value.trim().isEmpty()) {
			throw new IllegalArgumentException(message);
		}
	}

}
