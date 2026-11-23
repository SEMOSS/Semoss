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
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.apache.hc.core5.http.ContentType;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.io.connector.ConnectorPage;
import prerna.io.connector.ConnectorTimes;
import prerna.io.connector.calendar.AbstractRespondToEventReactor;
import prerna.io.connector.calendar.Availability;
import prerna.io.connector.calendar.CalendarEvent;
import prerna.io.connector.calendar.CalendarInfo;
import prerna.io.connector.calendar.CalendarPermission;
import prerna.io.connector.calendar.EventRequest;
import prerna.io.connector.calendar.EventTime;
import prerna.io.connector.calendar.Recurrence;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.security.HttpHelperUtility;
import prerna.security.HttpStatusException;
import prerna.util.ValueUtils;

/**
 * The calendar operations of Microsoft Graph, as plain calls.
 *
 * <p>
 * Everything here is delegated: the token says who the signed in user is, so a
 * url that names no mailbox is rooted at {@code /me}. A calendar id narrows the
 * call to one calendar of that mailbox, and leaving it out uses the default
 * one.
 * </p>
 *
 * <p>
 * A mailbox names somebody else, and is how a calendar that has been shared or
 * delegated to the signed in user is read and written. Graph answers
 * {@code /users/{owner}} only where that owner has actually shared or delegated
 * the calendar, and only when the token carries {@code Calendars.Read.Shared}
 * or {@code Calendars.ReadWrite.Shared}, so naming a mailbox cannot reach a
 * calendar the user was not given.
 * </p>
 *
 * <p>
 * Outlook offers two ways to reach the same shared calendar, and which one
 * works depends on the ids being used, so both are supported here:
 * </p>
 * <ul>
 * <li>Out of the owner's mailbox, by naming the mailbox. The calendar and event
 * ids that come back belong to the owner's mailbox and are only valid against
 * it. This is the only way to reach a shared or delegated <em>primary</em>
 * calendar.</li>
 * <li>Out of the signed in user's own mailbox, by naming no mailbox. A shared
 * <em>custom</em> calendar the user accepted appears in their own calendar list
 * with its owner named, and the id it has there is a local one, valid only
 * against {@code /me}.</li>
 * </ul>
 *
 * <p>
 * Every call asks Graph for its times in UTC, and
 * {@link MicrosoftCalendarEventMapper} turns what Graph answers with into the
 * records every calendar reactor answers with.
 * </p>
 */
public class MicrosoftCalendarHelper {

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String GRAPH_BASE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";

	/** The fields an event listing asks for when the caller wants the body. */
	private static final String EVENT_FIELDS = "id,subject,bodyPreview,body,start,end,isAllDay,location,attendees,"
			+ "organizer,webLink,onlineMeeting,isOnlineMeeting,isCancelled,showAs,importance,responseStatus,"
			+ "reminderMinutesBeforeStart,categories,seriesMasterId,type,originalStartTimeZone,originalEndTimeZone";

	/** The same without the body, for a listing that only wants the headline. */
	private static final String EVENT_FIELDS_NO_BODY = "id,subject,bodyPreview,start,end,isAllDay,location,attendees,"
			+ "organizer,webLink,onlineMeeting,isOnlineMeeting,isCancelled,showAs,importance,responseStatus,"
			+ "reminderMinutesBeforeStart,categories,seriesMasterId,type,originalStartTimeZone,originalEndTimeZone";

	private static final String CALENDAR_FIELDS = "id,name,color,canEdit,canShare,canViewPrivateItems,"
			+ "isDefaultCalendar,owner";

	private static final String EVENTS = "/events";
	private static final String DATE_TIME = "dateTime";
	private static final String TIME_ZONE = "timeZone";

	/** How many items one page of a listing asks Graph for. */
	private static final int PAGE_SIZE = 100;

	/**
	 * The most pages a listing reads while narrowing by subject, which Graph cannot
	 * do on the server, so a narrow search cannot walk a whole calendar.
	 */
	private static final int MAX_PAGES = 20;

	/** How the time can be made to read on an Outlook calendar. */
	public static final List<String> SHOW_AS_VALUES = List.of("free", "tentative", "busy", "oof", "workingElsewhere",
			"unknown");

	/** How urgent an Outlook event can be marked. */
	public static final List<String> IMPORTANCE_VALUES = List.of("low", "normal", "high");

	/** How the free and busy view is sliced, in minutes, which Graph requires. */
	private static final int AVAILABILITY_INTERVAL = 30;

	/**
	 * The delegated scope a meeting transcript is read with, beside
	 * {@code OnlineMeetings.Read} to find the meeting. An administrator has to
	 * consent to it, so a sign in asks for it only once its organization has.
	 */
	public static final String TRANSCRIPT_SCOPE = "OnlineMeetingTranscript.Read.All";

	/**
	 * How far outside an event's own time a transcript may start and still be that
	 * sitting's. A meeting opens early or runs over, while the sittings of one
	 * series are a day or more apart.
	 */
	private static final Duration SITTING_SLACK = Duration.ofHours(4);

	/**
	 * The most pages of a meeting's transcripts that are read. A series lists every
	 * sitting's, in an order Graph does not promise, so the walk goes on well past
	 * what a listing by subject reads; the cap only stops a walk that never ends.
	 */
	private static final int TRANSCRIPT_PAGES = 100;

	/**
	 * The most transcripts read for a one-off meeting that was not held at its own
	 * time, the newest ones. Its link may be one people kept meeting on, and each
	 * transcript is a request of its own.
	 */
	private static final int ONE_OFF_TRANSCRIPTS = 5;

	private static final String NO_MEETING = "Microsoft Teams has no meeting for this event that the signed in user "
			+ "can read. Only a meeting's organizer and the people invited can read its transcript, and only until "
			+ "the meeting expires.";

	/**
	 * What was said in an event's Teams meeting.
	 *
	 * @param lines     the transcript, a line for each turn, each recording of it
	 *                  under a line saying when it started; empty when the event's
	 *                  own time has none
	 * @param elsewhere when the meeting's transcripts started, oldest first, for an
	 *                  event whose own time has none
	 */
	public record EventTranscript(List<String> lines, List<Instant> elsewhere) {
	}

	private MicrosoftCalendarHelper() {

	}

	/**
	 * Check the values an Outlook event accepts that another calendar may not.
	 *
	 * @param request the event the caller wrote
	 * @throws IllegalArgumentException when showAs or importance is not a value
	 *                                  Outlook accepts
	 */
	public static void checkEventValues(EventRequest request) {
		requireOneOf("showAs", request.showAs(), SHOW_AS_VALUES);
		requireOneOf("importance", request.importance(), IMPORTANCE_VALUES);
		// a category is a name from the user's own Outlook list, which Outlook adds a
		// name to the first time it is used, so any name is accepted
	}

	/**
	 * @param key showAs or categories
	 * @return what an Outlook event accepts for it
	 */
	public static String describeEventValues(String key) {
		if ("showAs".equals(key)) {
			return "One of " + SHOW_AS_VALUES + ".";
		}
		return "Each one the name of an Outlook category, such as Red Category, which Outlook adds to the user's "
				+ "list the first time it is used.";
	}

	private static void requireOneOf(String key, String value, List<String> accepted) {
		if (value == null) {
			return;
		}
		for (String candidate : accepted) {
			if (candidate.equalsIgnoreCase(value.trim())) {
				return;
			}
		}
		throw new IllegalArgumentException(key + " must be one of " + accepted + " but received: " + value);
	}

	/**
	 * Lists the calendars a mailbox holds.
	 *
	 * <p>
	 * Read against the signed in user's own mailbox, this is also the list of
	 * calendars other people have shared with them: a shared custom calendar the
	 * user accepted sits in their list with its owner named, which is what
	 * {@code isSharedWithMe} reports.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param mailbox     optional mailbox to read; the signed in user's own when
	 *                    blank
	 * @param userEmail   optional address of the signed in user, used to tell their
	 *                    own calendars from the ones shared with them
	 * @param offset      how many calendars to skip
	 * @param limit       how many calendars the page holds
	 * @return the page
	 */
	public static ConnectorPage<CalendarInfo> listCalendars(String accessToken, String mailbox, String userEmail,
			int offset, int limit) {
		// a mailbox holds few calendars, so they are all read and the page is cut here
		String url = mailboxPath(mailbox) + "/calendars?$select=" + CALENDAR_FIELDS + "&$top=" + PAGE_SIZE;
		List<CalendarInfo> calendars = new ArrayList<>();
		for (int page = 0; url != null && page < MAX_PAGES && calendars.size() < offset + limit + 1; page++) {
			Map<String, Object> response = readMap(get(url, accessToken, false));
			for (Map<String, Object> calendar : values(response)) {
				calendars.add(MicrosoftCalendarEventMapper.toCalendar(calendar, userEmail));
			}
			url = nextLink(response);
		}
		return ConnectorPage.of(calendars, offset, limit);
	}

	/**
	 * Lists who can see a calendar and what each of them may do with it.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param mailbox     optional mailbox holding the calendar
	 * @param calendarId  optional calendar; the default one when blank
	 * @return one permission for each person the calendar is shared with
	 */
	public static List<CalendarPermission> listCalendarPermissions(String accessToken, String mailbox,
			String calendarId) {
		// the permissions of the default calendar hang off the calendar shortcut
		// rather than off the mailbox, so a blank calendar id is spelled out here
		String url = ValueUtils.isBlank(calendarId) ? mailboxPath(mailbox) + "/calendar/calendarPermissions"
				: calendarPath(mailbox, calendarId) + "/calendarPermissions";
		List<CalendarPermission> permissions = new ArrayList<>();
		for (Map<String, Object> permission : readValues(get(url, accessToken, false))) {
			permissions.add(MicrosoftCalendarEventMapper.toPermission(permission));
		}
		return permissions;
	}

	/**
	 * Lists the events that fall within a window, earliest first.
	 *
	 * <p>
	 * The window is what makes this a calendar view rather than a list of stored
	 * events, so a weekly meeting comes back once for every week it lands in. Graph
	 * skips on the server unless the listing is narrowed by subject, which it
	 * cannot be asked for alongside a calendar view, so a narrowed listing reads
	 * page by page and is cut here.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param mailbox     optional mailbox holding the calendar
	 * @param calendarId  optional calendar; the default one when blank
	 * @param start       start of the window
	 * @param end         end of the window
	 * @param subject     optional text the subject has to contain
	 * @param includeBody whether the bodies come back
	 * @param offset      how many events to skip
	 * @param limit       how many events the page holds
	 * @return the page
	 */
	public static ConnectorPage<CalendarEvent> listEvents(String accessToken, String mailbox, String calendarId,
			Instant start, Instant end, String subject, boolean includeBody, int offset, int limit) {
		String base = calendarPath(mailbox, calendarId) + "/calendarView?startDateTime="
				+ encode(DateTimeFormatter.ISO_INSTANT.format(start)) + "&endDateTime="
				+ encode(DateTimeFormatter.ISO_INSTANT.format(end)) + "&$select="
				+ (includeBody ? EVENT_FIELDS : EVENT_FIELDS_NO_BODY) + "&$orderby=" + encode("start/dateTime");

		if (ValueUtils.isBlank(subject)) {
			String url = base + "&$top=" + (limit + 1) + "&$skip=" + offset;
			List<CalendarEvent> events = new ArrayList<>();
			for (Map<String, Object> event : readValues(get(url, accessToken, includeBody))) {
				events.add(MicrosoftCalendarEventMapper.toEvent(event));
			}
			return ConnectorPage.ofWindow(events, limit);
		}

		String wanted = subject.trim().toLowerCase(Locale.ROOT);
		Predicate<Map<String, Object>> matches = event -> event.get("subject") != null
				&& event.get("subject").toString().toLowerCase(Locale.ROOT).contains(wanted);
		List<CalendarEvent> events = new ArrayList<>();
		String url = base + "&$top=" + PAGE_SIZE;
		for (int page = 0; url != null && page < MAX_PAGES && events.size() < offset + limit + 1; page++) {
			Map<String, Object> response = readMap(get(url, accessToken, includeBody));
			for (Map<String, Object> event : values(response)) {
				if (matches.test(event)) {
					events.add(MicrosoftCalendarEventMapper.toEvent(event));
				}
			}
			url = nextLink(response);
		}
		ConnectorPage<CalendarEvent> page = ConnectorPage.of(events, offset, limit);
		// a search that stopped at its last page before filling one may have more
		return new ConnectorPage<>(page.items(), page.hasMore() || url != null);
	}

	/**
	 * Reads one event.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param mailbox     optional mailbox holding the calendar
	 * @param calendarId  optional calendar; the default one when blank
	 * @param eventId     the event
	 * @return the event
	 */
	public static CalendarEvent getEvent(String accessToken, String mailbox, String calendarId, String eventId) {
		String url = calendarPath(mailbox, calendarId) + EVENTS + "/" + encode(eventId) + "?$select=" + EVENT_FIELDS;
		return toEvent(get(url, accessToken, true), eventId);
	}

	/**
	 * Creates an event.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param request     the event
	 * @return the event as Graph created it
	 */
	public static CalendarEvent createEvent(String accessToken, EventRequest request) {
		String url = calendarPath(request.mailbox(), request.calendarId()) + EVENTS;
		String response = HttpHelperUtility.postRequestStringBody(url, headers(accessToken, false),
				GSON.toJson(buildEvent(request)), ContentType.APPLICATION_JSON, null, null, null);
		return toEvent(response, null);
	}

	/**
	 * Changes an event, sending only the fields the request sets, so everything the
	 * caller left out stays as it was.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param request     the changes, with the event's id
	 * @return the event as Graph left it
	 */
	public static CalendarEvent updateEvent(String accessToken, EventRequest request) {
		String url = calendarPath(request.mailbox(), request.calendarId()) + EVENTS + "/" + encode(request.id());
		String response = HttpHelperUtility.patchRequestStringBody(url, headers(accessToken, false),
				GSON.toJson(buildEvent(request)), ContentType.APPLICATION_JSON, null, null, null);
		return toEvent(response, request.id());
	}

	/**
	 * Deletes an event. One the user organized is canceled for everybody invited,
	 * and one they were invited to is only removed from their own calendar.
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param mailbox     optional mailbox holding the calendar
	 * @param calendarId  optional calendar; the default one when blank
	 * @param eventId     the event
	 */
	public static void deleteEvent(String accessToken, String mailbox, String calendarId, String eventId) {
		String url = calendarPath(mailbox, calendarId) + EVENTS + "/" + encode(eventId);
		// a successful delete answers 204 with no body
		HttpHelperUtility.deleteRequestStringBody(url, headers(accessToken, false), null, null, null);
	}

	/**
	 * Replies to a meeting invitation.
	 *
	 * <p>
	 * Naming a mailbox replies on that mailbox's behalf, which is what a delegate
	 * has been given the right to do. A share that only grants reading or writing
	 * is not enough for this, and Graph refuses it.
	 * </p>
	 *
	 * @param accessToken  Microsoft Graph access token for the user
	 * @param mailbox      optional mailbox the invitation was sent to
	 * @param calendarId   optional calendar holding the event
	 * @param eventId      the event
	 * @param response     accept, decline or tentative
	 * @param comment      optional note sent with the reply
	 * @param sendResponse whether the organizer is told
	 */
	public static void respondToEvent(String accessToken, String mailbox, String calendarId, String eventId,
			String response, String comment, boolean sendResponse) {
		String action = AbstractRespondToEventReactor.TENTATIVE.equals(response) ? "tentativelyAccept" : response;
		Map<String, Object> body = new LinkedHashMap<>();
		if (!ValueUtils.isBlank(comment)) {
			body.put("comment", comment.trim());
		}
		body.put("sendResponse", sendResponse);
		String url = calendarPath(mailbox, calendarId) + EVENTS + "/" + encode(eventId) + "/" + action;
		// answers 202 with no body, so there is nothing to read back
		HttpHelperUtility.postRequestStringBody(url, headers(accessToken, false), GSON.toJson(body),
				ContentType.APPLICATION_JSON, null, null, null);
	}

	/**
	 * Reads when one or more mailboxes are busy.
	 *
	 * <p>
	 * This is how a caller finds a time that suits everybody without reading
	 * anybody's events. What comes back says when each mailbox is busy, and what
	 * with only where the mailbox has shared that much.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param schedules   the mailboxes, by email address
	 * @param start       start of the window
	 * @param end         end of the window
	 * @return one entry for each mailbox asked about
	 */
	public static List<Availability> getSchedule(String accessToken, List<String> schedules, Instant start,
			Instant end) {
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("schedules", schedules);
		body.put("startTime", utcMoment(start));
		body.put("endTime", utcMoment(end));
		body.put("availabilityViewInterval", AVAILABILITY_INTERVAL);

		String response = HttpHelperUtility.postRequestStringBody(GRAPH_BASE + "/me/calendar/getSchedule",
				headers(accessToken, false), GSON.toJson(body), ContentType.APPLICATION_JSON, null, null, null);
		List<Availability> entries = new ArrayList<>();
		for (Map<String, Object> entry : readValues(response)) {
			entries.add(MicrosoftCalendarEventMapper.toAvailability(entry));
		}
		return entries;
	}

	/**
	 * Reads what was said in an event's Teams meeting.
	 *
	 * <p>
	 * A transcript hangs off the Teams meeting rather than the event, and every
	 * sitting of a series shares one meeting. So the meeting is found by the
	 * event's join link, and the transcripts kept are the ones that started around
	 * the event's own time. An event that does not repeat owns every transcript of
	 * its meeting, so one held at another time than planned still returns them.
	 * </p>
	 *
	 * <p>
	 * Graph reads these out of the signed in user's own meetings whichever mailbox
	 * the event came from, so only the organizer and the people invited get an
	 * answer.
	 * </p>
	 *
	 * @param accessToken Microsoft Graph access token for the user
	 * @param event       the event, as {@link #getEvent} read it
	 * @return the transcript
	 * @throws IllegalArgumentException when the event has no Teams meeting, or
	 *                                  Microsoft will not let its transcript be
	 *                                  read
	 */
	public static EventTranscript getEventTranscript(String accessToken, CalendarEvent event) {
		if (ValueUtils.isBlank(event.joinUrl())) {
			throw new IllegalArgumentException("This event has no Teams meeting, so it has no transcript.");
		}
		try {
			// the link is quoted inside the filter, and the whole filter is then encoded
			String filter = "JoinWebUrl eq '" + event.joinUrl().replace("'", "''") + "'";
			List<Map<String, Object>> meetings = readValues(
					get(GRAPH_BASE + "/me/onlineMeetings?$filter=" + encode(filter), accessToken, false));
			if (meetings.isEmpty() || meetings.get(0).get("id") == null) {
				throw new IllegalArgumentException(NO_MEETING);
			}
			String transcripts = GRAPH_BASE + "/me/onlineMeetings/" + encode(meetings.get(0).get("id").toString())
					+ "/transcripts";

			// when each transcript of the meeting started, oldest first
			NavigableMap<Instant, String> all = new TreeMap<>();
			String url = transcripts;
			for (int page = 0; url != null && page < TRANSCRIPT_PAGES; page++) {
				Map<String, Object> response = readMap(get(url, accessToken, false));
				for (Map<String, Object> transcript : values(response)) {
					Instant started = ConnectorTimes
							.parseProviderTime(ValueUtils.toStringOrNull(transcript.get("createdDateTime")));
					if (started != null && transcript.get("id") != null) {
						all.put(started, transcript.get("id").toString());
					}
				}
				url = nextLink(response);
			}

			// shortcut: a sitting's transcripts are told apart by when they started, since
			// Graph does not say which sitting one belongs to, so a sitting of a series held
			// far from its own time is not found; upgrade if Graph ever links the two
			EventTime last = event.end() == null ? event.start() : event.end();
			NavigableMap<Instant, String> own = all.subMap(
					event.start().localIn(ZoneOffset.UTC).toInstant(ZoneOffset.UTC).minus(SITTING_SLACK), true,
					last.localIn(ZoneOffset.UTC).toInstant(ZoneOffset.UTC).plus(SITTING_SLACK), true);
			if (own.isEmpty() && !event.isRecurring()) {
				// a one-off held at another time than planned: its transcripts are still its own
				own = all;
				while (own.size() > ONE_OFF_TRANSCRIPTS) {
					own.pollFirstEntry();
				}
			}

			List<String> lines = new ArrayList<>();
			for (Map.Entry<Instant, String> transcript : own.entrySet()) {
				// transcription that was stopped and started again leaves several, in order
				lines.add("[Transcript started " + ConnectorTimes.format(transcript.getKey()) + "]");
				lines.addAll(MicrosoftCalendarEventMapper.transcriptLines(
						transcriptContent(transcripts + "/" + encode(transcript.getValue()) + "/content", accessToken)));
			}
			return new EventTranscript(lines, own.isEmpty() ? List.copyOf(all.keySet()) : List.of());
		} catch (HttpStatusException e) {
			throw transcriptRefusal(e);
		}
	}

	/**
	 * Reads the words of one transcript: WebVTT with the speakers named, or the
	 * same words without names where the organization keeps them out of Graph.
	 *
	 * @param url         the transcript's content
	 * @param accessToken Microsoft Graph access token for the user
	 * @return the transcript as Graph wrote it
	 */
	private static String transcriptContent(String url, String accessToken) {
		Map<String, String> headers = MicrosoftLoginUtils.getAuthorizationHeader(accessToken);
		try {
			return HttpHelperUtility.getRequest(url + "?$format=text/vtt", headers, null, null, null);
		} catch (HttpStatusException e) {
			if (!String.valueOf(e.getMessage()).contains("SpeakerAttributionNotAllowed")) {
				throw e;
			}
			// the format without names can only be asked for in a header
			headers.put("Accept", "application/vnd.microsoft.graph.transcript+text");
			return HttpHelperUtility.getRequest(url, headers, null, null, null);
		}
	}

	/**
	 * What to say when Graph will not let a transcript be read. Its own answer is a
	 * status and a code, which tell an administrator what to change but not the
	 * person asking.
	 *
	 * @param refused what Graph answered
	 * @return the same refusal in words, or as it came when it is none of these
	 */
	private static IllegalArgumentException transcriptRefusal(HttpStatusException refused) {
		if (String.valueOf(refused.getMessage()).contains("GraphAccessToTranscriptsDisabled")) {
			return new IllegalArgumentException("This organization has turned off reading meeting transcripts "
					+ "through Microsoft Graph. A Teams administrator can turn it back on in the meeting settings.");
		}
		if (refused.getStatusCode() == 403) {
			return new IllegalArgumentException("Microsoft would not let this meeting's transcript be read. Either "
					+ "the signed in user is neither its organizer nor invited to it, or the Microsoft sign in was "
					+ "not granted OnlineMeetings.Read and " + TRANSCRIPT_SCOPE + ", which an administrator "
					+ "consents to and which take effect the next time the user signs in to Microsoft.");
		}
		if (refused.getStatusCode() == 404) {
			return new IllegalArgumentException(NO_MEETING);
		}
		return refused;
	}

	/**
	 * Builds an event in the shape Graph reads, setting only what the request sets,
	 * which is what lets the same method build a change as well as a create.
	 *
	 * <p>
	 * Times are written as the clock reads in the event's own zone, labeled with
	 * that zone, so the calendar keeps the event in it and a series keeps its local
	 * time across daylight saving changes.
	 * </p>
	 *
	 * @param request the event
	 * @return the event in the shape Graph reads
	 */
	static Map<String, Object> buildEvent(EventRequest request) {
		ZoneId zone = request.zone() == null ? ZoneOffset.UTC : request.zone();
		Map<String, Object> event = new LinkedHashMap<>();
		if (request.subject() != null) {
			event.put("subject", request.subject());
		}
		if (request.body() != null) {
			event.put("body", Map.of("contentType", request.html() ? "HTML" : "Text", "content", request.body()));
		}
		if (request.start() != null) {
			event.put("start", moment(request.start(), zone));
		}
		if (request.end() != null) {
			event.put("end", moment(request.end(), zone));
		}
		if (request.isAllDay() != null) {
			event.put("isAllDay", request.isAllDay());
		}
		if (request.location() != null) {
			event.put("location", Map.of("displayName", request.location()));
		}
		if (request.setsAttendees()) {
			List<Map<String, Object>> invited = new ArrayList<>();
			addAttendees(invited, request.attendees(), "required");
			addAttendees(invited, request.optionalAttendees(), "optional");
			event.put("attendees", invited);
		}
		if (request.isOnlineMeeting() != null) {
			event.put("isOnlineMeeting", request.isOnlineMeeting());
			if (request.isOnlineMeeting()) {
				// the only provider a work or school account has, and Graph will not
				// make a link without being told which one to use
				event.put("onlineMeetingProvider", "teamsForBusiness");
			}
		}
		if (request.reminderMinutes() != null) {
			event.put("reminderMinutesBeforeStart", request.reminderMinutes());
			event.put("isReminderOn", true);
		}
		if (request.showAs() != null) {
			event.put("showAs", canonical(request.showAs(), SHOW_AS_VALUES));
		}
		if (request.importance() != null) {
			event.put("importance", canonical(request.importance(), IMPORTANCE_VALUES));
		}
		if (request.categories() != null) {
			event.put("categories", request.categories());
		}
		if (request.recurrence() != null && request.start() != null) {
			event.put("recurrence", recurrence(request.recurrence(), request.start(), zone));
		}
		return event;
	}

	/**
	 * Builds the rule Graph repeats a series by, on the weekday, the day of the
	 * month, or the date its first sitting falls on.
	 */
	private static Map<String, Object> recurrence(Recurrence recurrence, EventTime start, ZoneId zone) {
		LocalDate first = start.localIn(zone).toLocalDate();
		Map<String, Object> pattern = new LinkedHashMap<>();
		pattern.put("interval", 1);
		switch (recurrence.frequency()) {
		case Recurrence.WEEKLY:
			pattern.put("type", "weekly");
			pattern.put("daysOfWeek", List.of(dayName(first.getDayOfWeek())));
			break;
		case Recurrence.MONTHLY:
			pattern.put("type", "absoluteMonthly");
			pattern.put("dayOfMonth", first.getDayOfMonth());
			break;
		case Recurrence.YEARLY:
			pattern.put("type", "absoluteYearly");
			pattern.put("dayOfMonth", first.getDayOfMonth());
			pattern.put("month", first.getMonthValue());
			break;
		default:
			pattern.put("type", "daily");
		}
		Map<String, Object> range = new LinkedHashMap<>();
		range.put("type", recurrence.until() == null ? "noEnd" : "endDate");
		range.put("startDate", first.toString());
		if (recurrence.until() != null) {
			range.put("endDate", recurrence.until().toString());
		}
		range.put("recurrenceTimeZone", ConnectorTimes.providerZoneId(zone));
		return Map.of("pattern", pattern, "range", range);
	}

	private static String dayName(DayOfWeek day) {
		return day.name().toLowerCase(Locale.ROOT);
	}

	private static void addAttendees(List<Map<String, Object>> invited, List<String> addresses, String type) {
		if (addresses == null) {
			return;
		}
		for (String address : addresses) {
			Map<String, Object> attendee = new LinkedHashMap<>();
			attendee.put("emailAddress", Map.of("address", address));
			attendee.put("type", type);
			invited.add(attendee);
		}
	}

	/**
	 * @return the moment in the shape Graph reads it, as the clock in the zone
	 *         reads it, labeled with the zone
	 */
	private static Map<String, Object> moment(EventTime time, ZoneId zone) {
		LocalDateTime local = time.localIn(zone);
		Map<String, Object> moment = new LinkedHashMap<>();
		moment.put(DATE_TIME, local.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		moment.put(TIME_ZONE, ConnectorTimes.providerZoneId(zone));
		return moment;
	}

	private static Map<String, Object> utcMoment(Instant instant) {
		return moment(EventTime.of(instant), ZoneOffset.UTC);
	}

	private static String canonical(String value, List<String> accepted) {
		for (String candidate : accepted) {
			if (candidate.equalsIgnoreCase(value.trim())) {
				return candidate;
			}
		}
		return value.trim();
	}

	private static CalendarEvent toEvent(String response, String eventId) {
		Map<String, Object> event = readMap(response);
		if (event == null || event.get("id") == null) {
			throw new IllegalStateException(
					"Microsoft Graph returned no event" + (eventId == null ? "." : " for event id = " + eventId));
		}
		return MicrosoftCalendarEventMapper.toEvent(event);
	}

	private static String get(String url, String accessToken, boolean textBody) {
		return HttpHelperUtility.getRequest(url, headers(accessToken, textBody), null, null, null);
	}

	/**
	 * The part of a Graph url that says whose mailbox this is.
	 *
	 * @param mailbox the mailbox, or null for the signed in user's own
	 * @return the url up to the mailbox
	 */
	private static String mailboxPath(String mailbox) {
		if (ValueUtils.isBlank(mailbox)) {
			// the delegated shape, where the token already says who this is
			return GRAPH_BASE + "/me";
		}
		return GRAPH_BASE + "/users/" + encode(mailbox.trim());
	}

	/**
	 * The part of a Graph url that says which calendar this is.
	 *
	 * @param mailbox    the mailbox holding the calendar, or null for the signed in
	 *                   user's own
	 * @param calendarId the calendar, or null for the default calendar of that
	 *                   mailbox
	 * @return the url up to the calendar
	 */
	private static String calendarPath(String mailbox, String calendarId) {
		String owner = mailboxPath(mailbox);
		if (ValueUtils.isBlank(calendarId)) {
			return owner;
		}
		return owner + "/calendars/" + encode(calendarId.trim());
	}

	/**
	 * The headers a call carries: times in UTC, and for a read that returns bodies,
	 * Outlook's own plain text rendering of them. It keeps the paragraphs and line
	 * breaks of an invitation, and writes a link as its text followed by the
	 * address in angle brackets, where reducing the markup here would run
	 * everything into one line.
	 *
	 * @param accessToken the token to send
	 * @param textBody    whether bodies come back as plain text
	 * @return the headers
	 */
	private static Map<String, String> headers(String accessToken, boolean textBody) {
		Map<String, String> headers = new HashMap<>(MicrosoftLoginUtils.getBearerHeader(accessToken));
		// graph reads several preferences from one header, comma separated
		headers.put("Prefer", "outlook.timezone=\"UTC\"" + (textBody ? ", outlook.body-content-type=\"text\"" : ""));
		return headers;
	}

	/**
	 * @param response one page of a Graph listing
	 * @return the url of the page after it, or null when it is the last
	 */
	private static String nextLink(Map<String, Object> response) {
		Object next = response == null ? null : response.get("@odata.nextLink");
		return next == null ? null : next.toString();
	}

	private static List<Map<String, Object>> readValues(String response) {
		return values(readMap(response));
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> values(Map<String, Object> response) {
		if (response == null || !(response.get("value") instanceof List)) {
			return new ArrayList<>();
		}
		return (List<Map<String, Object>>) response.get("value");
	}

	private static Map<String, Object> readMap(String response) {
		if (response == null || response.trim().isEmpty()) {
			return null;
		}
		return GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	/**
	 * URL encodes a value going into the path or the query.
	 */
	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}

}
