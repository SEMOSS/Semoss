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
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.hc.core5.http.ContentType;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.io.connector.google.GoogleLoginUtils;
import prerna.security.HttpHelperUtility;

/**
 * The Google Calendar API, as plain calls for one signed in user.
 *
 * <p>
 * What the methods return is Google's own json, parsed into maps.
 * {@link GoogleCalendarEventMapper} turns that into the records every calendar
 * reactor answers with. Google answers an error with a status the http helper
 * throws on, so nothing here has to look for one.
 * </p>
 *
 * <p>
 * A calendar is named by its id, and somebody else's primary calendar by their
 * address, which reaches it only where they have shared it with the user.
 * </p>
 */
public final class GoogleCalendarHelper {

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String BASE = "https://www.googleapis.com/calendar/v3";

	/** The calendar Google reads when none is named. */
	public static final String PRIMARY = "primary";

	/** The most events Google lists on one page. */
	private static final int MAX_EVENTS_PAGE = 2500;

	/**
	 * How many events a page asks for at the least, so a narrowed listing does not
	 * read a few at a time.
	 */
	private static final int MIN_EVENTS_PAGE = 250;

	/**
	 * The most pages a listing reads, so a narrow search cannot walk a whole
	 * calendar.
	 */
	private static final int MAX_PAGES = 20;

	/** The most calendars Google lists on one page. */
	private static final int MAX_CALENDARS_PAGE = 250;

	private final String accessToken;

	/**
	 * @param accessToken the signed in user's Google access token
	 */
	public GoogleCalendarHelper(String accessToken) {
		this.accessToken = accessToken;
	}

	/**
	 * @param calendarId the calendar named, or null
	 * @param mailbox    whose calendar, or null
	 * @return the calendar to work against: the one named, else that person's
	 *         primary calendar, else the user's own
	 */
	public static String calendarOf(String calendarId, String mailbox) {
		if (calendarId != null && !calendarId.isBlank()) {
			return calendarId.trim();
		}
		if (mailbox != null && !mailbox.isBlank()) {
			return mailbox.trim();
		}
		return PRIMARY;
	}

	/**
	 * @return every calendar in the user's list, their own and the ones shared with
	 *         them
	 */
	public List<Map<String, Object>> listCalendars() {
		List<Map<String, Object>> calendars = new ArrayList<>();
		String pageToken = null;
		do {
			Map<String, Object> page = get(BASE + "/users/me/calendarList?maxResults=" + MAX_CALENDARS_PAGE
					+ (pageToken == null ? "" : "&pageToken=" + encode(pageToken)));
			calendars.addAll(listOf(page, "items"));
			pageToken = nextPageToken(page);
		} while (pageToken != null);
		return calendars;
	}

	/**
	 * @param calendarId the calendar
	 * @return the calendar, with its name and zone
	 */
	public Map<String, Object> getCalendar(String calendarId) {
		return get(BASE + "/calendars/" + encode(calendarId));
	}

	/**
	 * One run of events read from a calendar.
	 *
	 * @param events   the events, earliest first
	 * @param timeZone the calendar's own zone
	 * @param hasMore  whether there are more after them
	 */
	public record EventRun(List<Map<String, Object>> events, String timeZone, boolean hasMore) {

	}

	/**
	 * List the events in a window, earliest first, a repeating event once for every
	 * sitting, reading page after page until enough of them match.
	 *
	 * @param calendarId the calendar
	 * @param start      the start of the window
	 * @param end        the end of the window
	 * @param keep       which events count, such as the ones whose subject matches
	 * @param needed     how many matching events to read, counted from the first
	 * @return the events
	 */
	public EventRun listEvents(String calendarId, Instant start, Instant end, Predicate<Map<String, Object>> keep,
			int needed) {
		List<Map<String, Object>> events = new ArrayList<>();
		String timeZone = null;
		String pageToken = null;
		int pages = 0;
		do {
			String url = BASE + "/calendars/" + encode(calendarId) + "/events?singleEvents=true&orderBy=startTime"
					+ "&timeMin=" + encode(DateTimeFormatter.ISO_INSTANT.format(start)) + "&timeMax="
					+ encode(DateTimeFormatter.ISO_INSTANT.format(end)) + "&maxResults="
					+ Math.min(MAX_EVENTS_PAGE, Math.max(MIN_EVENTS_PAGE, needed))
					+ (pageToken == null ? "" : "&pageToken=" + encode(pageToken));
			Map<String, Object> page = get(url);
			if (timeZone == null && page != null && page.get("timeZone") != null) {
				timeZone = page.get("timeZone").toString();
			}
			for (Map<String, Object> event : listOf(page, "items")) {
				if (keep.test(event)) {
					events.add(event);
				}
			}
			pageToken = nextPageToken(page);
			pages++;
		} while (pageToken != null && events.size() < needed && pages < MAX_PAGES);
		return new EventRun(events, timeZone, pageToken != null);
	}

	/**
	 * @param calendarId the calendar
	 * @param eventId    the event
	 * @return the event
	 */
	public Map<String, Object> getEvent(String calendarId, String eventId) {
		return get(eventUrl(calendarId, eventId));
	}

	/**
	 * Create an event, inviting its attendees.
	 *
	 * @param calendarId the calendar
	 * @param event      the event, as {@link GoogleCalendarEventMapper#buildEvent}
	 *                   writes it
	 * @return the event as Google created it
	 */
	public Map<String, Object> insertEvent(String calendarId, Map<String, Object> event) {
		return send("POST",
				BASE + "/calendars/" + encode(calendarId) + "/events?conferenceDataVersion=1&sendUpdates=all", event);
	}

	/**
	 * Change an event, writing only what the changes set and telling its attendees.
	 *
	 * @param calendarId  the calendar
	 * @param eventId     the event
	 * @param changes     the fields to set
	 * @param sendUpdates whether attendees are told
	 * @return the event as Google left it
	 */
	public Map<String, Object> patchEvent(String calendarId, String eventId, Map<String, Object> changes,
			boolean sendUpdates) {
		return send("PATCH", eventUrl(calendarId, eventId) + "?conferenceDataVersion=1&sendUpdates="
				+ (sendUpdates ? "all" : "none"), changes);
	}

	/**
	 * Delete an event, canceling it for its attendees.
	 *
	 * @param calendarId the calendar
	 * @param eventId    the event
	 */
	public void deleteEvent(String calendarId, String eventId) {
		HttpHelperUtility.deleteRequestStringBody(eventUrl(calendarId, eventId) + "?sendUpdates=all",
				GoogleLoginUtils.getBearerHeader(this.accessToken), null, null, null);
	}

	/**
	 * @param calendars the people, by address
	 * @param start     the start of the window
	 * @param end       the end of the window
	 * @return when each of them is busy, keyed by address under {@code calendars}
	 */
	public Map<String, Object> freeBusy(List<String> calendars, Instant start, Instant end) {
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("timeMin", DateTimeFormatter.ISO_INSTANT.format(start));
		body.put("timeMax", DateTimeFormatter.ISO_INSTANT.format(end));
		body.put("items", calendars.stream().map(id -> Map.of("id", id)).toList());
		return send("POST", BASE + "/freeBusy", body);
	}

	/**
	 * @param calendarId the calendar
	 * @return who it is shared with, which only its owner may read
	 */
	public List<Map<String, Object>> listAcl(String calendarId) {
		List<Map<String, Object>> rules = new ArrayList<>();
		String pageToken = null;
		do {
			Map<String, Object> page = get(BASE + "/calendars/" + encode(calendarId) + "/acl"
					+ (pageToken == null ? "" : "?pageToken=" + encode(pageToken)));
			rules.addAll(listOf(page, "items"));
			pageToken = nextPageToken(page);
		} while (pageToken != null);
		return rules;
	}

	private static String eventUrl(String calendarId, String eventId) {
		return BASE + "/calendars/" + encode(calendarId) + "/events/" + encode(eventId);
	}

	private Map<String, Object> get(String url) {
		return readMap(HttpHelperUtility.getRequest(url, GoogleLoginUtils.getBearerHeader(this.accessToken), null, null,
				null));
	}

	private Map<String, Object> send(String method, String url, Map<String, Object> body) {
		String json = GSON.toJson(body);
		String response = "PATCH".equals(method)
				? HttpHelperUtility.patchRequestStringBody(url, GoogleLoginUtils.getBearerHeader(this.accessToken),
						json, ContentType.APPLICATION_JSON, null, null, null)
				: HttpHelperUtility.postRequestStringBody(url, GoogleLoginUtils.getBearerHeader(this.accessToken), json,
						ContentType.APPLICATION_JSON, null, null, null);
		return readMap(response);
	}

	private static String nextPageToken(Map<String, Object> page) {
		Object token = page == null ? null : page.get("nextPageToken");
		return token == null ? null : token.toString();
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> listOf(Map<String, Object> response, String key) {
		if (response == null || !(response.get(key) instanceof List)) {
			return List.of();
		}
		return (List<Map<String, Object>>) response.get(key);
	}

	private static Map<String, Object> readMap(String response) {
		if (response == null || response.trim().isEmpty()) {
			return null;
		}
		return GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}
}
