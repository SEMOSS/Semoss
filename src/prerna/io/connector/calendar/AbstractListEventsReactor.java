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
package prerna.io.connector.calendar;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.io.connector.ConnectorPage;
import prerna.io.connector.ConnectorTimes;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads the events that fall within a window, earliest first.
 *
 * <p>
 * A repeating event comes back once for every sitting that lands in the window,
 * rather than once as the series it was created as.
 * </p>
 */
public abstract class AbstractListEventsReactor extends AbstractCalendarReactor {

	/** How far ahead the window reaches when a caller does not say. */
	private static final int DEFAULT_DAYS = 7;

	/** How many events come back when a caller does not say. */
	private static final int DEFAULT_LIMIT = 50;

	/** The most a caller can ask for, so a pixel cannot pull a whole calendar. */
	private static final int MAX_LIMIT = 100;

	// a listing is many events at once; GetEvent reads one in full
	private static final int LIST_MAX_BODY_CHARS = 1_500;
	private static final int LIST_BODY_CHARS_CAP = 4_000;
	private static final int LIST_MAX_ATTENDEES = 15;
	// about 10k tokens; past it the page ends early and says where to continue
	private static final int LIST_OUTPUT_BUDGET_CHARS = 40_000;
	private static final Gson GSON = new Gson();

	private static final String[] KEYS = { START, END, DAYS, TIME_ZONE, SUBJECT, LIMIT, OFFSET, INCLUDE_BODY,
			MAX_BODY_CHARS, CALENDAR_ID, MAILBOX };

	/**
	 * What to read from a calendar.
	 *
	 * @param calendarId  the calendar, or null for the default one
	 * @param mailbox     whose calendar, or null for the user's own
	 * @param start       the start of the window
	 * @param end         the end of the window
	 * @param subject     text the subject has to contain, or null
	 * @param limit       how many events the page holds
	 * @param offset      how many events to skip
	 * @param includeBody whether the bodies come back
	 */
	public record ListEventsRequest(String calendarId, String mailbox, Instant start, Instant end, String subject,
			int limit, int offset, boolean includeBody) {

		/**
		 * @return how many events, counted from the first, a provider that cannot skip
		 *         has to read to fill the page and tell whether there is more
		 */
		public int needed() {
			return this.offset + this.limit + 1;
		}
	}

	protected AbstractListEventsReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet);
	}

	/**
	 * Read one page of the events in the window, earliest first.
	 *
	 * @param user    the signed in user
	 * @param request what to read
	 * @return the page
	 * @throws Exception when the calendar cannot be read
	 */
	protected abstract ConnectorPage<CalendarEvent> listEvents(User user, ListEventsRequest request) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("read your calendar", () -> {
			Instant[] window = readWindow(DEFAULT_DAYS);
			// a listing is usually about when rather than what, and a body each is the
			// bulk of what comes back, so it is left out unless it is asked for
			boolean includeBody = readBoolean(INCLUDE_BODY, false);
			ListEventsRequest request = new ListEventsRequest(readString(CALENDAR_ID), readString(MAILBOX), window[0],
					window[1], readString(SUBJECT), readCount(LIMIT, DEFAULT_LIMIT, MAX_LIMIT), readOffset(),
					includeBody);
			int maxBodyChars = readCount(MAX_BODY_CHARS, LIST_MAX_BODY_CHARS, LIST_BODY_CHARS_CAP);
			// times come back in the zone the caller named, else UTC as before
			ZoneId zone = readString(TIME_ZONE) == null ? null : readZone();

			ConnectorPage<CalendarEvent> page = listEvents(this.insight.getUser(), request);

			List<Map<String, Object>> events = new ArrayList<>();
			int used = 0;
			for (CalendarEvent event : page.items()) {
				Map<String, Object> row = event.inZone(event.toListMap(includeBody, maxBodyChars, LIST_MAX_ATTENDEES),
						zone);
				int size = GSON.toJson(row).length();
				// always keep the first, so a page never comes back empty
				if (!events.isEmpty() && used + size > LIST_OUTPUT_BUDGET_CHARS) {
					break;
				}
				events.add(row);
				used += size;
			}
			boolean truncated = events.size() < page.items().size();

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(START, ConnectorTimes.format(request.start()));
			output.put(END, ConnectorTimes.format(request.end()));
			output.put(OFFSET, request.offset());
			output.put("count", events.size());
			output.put("hasMore", page.hasMore() || truncated);
			if (truncated) {
				output.put("truncated", true);
				output.put("nextOffset", request.offset() + events.size());
			}
			output.put("events", events);
			return output;
		});
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.AUTO;
	}

	@Override
	protected final String getView() {
		return "calendar-agenda";
	}

	@Override
	protected final String describe() {
		return "Read the events on a " + calendar()
				+ ", the signed in user's own or one shared or delegated to them, earliest first, a page at a time. "
				+ "List first, then read the few events you need in full with GetEvent. A listing shows at most "
				+ LIST_MAX_ATTENDEES + " attendees per event and short bodies; a large page ends early with "
				+ "truncated and nextOffset to continue from.";
	}

	@Override
	protected final String describeKey(String key) {
		switch (key) {
		case START:
			return "Optional start of the window as an ISO 8601 date and time. Defaults to now.";
		case END:
			return "Optional end of the window as an ISO 8601 date and time. Defaults to the number of days after the start.";
		case DAYS:
			return "Optional number of days the window covers when no end is given. Defaults to " + DEFAULT_DAYS + ".";
		case TIME_ZONE:
			return "Optional IANA time zone, such as America/New_York. Start and end without an offset are read in "
					+ "it, and each event's start and end come back in it with a readable when, such as \"Mon Oct "
					+ "19, 9:00 AM - 9:05 AM\". Pass the user's zone so no conversion is needed. Defaults to UTC.";
		case SUBJECT:
			return "Optional text the subject has to contain, matched without regard to case.";
		case LIMIT:
			return "Optional number of events to return. Defaults to " + DEFAULT_LIMIT + " and is capped at "
					+ MAX_LIMIT + ".";
		case MAX_BODY_CHARS:
			return "Optional longest body per event, up to " + LIST_BODY_CHARS_CAP + ". Defaults to "
					+ LIST_MAX_BODY_CHARS + ".";
		case INCLUDE_BODY:
			return "Optional boolean for whether each event's body comes back, cut to " + LIST_MAX_BODY_CHARS
					+ " characters without the Teams join text. Defaults to false, since a listing is usually about "
					+ "when rather than what; use GetEvent for a full body.";
		default:
			return super.describeKey(key);
		}
	}
}
