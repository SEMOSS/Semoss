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

import prerna.io.connector.AbstractConnectorAppReactor;
import prerna.io.connector.ConnectorTimes;
import prerna.io.connector.IConnectorApp;

/**
 * What every calendar reactor has in common, whichever calendar it works
 * against.
 *
 * <p>
 * The keys naming a calendar, an event and a time mean the same thing wherever
 * they appear and for every provider, so they are named, read and described
 * once here. Times in a result are always UTC; a time a caller passes without
 * an offset is read in the zone they name, or in UTC when they name none.
 * </p>
 */
public abstract class AbstractCalendarReactor extends AbstractConnectorAppReactor {

	public static final String ID = "id";
	public static final String CALENDAR_ID = "calendarId";
	public static final String MAILBOX = "mailbox";
	public static final String TIME_ZONE = "timeZone";
	public static final String START = "start";
	public static final String END = "end";
	public static final String DAYS = "days";
	public static final String SUBJECT = "subject";
	public static final String LIMIT = "limit";
	public static final String OFFSET = "offset";
	public static final String INCLUDE_BODY = "includeBody";
	public static final String MAX_BODY_CHARS = "maxBodyChars";
	public static final String SCHEDULES = "schedules";
	public static final String BODY = "body";
	public static final String HTML = "html";
	public static final String IS_ALL_DAY = "isAllDay";
	public static final String LOCATION = "location";
	public static final String ATTENDEES = "attendees";
	public static final String OPTIONAL_ATTENDEES = "optionalAttendees";
	public static final String IS_ONLINE_MEETING = "isOnlineMeeting";
	public static final String REMINDER_MINUTES = "reminderMinutesBeforeStart";
	public static final String SHOW_AS = "showAs";
	public static final String CATEGORIES = "categories";
	public static final String RECURRENCE = "recurrence";
	public static final String RECURRENCE_UNTIL = "recurrenceUntil";
	public static final String RESPONSE = "response";
	public static final String COMMENT = "comment";
	public static final String SEND_RESPONSE = "sendResponse";

	/** Microsoft 365 only: how urgent an event is. Google keeps no such thing. */
	public static final String IMPORTANCE = "importance";

	/** How much of a body comes back before it is cut short. */
	public static final int DEFAULT_MAX_BODY_CHARS = 10_000;

	/**
	 * @return the calendar this reactor works against
	 */
	protected abstract CalendarApp getCalendarApp();

	@Override
	protected final IConnectorApp getApp() {
		return getCalendarApp();
	}

	/**
	 * @param operation the operation, such as ListEvents
	 * @return the name of this calendar's reactor for it, for a description to
	 *         point at
	 */
	protected final String reactorName(String operation) {
		return getCalendarApp().getReactorPrefix() + operation;
	}

	/**
	 * @return the calendar as a description names it
	 */
	protected final String calendar() {
		return getCalendarApp().getDisplayName();
	}

	@Override
	protected String describeKey(String key) {
		switch (key) {
		case ID:
			return "Id of the event, as returned by " + reactorName("ListEvents") + ".";
		case CALENDAR_ID:
			return "Optional id of the calendar to work against, as returned by " + reactorName("ListCalendars")
					+ ". The default calendar is used when omitted.";
		case MAILBOX:
			return "Optional email address of somebody whose calendar was shared or delegated to the signed in "
					+ "user, to work against that person's calendar instead of the user's own.";
		case TIME_ZONE:
			return "Optional IANA time zone, such as America/New_York, that times without an offset are read in. "
					+ "Defaults to UTC. Times in the result are always UTC.";
		case START:
			return "Start as an ISO 8601 date and time, such as 2026-09-01T17:00:00Z, or 2026-09-01T13:00:00 to "
					+ "read it in timeZone.";
		case END:
			return "End as an ISO 8601 date and time, such as 2026-09-01T18:00:00Z, or 2026-09-01T14:00:00 to read "
					+ "it in timeZone.";
		case LIMIT:
			return "Optional number of results to return.";
		case OFFSET:
			return "Optional number of results to skip, to read the page after one already read. Defaults to 0; "
					+ "hasMore in the result says whether there is another page.";
		case MAX_BODY_CHARS:
			return "Optional longest body to return before it is truncated. Defaults to " + DEFAULT_MAX_BODY_CHARS
					+ ".";
		default:
			return null;
		}
	}

	/**
	 * @return the zone the caller named, or UTC when they named none
	 */
	protected final ZoneId readZone() {
		return ConnectorTimes.readZone(readString(TIME_ZONE));
	}

	/**
	 * Read the window a listing covers, which starts now and runs for a number of
	 * days unless the caller frames it.
	 *
	 * @param defaultDays how many days the window covers when the caller gives no
	 *                    end and no number of days
	 * @return the start and end of the window
	 */
	protected final Instant[] readWindow(int defaultDays) {
		ZoneId zone = readZone();
		String startValue = readString(START);
		Instant start = startValue == null ? Instant.now() : ConnectorTimes.readMoment(startValue, zone);
		String endValue = readString(END);
		Instant end;
		if (endValue == null) {
			int days = readCount(DAYS, defaultDays, Integer.MAX_VALUE);
			end = start.atZone(zone).plusDays(days).toInstant();
		} else {
			end = ConnectorTimes.readMoment(endValue, zone);
		}
		if (!end.isAfter(start)) {
			throw new IllegalArgumentException("The end of the window has to be after its start.");
		}
		return new Instant[] { start, end };
	}

	/**
	 * Read the event this reactor was pointed at.
	 *
	 * @param toDo what is being done to it, used in the error
	 * @return the event id
	 */
	protected final String requireEventId(String toDo) {
		return requireString(ID,
				"An " + ID + ", as returned by " + reactorName("ListEvents") + ", is required to " + toDo + ".");
	}
}
