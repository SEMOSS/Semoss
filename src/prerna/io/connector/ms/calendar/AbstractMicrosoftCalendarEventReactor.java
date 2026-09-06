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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * What the reactors that write an event have in common.
 * 
 * <p>
 * Creating an event and changing one differ only in the Graph call at the end
 * and in what has to be there: a create needs a start and an end, and a change
 * needs whichever fields are being changed and nothing else. Everything before
 * that call - reading the attendees, checking the times, and assembling the
 * event - is the same, and is here so the two cannot drift apart.
 * </p>
 *
 * <p>
 * A field the caller left out is left out of the event as well rather than sent
 * as empty, which is what lets a change touch one thing without quietly
 * clearing the rest.
 * </p>
 */
public abstract class AbstractMicrosoftCalendarEventReactor extends AbstractMicrosoftCalendarReactor {

	protected static final String SUBJECT = "subject";
	protected static final String MESSAGE = "message";
	protected static final String HTML = "html";
	protected static final String IS_ALL_DAY = "isAllDay";
	protected static final String LOCATION = "location";
	protected static final String ATTENDEES = "attendees";
	protected static final String OPTIONAL_ATTENDEES = "optionalAttendees";
	protected static final String IS_ONLINE_MEETING = "isOnlineMeeting";
	protected static final String REMINDER_MINUTES = "reminderMinutesBeforeStart";
	protected static final String SHOW_AS = "showAs";
	protected static final String IMPORTANCE = "importance";
	protected static final String CATEGORIES = "categories";

	/**
	 * The keys every writing reactor takes, in the order it takes them. Clone it
	 * rather than assigning it straight to {@code keysToGet}, which belongs to the
	 * reactor instance.
	 */
	protected static final String[] EVENT_KEYS = { SUBJECT, START, END, TIME_ZONE, IS_ALL_DAY, LOCATION, ATTENDEES,
			OPTIONAL_ATTENDEES, MESSAGE, HTML, IS_ONLINE_MEETING, REMINDER_MINUTES, SHOW_AS, IMPORTANCE, CATEGORIES,
			CALENDAR_ID };

	/** How the time can be made to read on the calendar. */
	private static final List<String> SHOW_AS_VALUES = Arrays.asList("free", "tentative", "busy", "oof",
			"workingElsewhere", "unknown");

	/** How urgent an event can be marked. */
	private static final List<String> IMPORTANCE_VALUES = Arrays.asList("low", "normal", "high");

	/**
	 * Read the inputs and assemble the event.
	 *
	 * @param requireTimes whether a start and an end have to be there, which they
	 *                     do to create an event and do not to change one
	 * @param toDo         what this is being assembled for, used in the errors
	 * @return the event in the shape Graph reads
	 */
	protected Map<String, Object> composeEvent(boolean requireTimes, String toDo) {
		String start = trimToNull(this.keyValue.get(START));
		String end = trimToNull(this.keyValue.get(END));
		if (requireTimes) {
			if (start == null) {
				throw new SemossPixelException("A " + START + " is required to " + toDo + ".");
			}
			if (end == null) {
				throw new SemossPixelException("An " + END + " is required to " + toDo + ".");
			}
		}

		Integer reminderMinutes = optionalInt(REMINDER_MINUTES);
		if (reminderMinutes != null && reminderMinutes < 0) {
			throw new SemossPixelException(REMINDER_MINUTES + " cannot be negative.");
		}

		Map<String, Object> event;
		try {
			event = MicrosoftCalendarHelper.buildEvent(trimToNull(this.keyValue.get(SUBJECT)),
					this.keyValue.get(MESSAGE), Boolean.parseBoolean(this.keyValue.get(HTML)), start, end,
					trimToNull(this.keyValue.get(TIME_ZONE)), Boolean.TRUE.equals(optionalBoolean(IS_ALL_DAY)),
					trimToNull(this.keyValue.get(LOCATION)), values(ATTENDEES), values(OPTIONAL_ATTENDEES),
					optionalBoolean(IS_ONLINE_MEETING), reminderMinutes, oneOf(SHOW_AS, SHOW_AS_VALUES),
					oneOf(IMPORTANCE, IMPORTANCE_VALUES), values(CATEGORIES));
		} catch (IllegalArgumentException e) {
			throw new SemossPixelException(e.getMessage());
		}
		if (event.isEmpty()) {
			throw new SemossPixelException("Nothing was passed to " + toDo + ".");
		}
		return event;
	}

	/**
	 * Read a key that has to be one of a fixed set of words, matched however the
	 * caller happened to capitalize it.
	 *
	 * @param key      the key to read
	 * @param accepted the words Graph accepts, in the capitalization it uses
	 * @return the accepted word, or null when the caller left the key out
	 */
	private String oneOf(String key, List<String> accepted) {
		String value = trimToNull(this.keyValue.get(key));
		if (value == null) {
			return null;
		}
		for (String candidate : accepted) {
			if (candidate.equalsIgnoreCase(value)) {
				return candidate;
			}
		}
		throw new SemossPixelException(key + " must be one of " + accepted + " but received: " + value);
	}

	/**
	 * @return the zone naive times are read in, for a reactor answering with the
	 *         times it just wrote
	 */
	protected String requestedTimeZone() {
		String timeZone = trimToNull(this.keyValue.get(TIME_ZONE));
		return timeZone == null ? MicrosoftCalendarHelper.DEFAULT_TIME_ZONE : timeZone;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SUBJECT)) {
			return "Subject line of the event.";
		} else if (key.equals(MESSAGE)) {
			return "Body of the invitation, describing what the event is about.";
		} else if (key.equals(HTML)) {
			return "Optional boolean for whether the body is html rather than plain text. Defaults to false.";
		} else if (key.equals(IS_ALL_DAY)) {
			return "Optional boolean for whether the event covers whole days. The start and end are read as dates when it is true, and the end is the day after the last day.";
		} else if (key.equals(LOCATION)) {
			return "Optional place the event is held.";
		} else if (key.equals(ATTENDEES)) {
			return "Optional email addresses of the required attendees, passed as several values or as one comma separated value.";
		} else if (key.equals(OPTIONAL_ATTENDEES)) {
			return "Optional email addresses of the optional attendees, passed as several values or as one comma separated value.";
		} else if (key.equals(IS_ONLINE_MEETING)) {
			return "Optional boolean for whether a Microsoft Teams meeting link is created for the event.";
		} else if (key.equals(REMINDER_MINUTES)) {
			return "Optional number of minutes before the start that the reminder fires.";
		} else if (key.equals(SHOW_AS)) {
			return "Optional way the time reads on the calendar, one of " + SHOW_AS_VALUES + ".";
		} else if (key.equals(IMPORTANCE)) {
			return "Optional importance of the event, one of " + IMPORTANCE_VALUES + ".";
		} else if (key.equals(CATEGORIES)) {
			return "Optional categories to tag the event with, passed as several values or as one comma separated value.";
		}
		return super.getDescriptionForKey(key);
	}

}
